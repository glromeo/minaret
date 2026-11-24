import { signal } from "@preact/signals";

/**
 * A small force-directed layout: repulsion between every pair, a spring pulling each body toward
 * its parent (the Broker for a top-level client; a dynamic node for one of its channels; a channel
 * for one of its topics), a weak attraction between linked spikes, and velocity damping.
 *
 * Hand-rolled rather than d3-force because this graph is a fixed-depth tree — Broker, clients, and
 * for a dynamic node its channels and their topics — so the general-purpose link, collide and center
 * machinery would be weight this shape never uses. A couple dozen lines of physics against a ~90KB
 * dependency.
 *
 * Positions live outside the signal graph deliberately. They change every frame at 60fps, and routing
 * that through signals would re-run every computed and diff every VNode sixty times a second. The
 * simulation mutates a plain array in place and bumps `tick` once per frame, which is the single
 * subscription the canvas re-renders on.
 */

export interface Body {
    /** Unique across the whole tree — a RouteId for a client, a synthetic id for a channel/topic. */
    id: string;
    /** Root bodies (ordinary clients) have no parent and spring toward the Broker's fixed centre. */
    parentId: string | null;
    x: number;
    y: number;
    vx: number;
    vy: number;
    /**
     * Set once a drag releases this body.
     *
     * Behaves as pinned does — no spring, no velocity integration — but permanently rather than for
     * the drag's duration, so a dropped node stays exactly where the operator put it instead of
     * springing back to its orbit. It still takes part in repulsion, so other bodies avoid it.
     */
    fixed: boolean;
}

export interface BodySpec {
    id: string;
    parentId: string | null;
}

/**
 * Two bodies the layout should draw together: a client-role spike and the provider-role spike it
 * resolves against.
 *
 * `virusTree.ts` owns the pairing and hands the same pairs to `BusGraph`'s renderer, so the drawn
 * line and the applied force can never be between different bodies.
 *
 * Symmetric here — the physics has no notion of which end is which, it only wants the two ends of a
 * drawn line near each other so the line has less of the graph to cross.
 */
export interface LinkSpec {
    clientId: string;
    providerId: string;
}

/** Bumped once per simulated frame — the canvas's only reactive dependency on the layout. */
export const tick = signal(0);

/**
 * Per-depth tuning, because a level's repulsion-to-spring *ratio* is what decides whether a body can
 * settle, and the same numbers cannot serve two orbit radii.
 *
 * A root client (depth 0) orbits the Broker at long range against every other body, so it wants far
 * stronger repulsion. A channel or topic (depth 1/2) orbits its own parent at close range, where the
 * depth-0 strength would dwarf the tight spring holding it there — repulsion falls off as 1/distance²,
 * so at a 70px child orbit it is roughly 7x what it is at the 190px root orbit — and the body would
 * oscillate forever.
 *
 * Within a level the ratio fixes the geometry and the magnitude only fixes how fast it gets there, so
 * scaling a level's repulsion and spring together moves nothing and merely runs it faster. Depth 0 is
 * deliberately scaled up that way, 10x over its first values, which is why its spring reads oddly
 * stiff. Clients spread around the Broker ring by repulsion alone, and at the original strength that
 * crawled: three clients stranded themselves in a 130° arc with a 229° hole beside them, because
 * `REST` parked the loop long before repulsion had walked them apart. At 10x the equilibrium is
 * identical — the ring still sits at ~191-196px — but every gap is within ~10% of even, and within
 * ~5% from five clients up.
 *
 * `link` is the per-distance pull between linked spikes, and what it mostly buys is *rotation*. The
 * spring holds the ring radially — at the cap a channel link stretches a 70px orbit by about 8px and
 * no further — so the pull spends itself sliding the spike around that ring until it faces its
 * partner.
 *
 * Tangentially its only competition is sibling repulsion, which is why depth-1/2 repulsion is sized
 * against `link` rather than in isolation: one node providing several channels to the same partner
 * aims every spike the same way, and repulsion is all that stops them merging into one blob. This
 * pairing — link 0.002 against repulsion 40_000 — settles such siblings ~26px apart, clear at a 10px
 * drawn radius, while still swinging a spike through 180° in ~5s. Topics pull more gently, since a
 * topic mostly arrives in the right place by being carried there by its channel.
 *
 * All of these sit far below the spring in strength, which makes `REST` the constant deciding whether
 * they do anything at all.
 */
const LEVELS = [
    { repulsion: 2_600_000, spring: 0.055, orbit: 190, link: 0 },
    { repulsion: 40_000, spring: 0.05, orbit: 70, link: 0.002 },
    { repulsion: 8_000, spring: 0.08, orbit: 34, link: 0.0012 }
] as const;

/**
 * Distance past which a link stops pulling any harder.
 *
 * Without a cap the pull grows with separation, so a spike whose partner is across the canvas — the
 * normal case the moment two nodes sit on opposite sides of the Broker — would arrive with a
 * slingshot rather than a drift. Under the cap the force is near enough constant at every separation
 * that matters, easing off only once the two are close, which is when the drawn line no longer needs
 * help.
 */
const LINK_REACH = 200;

const DAMPING = 0.86;

/**
 * Below this kinetic energy per body the layout is at rest and the loop stops until the node set
 * changes.
 *
 * It doubles as the layout's *sensitivity floor*, which is the non-obvious part. Being a speed
 * threshold, any force too weak to push a body faster than sqrt(REST) px/frame is not merely slow but
 * invisible: the loop parks before it has moved anything. That is what made the link force, and
 * before it inter-body repulsion, a no-op at their original values.
 *
 * The bar is higher than terminal velocity suggests. Damping means a body accelerating at `a` only
 * reaches a/(1 - DAMPING) after many frames, but the rest check runs at the end of *every* frame
 * including the first, where it has reached just a * DAMPING. So a force earns its place only if
 * (a * DAMPING)² clears REST from a standing start.
 *
 * At 0.01 — this constant's first value — that floor sat at 0.1 px/frame, above every force here
 * except the springs: link attraction and inter-body repulsion both died in frame one, so spikes
 * never turned toward their partners and clients stayed bunched wherever they spawned instead of
 * spreading around the ring. The layout looked settled because it had stopped, not because it had
 * finished. 0.001 puts the floor at 0.032 px/frame, under the weak forces and still well over
 * rounding noise, at the price of a settling animation that runs seconds rather than instantly.
 */
const REST = 0.001;

export class Simulation {

    readonly bodies: Body[] = [];

    private frame = 0;

    private byId = new Map<string, Body>();

    /** Resolved once per `sync` rather than looked up per frame — membership only changes there. */
    private links: { a: Body; b: Body }[] = [];

    /** The body currently held by a pointer drag, if any — excluded from physics until released. */
    private pinned: Body | null = null;

    constructor(private readonly width: number, private readonly height: number) {}

    /**
     * Reconciles the body list against the current spec set, keeping the position of anything that
     * survived — a re-layout on every membership change would make the whole graph jump whenever
     * one client (or, for a virus node, one channel/topic) appeared.
     *
     * Order matters for newly-added bodies only: a child spawns near its parent's *current* position
     * when the parent already exists, so a topic doesn't spawn at the Broker and drift the width of
     * the canvas to reach its channel.
     *
     * Links are resolved to body references here, and any naming a body that isn't in `specs` is
     * dropped rather than kept as a dangling id — the caller builds both from the same tree, so a
     * miss means the pair is genuinely gone, not that it will turn up later.
     */
    sync(specs: readonly BodySpec[], links: readonly LinkSpec[] = []): void {
        const wanted = new Set(specs.map(spec => spec.id));

        for (const [id, body] of this.byId)
            if (!wanted.has(id)) {
                this.bodies.splice(this.bodies.indexOf(body), 1);
                this.byId.delete(id);
            }

        for (const spec of specs) {
            const existing = this.byId.get(spec.id);
            if (existing) {
                existing.parentId = spec.parentId;
                continue;
            }

            // New bodies enter on a ring around their spawn point rather than at it: coincident
            // bodies have no direction to repel along, so a shared spawn point would leave them
            // stuck together.
            const parent = spec.parentId ? this.byId.get(spec.parentId) : undefined;
            const originX = parent ? parent.x : this.width / 2;
            const originY = parent ? parent.y : this.height / 2;
            const angle = Math.random() * Math.PI * 2;
            const radius = parent ? LEVELS[1].orbit * 0.3 : LEVELS[0].orbit;

            const body: Body = {
                id: spec.id,
                parentId: spec.parentId,
                x: originX + Math.cos(angle) * radius,
                y: originY + Math.sin(angle) * radius,
                vx: 0,
                vy: 0,
                fixed: false
            };

            this.bodies.push(body);
            this.byId.set(spec.id, body);
        }

        this.links = links
            .map(link => {
                const a = this.byId.get(link.clientId);
                const b = this.byId.get(link.providerId);
                return a && b ? { a, b } : null;
            })
            .filter((link): link is { a: Body; b: Body } => link !== null);

        this.start();
    }

    start(): void {
        if (this.frame) return;
        this.frame = requestAnimationFrame(() => this.step());
    }

    /**
     * Holds a body at an explicit position for the duration of a drag — other bodies still push
     * against it (it stays in the repulsion pass) but it no longer springs toward its parent nor
     * accumulates velocity of its own, so it tracks the pointer exactly instead of fighting it.
     * Clears any earlier `fixed` from a previous drop, so re-dragging an already-placed body moves
     * it freely rather than fighting a stale fixed position.
     */
    pin(id: string, x: number, y: number): void {
        const body = this.byId.get(id);
        if (!body) return;
        this.pinned = body;
        body.fixed = false;
        body.x = x;
        body.y = y;
        body.vx = 0;
        body.vy = 0;
        this.start();
    }

    /**
     * Releases a pinned body, fixing it in place rather than handing it back to the spring — a
     * dropped node stays exactly where the operator put it instead of drifting back to its orbit.
     * Zeroed so nothing carries a stray velocity into the (skipped) integration step.
     */
    unpin(): void {
        if (this.pinned) {
            this.pinned.vx = 0;
            this.pinned.vy = 0;
            this.pinned.fixed = true;
        }
        this.pinned = null;
    }

    stop(): void {
        cancelAnimationFrame(this.frame);
        this.frame = 0;
    }

    /** 0 for a root client, 1 for a channel, 2 for a topic — how far down the tree a body sits. */
    private depthOf(body: Body): number {
        let depth = 0;
        let current = body;
        while (current.parentId) {
            const parent = this.byId.get(current.parentId);
            if (!parent) break;
            depth++;
            current = parent;
        }
        return Math.min(depth, LEVELS.length - 1);
    }

    /**
     * Draws each linked pair toward the other, so the dashed line between a client spike and its
     * provider has less of the graph to cross — the reason the force exists is the *line*, not the
     * spikes. It lands almost entirely as rotation: the parent spring holds each spike on its own
     * orbit, so what the pull can actually change is where on that ring the spike sits, which swings
     * both ends around to face each other and keeps the line off the nodes in between.
     *
     * Runs before the main pass rather than after it so the velocity it adds is damped, integrated
     * and counted toward `energy` in the same frame — added afterwards it would let the loop declare
     * the layout at rest while links were still pulling, and the graph would freeze half-aligned.
     * Strength comes from the shallower end of the pair, matching the repulsion rule for the same
     * reason: the weaker level's spring is the one that would lose control if outmatched.
     */
    private applyLinkForces(): void {
        for (const { a, b } of this.links) {
            const dx = b.x - a.x;
            const dy = b.y - a.y;
            const distance = Math.hypot(dx, dy);
            if (distance < 1) continue;

            const strength = Math.min(LEVELS[this.depthOf(a)]!.link, LEVELS[this.depthOf(b)]!.link);
            const pull = Math.min(distance, LINK_REACH) * strength;
            const fx = (dx / distance) * pull;
            const fy = (dy / distance) * pull;

            a.vx += fx;
            a.vy += fy;
            b.vx -= fx;
            b.vy -= fy;
        }
    }

    private step(): void {
        const centreX = this.width / 2;
        const centreY = this.height / 2;
        let energy = 0;

        const depths = this.bodies.map(body => this.depthOf(body));

        this.applyLinkForces();

        for (let i = 0; i < this.bodies.length; i++) {
            const body = this.bodies[i]!;
            const bodyLevel = LEVELS[depths[i]!]!;

            for (let j = i + 1; j < this.bodies.length; j++) {
                const other = this.bodies[j]!;
                let dx = body.x - other.x;
                let dy = body.y - other.y;
                let distanceSquared = dx * dx + dy * dy;

                // Two bodies can land exactly on top of each other; nudge them apart rather than
                // dividing by zero and sending both to NaN, which would blank the whole canvas.
                if (distanceSquared < 1) {
                    dx = Math.random() - 0.5;
                    dy = Math.random() - 0.5;
                    distanceSquared = 1;
                }

                // Repulsion strength is the weaker of the two bodies' own levels — a topic brushing
                // past an unrelated root client repels at the topic's gentle short-range strength,
                // not the client's, or it would fly off far past its own spring's reach.
                const otherLevel = LEVELS[depths[j]!]!;
                const repulsion = Math.min(bodyLevel.repulsion, otherLevel.repulsion);

                const distance = Math.sqrt(distanceSquared);
                const force = repulsion / distanceSquared;
                const fx = (dx / distance) * force;
                const fy = (dy / distance) * force;

                body.vx += fx / 1000;
                body.vy += fy / 1000;
                other.vx -= fx / 1000;
                other.vy -= fy / 1000;
            }

            // A pinned body is driven by the pointer, not the physics — it neither springs toward
            // its parent nor integrates its own velocity, or it would fight the drag. A fixed body
            // is the same, permanently: dropping a drag fixes it in place (see unpin) instead of
            // handing it back to the spring, so it doesn't drift back to its orbit after release.
            if (body === this.pinned || body.fixed) continue;

            // The spring pulls toward an orbit around the body's own parent (the Broker's fixed
            // centre for a root client, or a live, moving parent body for a channel/topic) — not
            // toward the centre itself. The error term is signed, so a body inside the ring is
            // pushed out just as one outside is pulled in.
            const parent = body.parentId ? this.byId.get(body.parentId) : undefined;
            const targetX = parent ? parent.x : centreX;
            const targetY = parent ? parent.y : centreY;

            const dx = targetX - body.x;
            const dy = targetY - body.y;
            const distance = Math.sqrt(dx * dx + dy * dy) || 1;
            const stretch = distance - bodyLevel.orbit;

            body.vx += (dx / distance) * stretch * bodyLevel.spring;
            body.vy += (dy / distance) * stretch * bodyLevel.spring;

            body.vx *= DAMPING;
            body.vy *= DAMPING;

            body.x += body.vx;
            body.y += body.vy;

            energy += body.vx * body.vx + body.vy * body.vy;
        }

        tick.value++;

        // Settle rather than burn a frame budget forever on a static graph; sync() restarts the
        // loop whenever membership changes. A pin keeps the loop alive regardless of energy, since
        // bodies repelled by the dragged one need to keep reacting every frame.
        if (!this.pinned && energy < REST * this.bodies.length) {
            this.frame = 0;
            return;
        }

        this.frame = requestAnimationFrame(() => this.step());
    }

}
