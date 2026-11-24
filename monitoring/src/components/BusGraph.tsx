import {useComputed, useSignal} from "@preact/signals";
import {useEffect, useMemo, useRef} from "preact/hooks";
import type {DynamicNode} from "../graph/dynamicNodes";
import {
    discardDynamicNode,
    dynamicNodes,
    openDynamicNodeWindow,
    removeDynamicNode
} from "../graph/dynamicNodes";
import {openExternalNodeWindow} from "../graph/externalNodes";
import {type Body, Simulation, tick} from "../graph/simulation";
import {brokerRouteId, clientList, connectionState, selectedRouteId} from "../graph/store";
import {PULSE_DURATION_MS, pulses, pulseTick} from "../graph/traffic";
import {buildVirusTree, type GraphNode} from "../graph/virusTree";

const WIDTH = 1200;
const HEIGHT = 800;
const BROKER_RADIUS = 46;
const CLIENT_RADIUS = 16;
const PROVIDER_RADIUS = 20;
const CHANNEL_RADIUS = 10;
const TOPIC_RADIUS = 6;
const GRID_SIZE = 24;
const MIN_SCALE = 0.25;
const MAX_SCALE = 4;
const PULSE_RADIUS = 4;
// styles.css grows a hovered node (and a selected spike) with a `transform: scale`, which the edge
// trimming below has to know about: it computes the rim in JS and cannot read a CSS transform, so a
// value out of step with `.client:hover .client-node` / `.spike:hover .spike-node` leaves the line
// ending short of the grown circle.
const CLIENT_HOVER_GROWTH = 1.12;
const SPIKE_HOVER_GROWTH = 1.2;

/** Pan/zoom applied to the graph's own `<g>`, independent of the fixed viewBox above it. */
interface View {
    x: number;
    y: number;
    scale: number;
}

/**
 * The graph canvas: the Broker as one large node at the centre, every connected client orbiting it.
 *
 * A dynamic node grows a "virus" spike tree — one satellite per channel it joined, a smaller one per
 * topic on that channel — all in the same force simulation. `graph/virusTree.ts` builds the body-spec
 * tree; `Simulation` springs each body toward its own parent rather than uniformly toward the Broker.
 *
 * The dashed client/provider links are also fed to the simulation as an attraction, which is what
 * keeps them short and roughly radial instead of cutting across unrelated nodes. The pairing is
 * computed once, in `virusTree`, so the line drawn here and the force applied there can never be
 * between different bodies.
 *
 * A fixed-size viewBox with preserveAspectRatio does the responsive work, so the simulation runs in a
 * stable coordinate space and resizing the window never perturbs the layout. User pan/zoom is a
 * separate transform on an inner `<g>` — the `view` signal — layered on top rather than replacing it,
 * so the simulation never needs to know the graph is scaled or panned.
 *
 * Edges are drawn before nodes, so the circles paint over the line ends rather than the lines
 * crossing the fills. `vector-effect="non-scaling-stroke"` on every stroked element keeps line weight
 * constant under both the viewBox fit and the user's zoom.
 *
 * Dragging a node pins its body (`Simulation.pin`/`unpin`) so it tracks the pointer exactly instead
 * of fighting the spring, and releases back to the physics on pointerup.
 */
export function BusGraph() {
    const simulation = useMemo(() => new Simulation(WIDTH, HEIGHT), []);
    const hovered = useSignal<string | null>(null);
    // Separate from `hovered`, which also drives edge and label dimming for every node kind. Only a
    // dynamic node's popover needs "stay open while the pointer crosses from node to popover", and
    // gating that on the general-purpose signal would close it when the pointer leaves the circle,
    // before it ever reaches the popover's own buttons.
    const popoverPinned = useSignal<string | null>(null);
    // The popover sits a few px above the node circle, so a straight-line mouse move between them
    // crosses a gap neither element owns. Closing on the node's onMouseLeave would fire before the
    // popover's onMouseEnter could cancel it, so a short grace period covers that transit.
    const popoverCloseTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
    const dragging = useSignal<string | null>(null);
    const svgRef = useRef<SVGSVGElement>(null);
    const view = useSignal<View>({ x: 0, y: 0, scale: 1 });

    function openPopover(id: string) {
        if (popoverCloseTimer.current) {
            clearTimeout(popoverCloseTimer.current);
            popoverCloseTimer.current = null;
        }
        popoverPinned.value = id;
    }

    function schedulePopoverClose() {
        if (popoverCloseTimer.current) clearTimeout(popoverCloseTimer.current);
        popoverCloseTimer.current = setTimeout(() => {
            popoverPinned.value = null;
            popoverCloseTimer.current = null;
        }, 300);
    }

    useEffect(() => () => {
        if (popoverCloseTimer.current) clearTimeout(popoverCloseTimer.current);
    }, []);

    const tree = useComputed(() => buildVirusTree(clientList.value, dynamicNodes.value));

    useEffect(() => {
        simulation.sync(tree.value.specs, tree.value.links);
    }, [tree.value]);

    useEffect(() => () => simulation.stop(), []);

    // Reading tick.value is what subscribes this component to the simulation's per-frame updates. The
    // positions themselves are plain mutable objects, deliberately outside the signal graph.
    //
    // pulseTick is the same arrangement for traffic, and a *second* signal rather than the same one
    // because the two loops start and stop independently: the simulation rests when the layout
    // settles, which is exactly when a dot most needs to keep moving.
    tick.value;
    pulseTick.value;

    // One reading per render, so every dot on this frame is placed against the same instant.
    const now = performance.now();

    function select(id: string | "broker") {
        selectedRouteId.value = selectedRouteId.value === id ? null : id;
    }

    function closePopoverNow() {
        if (popoverCloseTimer.current) {
            clearTimeout(popoverCloseTimer.current);
            popoverCloseTimer.current = null;
        }
        popoverPinned.value = null;
    }

    function editDynamicNode(id: string) {
        openDynamicNodeWindow(id);
        closePopoverNow();
    }

    /**
     * The only thing in the app that ends a node.
     *
     * The hover popover's delete button is the caller, and the only surface a node still has once its
     * window is away — the window's own controls all just hide it.
     *
     * Asks the node to go and waits for the Broker to confirm it did. On an already-closing node this
     * is the force path instead: the confirming event is the only thing that would ever remove it, so
     * a second click has to be able to do it by hand when that event cannot arrive — the monitor's
     * own connection being down, most obviously.
     */
    function deleteDynamicNode(node: DynamicNode) {
        if (node.closing) removeDynamicNode(node.id);
        else discardDynamicNode(node.id);

        // Either call may or may not have actually dropped it — a graceful close usually leaves it
        // listed and waiting — so the outcome is read rather than predicted. A selection pointing at
        // a body that no longer exists would dim the whole graph with nothing to un-dim it.
        if (selectedRouteId.value === node.id && !dynamicNodes.value.some(n => n.id === node.id)) selectedRouteId.value = null;
        closePopoverNow();
    }

    function parentOf(body: Body): Body | undefined {
        return body.parentId ? simulation.bodies.find(b => b.id === body.parentId) : undefined;
    }

    /**
     * The radius a body is actually drawn at this frame — `radiusOf` plus whatever the hover/selected
     * `transform: scale` in styles.css is adding to it.
     *
     * Every line endpoint is trimmed with this rather than the base radius, so a node growing under
     * the pointer takes the lines meeting it along instead of pulling its rim out from under them.
     * Takes the id beside the node because a `GraphNode` doesn't carry one — the tree keys them.
     */
    function drawnRadiusOf(id: string, node: GraphNode): number {
        const radius = radiusOf(node);
        if (node.kind === "client") return hovered.value === id ? radius * CLIENT_HOVER_GROWTH : radius;
        return hovered.value === id || selectedRouteId.value === id ? radius * SPIKE_HOVER_GROWTH : radius;
    }

    /**
     * Renders one dashed line between a client-role and provider-role spike sharing a name.
     *
     * Serves both a channel link and a topic link — same shape, differing only in CSS class and in
     * the radius each end is trimmed by, which is read per end: a topic spike is drawn smaller than
     * a channel spike, and either end may be the one currently grown under the pointer.
     */
    function renderRoleLink(link: { clientId: string; providerId: string }, className: string) {
        const client = simulation.bodies.find(b => b.id === link.clientId);
        const provider = simulation.bodies.find(b => b.id === link.providerId);
        const clientNode = tree.value.nodes.get(link.clientId);
        const providerNode = tree.value.nodes.get(link.providerId);
        if (!client || !provider || !clientNode || !providerNode) return null;

        const dx = provider.x - client.x;
        const dy = provider.y - client.y;
        const length = Math.hypot(dx, dy) || 1;
        const ux = dx / length;
        const uy = dy / length;
        const startX = client.x + ux * drawnRadiusOf(link.clientId, clientNode);
        const startY = client.y + uy * drawnRadiusOf(link.clientId, clientNode);
        const endX = provider.x - ux * drawnRadiusOf(link.providerId, providerNode);
        const endY = provider.y - uy * drawnRadiusOf(link.providerId, providerNode);

        const active = hovered.value === link.clientId || hovered.value === link.providerId;
        const dimmed = (hovered.value !== null || selectedRouteId.value !== null) && !active;

        return (
            <path
                key={`${link.clientId}-${link.providerId}`}
                d={`M${startX} ${startY} L${endX} ${endY}`}
                fill="none"
                vector-effect="non-scaling-stroke"
                class={[className, active && `${className}--active`, dimmed && `${className}--dimmed`].filter(Boolean).join(" ")}
            />
        );
    }

    /**
     * The polyline a traffic dot travels: from the body the message concerns, up through each of its
     * parents, ending at the Broker's fixed centre.
     *
     * The same chain `.edges` draws an edge for at every step, so a dot rides the visible lines
     * rather than cutting across the canvas — a topic-addressed message visibly leaves its topic
     * spike, crosses its channel spike and its node, and only then heads for the Broker.
     *
     * Read off live body positions every frame rather than resolved once when the pulse was reported,
     * so a dot tracks a node that is still settling or being dragged.
     *
     * Answers null for a body that has since left the graph: the pulse simply isn't drawn, and
     * expires on its own.
     */
    function pulsePath(originId: string): { x: number; y: number }[] | null {
        const origin = simulation.bodies.find(b => b.id === originId);
        if (!origin) return null;

        const points: { x: number; y: number }[] = [];
        for (let body: Body | undefined = origin; body; body = parentOf(body)) points.push({ x: body.x, y: body.y });
        points.push({ x: WIDTH / 2, y: HEIGHT / 2 });

        return points;
    }

    /**
     * The uniform scale and letterbox offset the SVG's own `xMidYMid meet` viewBox fit applies,
     * recovered from the rendered element's rect.
     *
     * Shared by every screen-to-viewBox conversion below, because a screen-pixel *delta* needs the
     * same `viewScale` divided out that an absolute point does. `onPanBackground` learned that the
     * hard way: it used to add raw screen-pixel deltas straight to `view.value`, which are viewBox
     * units, so panning visibly lagged the pointer on any canvas rendered smaller than the fixed
     * viewBox — the normal case.
     */
    function viewboxFit(): { rect: DOMRect; viewScale: number; offsetX: number; offsetY: number } {
        const svg = svgRef.current!;
        const rect = svg.getBoundingClientRect();
        const viewScale = Math.min(rect.width / WIDTH, rect.height / HEIGHT);
        const offsetX = (rect.width - WIDTH * viewScale) / 2;
        const offsetY = (rect.height - HEIGHT * viewScale) / 2;
        return { rect, viewScale, offsetX, offsetY };
    }

    /**
     * Client (screen) pixels to the graph's own coordinate space — the fixed viewBox further
     * transformed by the current pan/zoom `<g>`.
     *
     * Every drag needs it, panning the canvas and moving a node alike, so pointer motion tracks 1:1
     * with the graph regardless of scale.
     */
    function toGraphSpace(clientX: number, clientY: number): { x: number; y: number } {
        const { rect, viewScale, offsetX, offsetY } = viewboxFit();
        const vx = (clientX - rect.left - offsetX) / viewScale;
        const vy = (clientY - rect.top - offsetY) / viewScale;
        return { x: (vx - view.value.x) / view.value.scale, y: (vy - view.value.y) / view.value.scale };
    }

    /**
     * Inverse of `toGraphSpace`, relative to `.graph-container` rather than the viewport.
     *
     * The hover popover uses it, which is what lets that popover be an ordinary
     * absolutely-positioned HTML sibling of the `<svg>` instead of living inside it — SVG has no
     * button or box-shadow ergonomics worth fighting for.
     *
     * `.graph-container` is the popover's positioned ancestor, and — with no border or padding — the
     * same origin `.canvas-stack` uses for `.node-window`.
     */
    function toContainerSpace(graphX: number, graphY: number): { x: number; y: number } {
        const svg = svgRef.current!;
        const container = svg.parentElement!.getBoundingClientRect();
        const { rect, viewScale, offsetX, offsetY } = viewboxFit();
        const vx = graphX * view.value.scale + view.value.x;
        const vy = graphY * view.value.scale + view.value.y;
        return {
            x: rect.left - container.left + offsetX + vx * viewScale,
            y: rect.top - container.top + offsetY + vy * viewScale
        };
    }

    function onWheelZoom(event: WheelEvent) {
        event.preventDefault();
        const { x: gx, y: gy } = toGraphSpace(event.clientX, event.clientY);
        const factor = Math.exp(-event.deltaY * 0.001);
        const nextScale = Math.min(MAX_SCALE, Math.max(MIN_SCALE, view.value.scale * factor));
        // x/y are re-derived from the *same* graph point, so it stays under the cursor after
        // rescaling rather than zooming around the viewBox centre.
        view.value = { scale: nextScale, x: view.value.x - gx * (nextScale - view.value.scale), y: view.value.y - gy * (nextScale - view.value.scale) };
    }

    function onPanBackground(event: PointerEvent) {
        const target = event.target as Element;
        // The Broker sits at a fixed point (WIDTH/2, HEIGHT/2) rather than a draggable body — a
        // press on it pans the canvas exactly like a press on empty background, both to keep the
        // interaction consistent (nothing else in the graph is drag-fixed like this) and because an
        // SVG shape left with no pointerdown handling of its own falls back to the browser's native
        // image drag-and-drop, which is what a plain onClick here would otherwise allow through.
        const isBroker = target.closest(".broker") !== null;
        if (!isBroker && target !== event.currentTarget && !target.classList.contains("graph-bg")) return;
        // Without this, a fast press-drag over a <text> label or shape is sometimes read by the
        // browser as the start of native drag-and-drop (or a text selection) instead of a pan.
        event.preventDefault();
        selectedRouteId.value = null;

        const svg = event.currentTarget as SVGSVGElement;
        svg.setPointerCapture(event.pointerId);
        const startX = event.clientX;
        const startY = event.clientY;
        const origin = view.value;
        // A screen-pixel delta is in rendered pixels; view.x/y are viewBox units, one further
        // remove behind the SVG's own xMidYMid-meet fit — dividing by viewScale is what makes the
        // point under the cursor the one that stays under it (see viewboxFit's doc block).
        const { viewScale } = viewboxFit();

        function onMove(moveEvent: PointerEvent) {
            view.value = {
                ...origin,
                x: origin.x + (moveEvent.clientX - startX) / viewScale,
                y: origin.y + (moveEvent.clientY - startY) / viewScale
            };
        }

        function onRelease() {
            svg.removeEventListener("pointermove", onMove);
            svg.removeEventListener("lostpointercapture", onRelease);
        }

        svg.addEventListener("pointermove", onMove);
        svg.addEventListener("lostpointercapture", onRelease);
    }

    function onDragNode(event: PointerEvent, id: string) {
        event.stopPropagation();
        event.preventDefault();
        const target = event.currentTarget as SVGGElement;
        target.setPointerCapture(event.pointerId);
        dragging.value = id;

        function onMove(moveEvent: PointerEvent) {
            const { x, y } = toGraphSpace(moveEvent.clientX, moveEvent.clientY);
            simulation.pin(id, x, y);
        }

        // lostpointercapture rather than pointerup: it fires whenever capture ends for *any* reason
        // (a normal release, a pointercancel, the browser stealing capture mid-gesture) — pointerup
        // alone misses the cancel case and leaves the body pinned and `dragging` stuck forever, with
        // no further input able to clear it since the listeners were only ever armed for "up".
        function onRelease() {
            target.removeEventListener("pointermove", onMove);
            target.removeEventListener("lostpointercapture", onRelease);
            simulation.unpin();
            dragging.value = null;
        }

        target.addEventListener("pointermove", onMove);
        target.addEventListener("lostpointercapture", onRelease);
    }

    // Not on the bus: added but not registered yet, or dropped and
    // reconnecting. A closing node counts too, and cannot be read off
    // `client` — its socket is already gone but its `clients` entry lives
    // until the Broker's own event arrives, which is the very wait this
    // state exists to show.
    //
    // Narrowed to the client arm rather than taking a bare GraphNode: `client` and `dynamicNode`
    // only exist there, and being on the bus is only a question a client node can be asked — a
    // channel or topic spike is drawn off its own node's state and has no connection of its own.
    function isPending(node: Extract<GraphNode, { kind: "client" }>) {
        return node.client === null || node.dynamicNode?.closing || false;
    }

    return (
        <div class="graph-container">
            {/* The spawn buttons that used to sit here are `CanvasToolbar` now, rendered by `App`
                against the canvas rather than the graph — neither of them is about the graph, and
                inside here they disappeared with it. */}
            <svg
                ref={svgRef}
                class="graph"
                viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
                preserveAspectRatio="xMidYMid meet"
                onWheel={onWheelZoom}
                onPointerDown={onPanBackground}
            >
                <defs>
                    <pattern id="bg-grid" width={GRID_SIZE} height={GRID_SIZE} patternUnits="userSpaceOnUse">
                        <circle cx="1" cy="1" r="1" fill="var(--border-raised)" />
                    </pattern>
                </defs>

                <rect class="graph-bg" width={WIDTH} height={HEIGHT} fill="url(#bg-grid)" opacity="0.6" />

                <g transform={`translate(${view.value.x} ${view.value.y}) scale(${view.value.scale})`}>
                <g class="edges">
                    {simulation.bodies.map(body => {
                        const node = tree.value.nodes.get(body.id);
                        if (!node) return null;

                        // A client's edge always goes to the Broker's fixed centre; a channel/topic's
                        // edge goes to its live parent body instead — the "virus" spike itself.
                        const parent = parentOf(body);
                        const cx = parent ? parent.x : WIDTH / 2;
                        const cy = parent ? parent.y : HEIGHT / 2;
                        const parentRadius = parent ? drawnRadiusOf(parent.id, tree.value.nodes.get(parent.id)!) : BROKER_RADIUS;
                        const radius = drawnRadiusOf(body.id, node);

                        // Trim each endpoint back along the line so it meets the circle's rim exactly
                        // instead of the stroke burying itself under the fill.
                        const dx = body.x - cx;
                        const dy = body.y - cy;
                        const length = Math.hypot(dx, dy) || 1;
                        const ux = dx / length;
                        const uy = dy / length;
                        const startX = cx + ux * parentRadius;
                        const startY = cy + uy * parentRadius;
                        const endX = body.x - ux * radius;
                        const endY = body.y - uy * radius;

                        const active = hovered.value === body.id || selectedRouteId.value === body.id;
                        const dimmed =
                            (hovered.value !== null || selectedRouteId.value !== null) && !active;

                        return (
                            <path
                                key={body.id}
                                d={`M${startX} ${startY} L${endX} ${endY}`}
                                fill="none"
                                vector-effect="non-scaling-stroke"
                                class={[
                                    "edge",
                                    node.kind !== "client" && "edge--spike",
                                    node.kind === "client" && isPending(node) && "edge--pending",
                                    active && "edge--active",
                                    dimmed && "edge--dimmed"
                                ]
                                    .filter(Boolean)
                                    .join(" ")}
                            />
                        );
                    })}
                </g>

                <g class="channel-links">
                    {tree.value.links
                        .filter(link => link.kind === "channel")
                        .map(link => renderRoleLink(link, "channel-link"))}
                </g>

                <g class="topic-links">
                    {tree.value.links.filter(link => link.kind === "topic").map(link => renderRoleLink(link, "topic-link"))}
                </g>

                <g class="broker" onClick={() => select("broker")} style={{ cursor: "pointer" }}>
                    <circle cx={WIDTH / 2} cy={HEIGHT / 2} r={BROKER_RADIUS} class="broker-node" vector-effect="non-scaling-stroke" />
                    {selectedRouteId.value === "broker" && (
                        <circle
                            cx={WIDTH / 2}
                            cy={HEIGHT / 2}
                            r={BROKER_RADIUS + 14}
                            fill="none"
                            stroke="var(--node-broker)"
                            stroke-width="1.5"
                            stroke-opacity="0.4"
                            vector-effect="non-scaling-stroke"
                        />
                    )}
                    <text x={WIDTH / 2} y={HEIGHT / 2 + 5} class="broker-label">
                        Broker
                    </text>
                    <text x={WIDTH / 2} y={HEIGHT / 2 + BROKER_RADIUS + 22} class="broker-sub">
                        {connectionState.value === "open" ? `${clientList.value.length} connected` : connectionState.value}
                    </text>
                </g>

                <g class="clients">
                    {simulation.bodies.map(body => {
                        const node = tree.value.nodes.get(body.id);
                        if (!node) return null;

                        const isSelected = selectedRouteId.value === body.id;
                        const isHovered = hovered.value === body.id;
                        const isDragging = dragging.value === body.id;
                        const dimmed = (hovered.value !== null || selectedRouteId.value !== null) && !isHovered && !isSelected;

                        if (node.kind === "client") {
                            const { client, dynamicNode } = node;
                            // A disconnected node has no ClientNode to read channels off, so its
                            // own reported channels stand in — it keeps the size it had rather than
                            // shrinking to a plain client the instant its socket drops.
                            const isProvider = (client ? client.channels.length : dynamicNode?.channels.length ?? 0) > 0;
                            const radius = isProvider ? PROVIDER_RADIUS : CLIENT_RADIUS;
                            const isSelf = body.id === brokerRouteId.value;
                            // An external client is a real bus participant this app didn't spawn —
                            // it wears the same hover popover a dynamic node does, but with a view
                            // (not edit) affordance, and never a delete one (see the popover below).
                            const isExternal = client !== null && dynamicNode === null && !isSelf;

                            return (
                                <g
                                    key={body.id}
                                    transform={`translate(${body.x} ${body.y})`}
                                    class={[
                                        "client",
                                        isProvider && "client--provider",
                                        isSelf && "client--self",
                                        isSelected && "client--selected",
                                        isDragging && "client--dragging",
                                        dimmed && "client--dimmed",
                                        isPending(node) && "client--pending",
                                        dynamicNode && !dynamicNode.windowOpen && !dynamicNode.closing && "client--minimized"
                                    ]
                                        .filter(Boolean)
                                        .join(" ")}
                                    tabIndex={0}
                                    onMouseEnter={() => {
                                        hovered.value = body.id;
                                        if (dynamicNode) openPopover(dynamicNode.id);
                                        else if (isExternal) openPopover(body.id);
                                    }}
                                    onMouseLeave={() => {
                                        hovered.value = null;
                                        if (dynamicNode || isExternal) schedulePopoverClose();
                                    }}
                                    onPointerDown={event => onDragNode(event, body.id)}
                                    onClick={event => {
                                        event.stopPropagation();
                                        select(body.id);
                                    }}
                                    onDblClick={event => {
                                        event.stopPropagation();
                                        // A dynamic node's window can be minimized (its − button) while
                                        // the node itself stays on the bus — double-clicking it, or the
                                        // hover popover's edit button, is how the window comes back.
                                        if (dynamicNode) openDynamicNodeWindow(dynamicNode.id);
                                        else if (isExternal) openExternalNodeWindow(body.id);
                                    }}
                                    onKeyDown={event => {
                                        if (event.key === "Enter" || event.key === " ") {
                                            event.preventDefault();
                                            select(body.id);
                                        }
                                    }}
                                >
                                    {isSelected && (
                                        <circle
                                            r={radius + 8}
                                            class="client-halo"
                                            fill="none"
                                            stroke="currentColor"
                                            stroke-width="1.5"
                                            stroke-opacity="0.4"
                                            vector-effect="non-scaling-stroke"
                                        />
                                    )}
                                    <circle r={radius} class="client-node" vector-effect="non-scaling-stroke" />
                                    {isSelected && <circle r="3" class="client-center-dot" />}
                                    {dynamicNode && !dynamicNode.windowOpen && !dynamicNode.closing && (
                                        // A minimized dynamic node is still a live, controllable participant
                                        // (its bus connection never drops — see DynamicNode.tsx) — this
                                        // badge is the only on-graph cue that it isn't just an inert client, and
                                        // doubles as a one-click way back into its window instead of relying on
                                        // the easily-missed double-click.
                                        <g
                                            class="client-minimized-badge"
                                            transform={`translate(${radius * 0.7} ${-radius * 0.7})`}
                                            onClick={event => {
                                                event.stopPropagation();
                                                openDynamicNodeWindow(dynamicNode.id);
                                            }}
                                        >
                                            <circle r="7" />
                                            <text y="3">&#x25a2;</text>
                                        </g>
                                    )}
                                    <text y={radius + 16} class="client-label">
                                        {isSelf ? (node.client!.info?.identity ?? label(node)) : label(node)}
                                    </text>
                                </g>
                            );
                        }

                        if (node.kind === "channel") {
                            return (
                                <g
                                    key={body.id}
                                    transform={`translate(${body.x} ${body.y})`}
                                    class={[
                                        "spike",
                                        "spike--channel",
                                        `spike--${node.role}`,
                                        isSelected && "spike--selected",
                                        dimmed && "spike--dimmed"
                                    ]
                                        .filter(Boolean)
                                        .join(" ")}
                                    tabIndex={0}
                                    onMouseEnter={() => (hovered.value = body.id)}
                                    onMouseLeave={() => (hovered.value = null)}
                                    onPointerDown={event => onDragNode(event, body.id)}
                                    onClick={event => {
                                        event.stopPropagation();
                                        select(body.id);
                                    }}
                                >
                                    <circle r={CHANNEL_RADIUS} class="spike-node" vector-effect="non-scaling-stroke" />
                                    <text y={CHANNEL_RADIUS + 13} class="spike-label">
                                        {node.channelName}
                                    </text>
                                </g>
                            );
                        }

                        return (
                            <g
                                key={body.id}
                                transform={`translate(${body.x} ${body.y})`}
                                class={["spike", "spike--topic", isSelected && "spike--selected", dimmed && "spike--dimmed"]
                                    .filter(Boolean)
                                    .join(" ")}
                                tabIndex={0}
                                onMouseEnter={() => (hovered.value = body.id)}
                                onMouseLeave={() => (hovered.value = null)}
                                onPointerDown={event => onDragNode(event, body.id)}
                                onClick={event => {
                                    event.stopPropagation();
                                    select(body.id);
                                }}
                            >
                                <circle r={TOPIC_RADIUS} class="spike-node" vector-effect="non-scaling-stroke" />
                                <text y={TOPIC_RADIUS + 12} class="spike-label spike-label--topic">
                                    {node.topic}
                                </text>
                            </g>
                        );
                    })}
                </g>

                {/* Painted after the nodes so a dot crossing a circle stays on top of it rather
                    than disappearing under the fill for the length of a node. */}
                <g class="pulses">
                    {pulses.map(pulse => {
                        const path = pulsePath(pulse.originId);
                        if (!path) return null;

                        // An inbound dot waiting on the send it answers (see traffic.ts `handoff`)
                        // is not drawn at all rather than parked at the Broker: the message is *in*
                        // the Broker for that gap, and the outbound dot has just arrived there.
                        if (now < pulse.start) return null;

                        // Clamped because a pulse is drawn on the frame that also retires it.
                        const progress = Math.min(1, (now - pulse.start) / PULSE_DURATION_MS);
                        // The path is built origin-first, which is the outbound direction; an
                        // inbound dot is the same line walked back from the Broker.
                        const { x, y } = pointAlong(path, pulse.direction === "out" ? progress : 1 - progress);

                        return <circle key={pulse.id} cx={x} cy={y} r={PULSE_RADIUS} class={`pulse pulse--${pulse.direction}`} />;
                    })}
                </g>

                </g>
            </svg>

            {popoverPinned.value && (() => {
                const dynamicNode = dynamicNodes.value.find(n => n.id === popoverPinned.value);
                if (dynamicNode) {
                    const body = simulation.bodies.find(b => b.id === dynamicNode.id);
                    if (!body) return null;
                    const radius = dynamicNode.channels.length > 0 ? PROVIDER_RADIUS : CLIENT_RADIUS;
                    const { x, y } = toContainerSpace(body.x, body.y - radius);

                    return (
                        <div
                            class="node-popover"
                            style={{ left: `${x}px`, top: `${y}px` }}
                            onMouseEnter={() => openPopover(dynamicNode.id)}
                            onMouseLeave={() => schedulePopoverClose()}
                        >
                            {/* Nothing to reopen once the window (and the connection in it) is gone. */}
                            {!dynamicNode.closing && (
                                <button class="node-popover-btn node-popover-edit" onClick={() => editDynamicNode(dynamicNode.id)} aria-label="edit node" title="Edit">
                                    &#x270e;
                                </button>
                            )}
                            <button
                                class="node-popover-btn node-popover-delete"
                                onClick={() => deleteDynamicNode(dynamicNode)}
                                aria-label="delete node"
                                title={dynamicNode.closing ? "Force remove (waiting on the Broker)" : "Delete"}
                            >
                                &#x2715;
                            </button>
                        </div>
                    );
                }

                // Not a dynamic node — an external client's popover instead, view-only: there is
                // nothing here to edit or delete, only a window to open onto what the Broker already
                // shows this connection providing (see ExternalNodeApp).
                const client = clientList.value.find(c => c.routeId === popoverPinned.value);
                if (!client) return null;
                const body = simulation.bodies.find(b => b.id === client.routeId);
                if (!body) return null;
                const radius = client.channels.length > 0 ? PROVIDER_RADIUS : CLIENT_RADIUS;
                const { x, y } = toContainerSpace(body.x, body.y - radius);

                return (
                    <div
                        class="node-popover"
                        style={{ left: `${x}px`, top: `${y}px` }}
                        onMouseEnter={() => openPopover(client.routeId)}
                        onMouseLeave={() => schedulePopoverClose()}
                    >
                        <button
                            class="node-popover-btn node-popover-view"
                            onClick={() => {
                                openExternalNodeWindow(client.routeId);
                                closePopoverNow();
                            }}
                            aria-label="view node"
                            title="View"
                        >
                            <svg viewBox="0 0 16 16" width="13" height="13" aria-hidden="true">
                                <path d="M1.5 8S4 3 8 3s6.5 5 6.5 5-2.5 5-6.5 5-6.5-5-6.5-5Z" />
                                <circle cx="8" cy="8" r="2" />
                            </svg>
                        </button>
                    </div>
                );
            })()}
        </div>
    );
}

/**
 * The client's declared Identity — the stable "{program}@{version}"-shaped handle it registers
 * under (see ClientInfo.identity in the wire-contract skill), so the same program reads as the same
 * label across a reconnect rather than as a fresh RouteId prefix. Falls back to a short RouteId
 * prefix — at least enough to correlate against a log line — for a client whose enriched ClientInfo
 * hasn't arrived yet (info is populated from the same client.connected event as the node itself, so
 * this should be rare rather than never).
 *
 * A dynamic node is labeled by its own title instead, for the whole of its life: it has no RouteId
 * to show before it registers, and switching from the title to an identity/hex label at the moment
 * the ack lands would make the node look like it had been replaced by a different one.
 */
function label(node: Extract<GraphNode, { kind: "client" }>): string {
    if (node.dynamicNode) return node.dynamicNode.title;
    return node.client!.info?.identity ?? node.client!.routeId.substring(0, 8);
}

/**
 * The point a fraction `t` of the way along a polyline, measured by **arc length** rather than by
 * segment count: a dot's speed has to stay constant across the whole trip, and a topic spike sits
 * ~34px from its channel where a client sits ~190px from the Broker, so dividing the time evenly
 * between segments would make the same dot crawl over the short hops and bolt over the long one.
 */
function pointAlong(points: readonly { x: number; y: number }[], t: number): { x: number; y: number } {
    const spans: number[] = [];
    let total = 0;

    for (let i = 1; i < points.length; i++) {
        const length = Math.hypot(points[i]!.x - points[i - 1]!.x, points[i]!.y - points[i - 1]!.y);
        spans.push(length);
        total += length;
    }

    // A zero-length path — every body coincident with the Broker's centre — has no direction to
    // travel in, and dividing by it would put the dot at NaN and blank the whole canvas.
    if (total === 0) return points[0]!;

    let travelled = t * total;
    for (let i = 0; i < spans.length; i++) {
        const span = spans[i]!;
        if (travelled <= span) {
            const fraction = span === 0 ? 0 : travelled / span;
            return {
                x: points[i]!.x + (points[i + 1]!.x - points[i]!.x) * fraction,
                y: points[i]!.y + (points[i + 1]!.y - points[i]!.y) * fraction
            };
        }
        travelled -= span;
    }

    return points[points.length - 1]!;
}

/** The drawn circle radius for a body — matches the `<circle r={...}>` each node kind renders. */
function radiusOf(node: GraphNode): number {
    switch (node.kind) {
        case "client":
            return (node.client ? node.client.channels.length : node.dynamicNode?.channels.length ?? 0) > 0
                ? PROVIDER_RADIUS
                : CLIENT_RADIUS;
        case "channel":
            return CHANNEL_RADIUS;
        case "topic":
            return TOPIC_RADIUS;
    }
}

export type { GraphNode };
