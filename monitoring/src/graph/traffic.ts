import { signal } from "@preact/signals";
import { channelBodyId, topicBodyId } from "./virusTree";

/**
 * Message traffic, as short-lived dots riding the graph's own edges: a yellow one travelling from a
 * sender out to the Broker, a purple one travelling from the Broker in to a destination.
 *
 * The source is local, not the Broker, and that is the constraint to know. The event plane is
 * presence-only, so nothing on the wire reports that a message was routed, and the monitor's own
 * connection sees only frames addressed to it. What is observable is the dynamic nodes: `NodeApp`
 * shares the host's JS realm and module graph, so it reports its own sends and receives straight into
 * here, exactly as it reports channels through `dynamicNodes.ts`.
 *
 * So an external client's traffic animates nothing — an `Examples/Layer1` run is invisible here.
 * Covering that needs a Broker-side per-message event, a wire-contract change with a per-message
 * cost, not something this module can infer.
 *
 * Pulses live in a plain array outside the signal graph, for the same reason the simulation's
 * positions do: they move every frame. `pulseTick` is the one signal bumped per frame, the canvas's
 * only reactive dependency on traffic.
 *
 * It runs its own `requestAnimationFrame` loop rather than the simulation's, because the simulation
 * self-stops once the layout is at rest — precisely when someone is watching for traffic. Sharing it
 * would also mean running the physics to move a dot, jittering a layout the operator has just
 * arranged.
 *
 * A pulse names a *body*, never a position. `BusGraph.pulsePath` resolves the polyline from live body
 * positions every frame, so a dot follows a node that is still settling or being dragged instead of
 * flying to where it used to be.
 */

export type PulseDirection = "out" | "in";

export interface Pulse {
    id: number;
    /**
     * The deepest body this message concerns — a topic spike, a channel spike, or the node itself
     * for Layer-0 traffic. The path runs from here up through its parents to the Broker.
     */
    originId: string;
    direction: PulseDirection;
    /**
     * `performance.now()` at which the dot leaves — *not* when it was reported.
     *
     * An inbound pulse is held back to the moment the send it answers reaches the Broker, so this can
     * sit in the future. That is why expiry is a compacting walk rather than a splice of the array's
     * front, and why the canvas skips a not-yet-departed dot instead of parking it.
     */
    start: number;
}

/**
 * How long a dot takes to cover its whole path, however long that path is.
 *
 * A fixed duration rather than a constant speed, so one message reads as one trip whether it starts
 * at a node or three levels down at a topic spike — a constant speed would make the deeper journey
 * take visibly longer for no reason the viewer can see.
 *
 * This is one *leg*. The two legs run back to back, so a full source → Broker → destination trip is
 * twice this.
 */
export const PULSE_DURATION_MS = 280;

/**
 * A burst of traffic is worth showing; an unbounded one is worth surviving. Old pulses are dropped
 * from the front, which is also the only place they can be dropped from — see `step`.
 */
const PULSE_LIMIT = 200;

export const pulses: Pulse[] = [];

/** Bumped once per animated frame — the canvas's only reactive dependency on traffic. */
export const pulseTick = signal(0);

let sequence = 0;
let frame = 0;

/**
 * When the most recently reported send reaches the Broker — the instant the next inbound dot may
 * leave it.
 *
 * Sender and receiver share this JS realm, so both halves of a local message are reported within a
 * frame or two of each other and would otherwise animate *simultaneously*: a dot leaving the source
 * while another already crosses toward the destination, which reads as two messages rather than one
 * hop. Deferring the inbound leg to this instant makes the relay legible — the message arrives at the
 * Broker, then leaves it.
 *
 * One timestamp rather than a per-message correlation, because nothing here has a message identity to
 * correlate on. It is only ever ahead of `now` for the length of one leg, so it can only affect a
 * receive reported while a send is genuinely in flight.
 * A fanout's receives all read the same value and so leave the Broker together, which is what a
 * fanout is.
 */
let handoff = 0;

/** A message this node has just put on the wire: travels outward, origin to Broker. */
export function reportSent(nodeId: string, channelName?: string, topic?: string): void {
    push("out", nodeId, channelName, topic);
}

/** A message that has just arrived here: travels inward, Broker to origin. */
export function reportReceived(nodeId: string, channelName?: string, topic?: string): void {
    push("in", nodeId, channelName, topic);
}

function push(direction: PulseDirection, nodeId: string, channelName?: string, topic?: string): void {
    // The same id scheme `virusTree` builds the bodies under, taken from it rather than rebuilt
    // here — a dot that named a body the tree never created would silently never draw.
    const originId =
        channelName === undefined
            ? nodeId
            : topic === undefined
              ? channelBodyId(nodeId, channelName)
              : topicBodyId(nodeId, channelName, topic);

    const now = performance.now();
    const start = direction === "out" ? now : Math.max(now, handoff);
    if (direction === "out") handoff = start + PULSE_DURATION_MS;

    pulses.push({ id: ++sequence, originId, direction, start });
    if (pulses.length > PULSE_LIMIT) pulses.splice(0, pulses.length - PULSE_LIMIT);

    if (!frame) frame = requestAnimationFrame(step);
}

/**
 * Retiring is a compacting walk of the whole list rather than a splice of its front: a held-back
 * inbound pulse outlives sends reported after it, so the expired ones are no longer a prefix of an
 * array that is still appended to in *report* order. The list is capped at `PULSE_LIMIT`, so the
 * walk is bounded whatever the traffic.
 *
 * A pulse yet to leave (`start` in the future) is kept, which is also what holds the loop alive
 * across the gap between the two legs of a message.
 *
 * The tick is bumped *before* the loop decides to stop, so the frame that empties the list is also
 * the frame that repaints the canvas without it. Bumping only while pulses remain would leave the
 * last dot painted at its destination until something else re-rendered the graph.
 */
function step(): void {
    const now = performance.now();

    let kept = 0;
    for (const pulse of pulses) if (now - pulse.start < PULSE_DURATION_MS) pulses[kept++] = pulse;
    pulses.length = kept;

    pulseTick.value++;
    frame = pulses.length > 0 ? requestAnimationFrame(step) : 0;
}
