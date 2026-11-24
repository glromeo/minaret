import { signal } from "@preact/signals";
import { newGuid, type Guid } from "@messagebus/client";
import {
    moveWindow,
    newWindowState,
    raiseWindow,
    resizeWindow,
    setWindowOpen,
    toggleWindowMaximized,
    type WindowState
} from "./windows";

/**
 * Which side of a channel a node joined it on. A UI axis rather than a protocol one — the client
 * runtime expresses the same distinction as two different handle types (`ChannelProvider` vs
 * `ChannelClient`) — so it lives here, where the accordion rows that carry it do.
 */
export type ChannelRole = "provider" | "client";

/**
 * Dynamic nodes the operator adds via the (+) button — each an independent bus participant
 * (its iframe opens its own `MessageBus` connection, see `NodeApp.tsx`) rather than a passive
 * graph annotation. Held separately from `graph/store.ts`'s `clients` map: that map is a
 * *reflection* of Broker-observed state, entirely rebuilt from events, whereas this list is
 * locally authored and would otherwise be wiped the moment the store's event handlers touched it.
 */
export interface DynamicChannel {
    name: string;
    role: ChannelRole;
    topics: string[];
}

export interface DynamicNode extends WindowState {
    id: Guid;
    title: string;
    /**
     * Every channel this node has joined (as either role) and the topics on each, mirrored out of
     * `NodeApp`'s own accordion state (see `reportChannel`/`reportTopic`) so the graph can draw the
     * "virus" hierarchy — channels spiking off the node, topics spiking off each channel — which
     * otherwise lives only inside the node's iframe and is invisible to the host document.
     */
    channels: DynamicChannel[];
    /** Stacking order among floating windows — bumped to the current top on click/drag/reopen. */
    zIndex: number;
    /**
     * The Broker-assigned RouteId this node registered under, once `connect()`
     * resolves (see `reportRouteId`) — null before that. This is the id `graph/store.ts`'s
     * `ClientNode`s are keyed by, so it is how the graph decides whether this node is *currently on
     * the bus*: present in `clients` means connected, absent means not (yet, or any more).
     *
     * It survives a drop untouched — `router.reconnect()` (armed by `NodeApp`'s `persistent: true`)
     * reconnects with the same `ClientInfo` and therefore the same RouteId — so a transient outage
     * needs no bookkeeping here at all: the node simply leaves `clients` and comes back.
     */
    routeId: Guid | null;
    /**
c     * Set by the graph's delete button (never by a window control — see `DynamicNode.tsx`): the
     * operator has asked for this node to go, but it is not gone until the *Broker* says so. It
     * unmounts the window, which is what drops the iframe's connection (see `App`'s render filter),
     * and leaves the node in this list, drawn disconnected, until `client.disconnected` arrives for
     * its RouteId and `reportDisconnected` finally removes it.
     *
     * Deliberately not "the node is gone, optimistically" — the point of the monitor is to show what
     * the Broker observes, and a node that vanished from the graph before the Broker noticed would
     * be the one thing on this canvas that isn't true.
     */
    closing: boolean;
}

export const dynamicNodes = signal<DynamicNode[]>([]);

export const DEFAULT_WINDOW_WIDTH = 320;
export const DEFAULT_WINDOW_HEIGHT = 380;
export const MIN_WINDOW_WIDTH = 240;
export const MIN_WINDOW_HEIGHT = 220;

let spawnCount = 0;

export function addDynamicNode(): DynamicNode {
    const node: DynamicNode = {
        id: newGuid(),
        title: `node-${++spawnCount}`,
        ...newWindowState(DEFAULT_WINDOW_WIDTH, DEFAULT_WINDOW_HEIGHT),
        channels: [],
        routeId: null,
        closing: false
    };

    dynamicNodes.value = [...dynamicNodes.value, node];
    return node;
}

export function bringToFront(id: Guid): void {
    dynamicNodes.value = raiseWindow(dynamicNodes.value, id);
}

/**
 * Records the RouteId `NodeApp`'s `connect()` resolved to, once known.
 *
 * Deliberately *after* the registration ack rather than at the RouteId's client-side birth
 * (`newClientInfo` mints it, so it could be reported before connecting): a RouteId here is read as
 * "the Broker has a connection under this id", which is what lets `discardDynamicNode` tell a node
 * worth waiting on from one the Broker never saw.
 */
export function reportRouteId(id: Guid, routeId: Guid): void {
    dynamicNodes.value = dynamicNodes.value.map(node => (node.id === id ? { ...node, routeId } : node));
}

/** Mirrors a channel `NodeApp` just added (registered or resolved) into the graph-visible model. */
export function reportChannel(id: Guid, channelName: string, role: ChannelRole): void {
    dynamicNodes.value = dynamicNodes.value.map(node =>
        node.id === id && !node.channels.some(c => c.name === channelName)
            ? { ...node, channels: [...node.channels, { name: channelName, role, topics: [] }] }
            : node
    );
}

/** Mirrors a topic `NodeApp` just added on channelName into the graph-visible model. */
export function reportTopic(id: Guid, channelName: string, topic: string): void {
    dynamicNodes.value = dynamicNodes.value.map(node =>
        node.id === id
            ? {
                  ...node,
                  channels: node.channels.map(c => (c.name === channelName && !c.topics.includes(topic) ? { ...c, topics: [...c.topics, topic] } : c))
              }
            : node
    );
}

/**
 * Ends a node's participation, which the window controls deliberately never do (see
 * `DynamicNode.tsx`) — this is the graph's own delete. Marking `closing` is what drops the node out
 * of `App`'s render list, unmounting the iframe and with it the bus connection inside it; the node
 * stays listed, drawn disconnected, until the Broker confirms the departure through
 * `reportDisconnected`.
 *
 * A node that never got a RouteId is removed outright: there is no connection the Broker ever saw,
 * so no `client.disconnected` will ever arrive to remove it, and waiting for one would strand it.
 * (Aborting the in-flight `MessageBus.connect` is the unmount's own job, see `NodeApp`.)
 */
export function discardDynamicNode(id: Guid): void {
    const node = dynamicNodes.value.find(n => n.id === id);
    if (!node) return;

    if (node.routeId === null) removeDynamicNode(id);
    else dynamicNodes.value = dynamicNodes.value.map(n => (n.id === id ? { ...n, closing: true, windowOpen: false } : n));
}

/**
 * Reconciles a Broker-observed `client.disconnected` against this list, answering the id of the node
 * it removed (or null) so the caller can drop a selection pointing at it.
 *
 * Only a *closing* node is removed. A node that dropped without being discarded is a node whose
 * connection failed — its Router is still retrying inside its iframe, with the same RouteId — so it
 * stays listed and simply reads as disconnected until it comes back.
 */
export function reportDisconnected(routeId: Guid): Guid | null {
    const node = dynamicNodes.value.find(n => n.routeId === routeId && n.closing);
    if (!node) return null;

    removeDynamicNode(node.id);
    return node.id;
}

/**
 * Drops a node immediately, without waiting on the Broker — the escape hatch behind a second click
 * on an already-closing node's delete button, for the case where the confirming event never comes
 * (the monitor's own connection dropped, say) and the node would otherwise linger forever.
 */
export function removeDynamicNode(id: Guid): void {
    dynamicNodes.value = dynamicNodes.value.filter(node => node.id !== id);
}

export function closeDynamicNodeWindow(id: Guid): void {
    dynamicNodes.value = setWindowOpen(dynamicNodes.value, id, false);
}

export function openDynamicNodeWindow(id: Guid): void {
    dynamicNodes.value = setWindowOpen(dynamicNodes.value, id, true);
}

export function moveDynamicNode(id: Guid, x: number, y: number): void {
    dynamicNodes.value = moveWindow(dynamicNodes.value, id, x, y);
}

export function resizeDynamicNode(id: Guid, width: number, height: number): void {
    dynamicNodes.value = resizeWindow(dynamicNodes.value, id, width, height, MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT);
}

export function toggleMaximizeDynamicNode(id: Guid): void {
    dynamicNodes.value = toggleWindowMaximized(dynamicNodes.value, id);
}
