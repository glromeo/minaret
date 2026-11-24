import { computed, signal } from "@preact/signals";
import { EventNames } from "@messagebus/client";
import { reportDisconnected } from "./dynamicNodes";
import { dropSubscriber, refreshInfo, rememberProvider, rememberSubscriber } from "./participants";
import type {
    ChannelEvent,
    ClientEvent,
    ClientInfo,
    ConnectionStateValue,
    Guid,
    Router,
    TopicEvent
} from "@messagebus/client";

/**
 * The observed state of the bus, as signals.
 *
 * Signals rather than hooks because this state is written from a *WebSocket callback*, not from a
 * component. A hook-based store would need the write to be routed back through a setter owned by
 * some component's render cycle; a signal is written directly from the event handler and every
 * subscriber re-renders on its own. It also means the force simulation (a plain requestAnimationFrame
 * loop, no React lifecycle at all) can read the node set without a re-render being involved.
 */

export interface ClientChannel {
    name: string;
    /** Topics this provider has declared on the channel (level-triggered — see EventNames.TopicRegistered). */
    topics: readonly string[];
}

export interface ClientNode {
    routeId: Guid;
    info: ClientInfo | null;
    /** Channels this client provides — a connection may provide more than one (see broker/CLAUDE.md). */
    channels: readonly ClientChannel[];
    /** When this node arrived, so the view can distinguish a new client from a replayed one. */
    since: number;
}

export const connectionState = signal<ConnectionStateValue>("connecting");

export const brokerRouteId = signal<Guid | null>(null);

/** The node the Inspector panel shows — a client's RouteId, the literal "broker", or nothing. */
export const selectedRouteId = signal<Guid | "broker" | null>(null);

/**
 * A row the Log Viewer renders — one real occurrence off the event stream, not a fabricated
 * per-message log line (the wire protocol carries no per-message telemetry to render one from).
 */
export interface LogRow {
    id: string;
    time: number;
    text: string;
    severity: "ok" | "warn" | "error";
    /** The node this row concerns, so a click can select it in the graph. */
    routeId: Guid | null;
}

const LOG_LIMIT = 200;

export const log = signal<LogRow[]>([]);

let logSeq = 0;

function pushLog(row: Omit<LogRow, "id">): void {
    log.value = [{ ...row, id: `${Date.now()}-${logSeq++}` }, ...log.value].slice(0, LOG_LIMIT);
}

/**
 * Keyed by RouteId. A Map in a signal — replaced rather than mutated on every change, since a
 * signal notifies on assignment and an in-place mutation would be invisible to it.
 */
export const clients = signal<ReadonlyMap<Guid, ClientNode>>(new Map());

/**
 * The monitor's own connection is a client like any other, so it is included here — `brokerRouteId`
 * exists so a view can single it out (e.g. label it distinctly) rather than to exclude it.
 */
export const clientList = computed(() => [...clients.value.values()]);

function update(routeId: Guid, mutate: (node: ClientNode) => ClientNode): void {
    const next = new Map(clients.value);
    const existing = next.get(routeId) ?? { routeId, info: null, channels: [], since: Date.now() };
    next.set(routeId, mutate(existing));
    clients.value = next;
}

export function label(routeId: Guid): string {
    return clients.value.get(routeId)?.channels[0]?.name ?? routeId.substring(0, 8);
}

/** The RouteId currently providing `channelName`, if any — channel.registered's own replay/live
 * ordering (see below) guarantees this is populated before any topic.registered for that channel
 * can arrive, since a topic can only be declared on a channel that already exists. */
function findProviderRouteId(channelName: string): Guid | null {
    for (const client of clients.value.values()) if (client.channels.some(c => c.name === channelName)) return client.routeId;
    return null;
}

/**
 * Subscribes to everything the graph draws. The three subscriptions are awaited together — each
 * resolves only after its replay has been delivered, so when this returns the store holds the
 * Broker's complete current picture rather than a partial one that fills in over the next frames.
 *
 * Log rows are pushed only for `live` occurrences, never `replay` ones — the doc's own rationale
 * for the origin distinction (state vs. news) applies just as much to a log as to a graph
 * arrival animation: replaying forty pre-existing clients as forty log lines would swamp the feed.
 */
export async function trackBus(router: Router): Promise<void> {
    await Promise.all([
        router.on(EventNames.ClientConnected, (occurrence, origin) => {
            const event = occurrence as ClientEvent;
            update(event.routeId, node => ({ ...node, info: event.client }));

            // The repository tree's remembered rows may have been written by a channel event that
            // beat this one (the six replays below are independent), or by a previous session
            // entirely — either way this is where they learn their identity and clock offset.
            refreshInfo(event.routeId, event.client);

            if (origin === "live") logClientEvent(event);
        }),

        router.on(EventNames.ClientDisconnected, (occurrence, origin) => {
            const event = occurrence as ClientEvent;
            if (origin === "live") logClientEvent(event);
            const next = new Map(clients.value);
            next.delete(event.routeId);
            clients.value = next;
            if (selectedRouteId.value === event.routeId) selectedRouteId.value = null;

            // This event is what actually retires a locally-added node: the close button only marks
            // it closing and drops its connection, so the graph keeps drawing it (disconnected)
            // until the Broker confirms the departure here. A node that dropped without being asked
            // to is left alone — it is reconnecting, not leaving.
            const removed = reportDisconnected(event.routeId);
            if (removed !== null && selectedRouteId.value === removed) selectedRouteId.value = null;
        }),

        // Channel presence rides on the client node rather than becoming a node of its own: a
        // channel is not independently addressable, it is a name its provider holds, so it reads
        // as a label on that provider.
        router.on(EventNames.ChannelRegistered, (occurrence, origin) => {
            const event = occurrence as ChannelEvent;
            update(event.providerRouteId, node =>
                node.channels.some(c => c.name === event.channel)
                    ? node
                    : { ...node, channels: [...node.channels, { name: event.channel, topics: [] }] }
            );

            // Remembered for the repository tree, which lists the last known provider of a channel
            // whether or not it is still on the bus. Info is read from the store rather than the
            // event (a ChannelEvent carries no ClientInfo) and may not be there yet — see refreshInfo.
            rememberProvider(event.channel, event.providerRouteId, clients.value.get(event.providerRouteId)?.info ?? null);

            if (origin === "live")
                pushLog({
                    time: Date.now(),
                    text: `${event.providerRouteId.substring(0, 8)} registered channel "${event.channel}"`,
                    severity: "ok",
                    routeId: event.providerRouteId
                });
        }),

        router.on(EventNames.ChannelRemoved, (occurrence, origin) => {
            const event = occurrence as ChannelEvent;
            // Drop only this one name — a provider holding several channels keeps the rest.
            update(event.providerRouteId, node => ({ ...node, channels: node.channels.filter(c => c.name !== event.channel) }));
            if (origin === "live")
                pushLog({
                    time: Date.now(),
                    text: `${event.providerRouteId.substring(0, 8)} released channel "${event.channel}"`,
                    severity: "warn",
                    routeId: event.providerRouteId
                });
        }),

        router.on(EventNames.TopicRegistered, (occurrence, origin) => {
            const event = occurrence as TopicEvent;

            // Mirrored onto the provider's own channel entry, exactly as dynamicNodes.ts's
            // reportTopic mirrors NodeApp's accordion state — this is what lets an ordinary
            // Broker-observed provider grow the same channel/topic satellites a dynamic node does
            // (see virusTree.ts). The provider isn't on the event itself (TopicEvent only names the
            // channel), so it's resolved by lookup; channel.registered's replay/live ordering
            // guarantees the channel — and so its provider — is already known by the time this fires.
            const providerRouteId = findProviderRouteId(event.channel);
            if (providerRouteId !== null)
                update(providerRouteId, node => ({
                    ...node,
                    channels: node.channels.map(c =>
                        c.name === event.channel && !c.topics.includes(event.topic) ? { ...c, topics: [...c.topics, event.topic] } : c
                    )
                }));

            if (origin === "live")
                pushLog({
                    time: Date.now(),
                    text: `${event.channel} registered topic "${event.topic}"`,
                    severity: "ok",
                    routeId: null
                });
        }),

        // Subscribed to for the repository tree alone — the graph draws topics off their provider's
        // channel entry and has never needed to know who listens. It is not logged either: a topic
        // gaining a subscriber is the ordinary case, and the unsubscribe below is the notable one.
        router.on(EventNames.TopicSubscribed, occurrence => {
            const event = occurrence as TopicEvent;
            if (event.subscriberRouteId === null) return;
            rememberSubscriber(event.channel, event.topic, event.subscriberRouteId, clients.value.get(event.subscriberRouteId)?.info ?? null);
        }),

        router.on(EventNames.TopicUnsubscribed, (occurrence, origin) => {
            const event = occurrence as TopicEvent;
            if (event.subscriberRouteId !== null) dropSubscriber(event.channel, event.topic, event.subscriberRouteId);
            if (origin === "live")
                pushLog({
                    time: Date.now(),
                    text: `${event.subscriberRouteId?.substring(0, 8) ?? "?"} unsubscribed from ${event.channel}/${event.topic}`,
                    severity: "warn",
                    routeId: event.subscriberRouteId
                });
        })
    ]);
}

function logClientEvent(event: ClientEvent): void {
    const name = event.client?.properties["role"] ?? event.routeId.substring(0, 8);
    pushLog({
        time: Date.now(),
        text: event.name === EventNames.ClientConnected ? `${name} connected` : `${label(event.routeId)} disconnected`,
        severity: event.name === EventNames.ClientConnected ? "ok" : "warn",
        routeId: event.routeId
    });
}
