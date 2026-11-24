import type { Guid } from "./utils/guid.js";

export declare const EventNames: {
    readonly ClientConnected: "client.connected";
    readonly ClientDisconnected: "client.disconnected";
    readonly ChannelRegistered: "channel.registered";
    readonly ChannelRemoved: "channel.removed";
    readonly TopicRegistered: "topic.registered";
    readonly TopicSubscribed: "topic.subscribed";
    readonly TopicUnsubscribed: "topic.unsubscribed";
};

export type EventName = (typeof EventNames)[keyof typeof EventNames];

export declare const EventOrigin: {
    readonly Live: "live";
    readonly Replay: "replay";
};

export type EventOriginValue = (typeof EventOrigin)[keyof typeof EventOrigin];

/**
 * A client's self-description, as it registers it and as the Broker hands it back enriched.
 *
 * Properties is a free-form bag — role, host, region, version, whatever a deployment finds worth
 * distinguishing — and is the axis client.* subscriptions narrow on. String values only: these exist
 * to be *matched*, and exact equality is the one comparison needing no type negotiation across
 * languages.
 */
export interface ClientInfo {
    /**
     * This client's RouteId, or null to let the Broker assign one. Left unset on a first-ever
     * connect; the Broker's "client.registered" ack always carries a concrete value. A caller that
     * wants RouteId to survive a *process restart* caches that ack's routeId and passes it back in
     * here next time — whether the Broker honors it then depends on identity below.
     */
    routeId: Guid | null;
    /**
     * A stable handle the caller vouches for itself with, so the Broker can hand back the same
     * RouteId across a reconnect from a fresh process. Opaque to the Broker: used only as a lookup
     * key, never parsed. Mandatory — `newClientInfo` supplies a default so this is free.
     */
    identity: string;
    /** The Broker's one-way estimate of this client's clock offset, stamped at registration. */
    clockOffsetMs: number;
    properties: Record<string, string>;
}

export interface ClientEvent {
    name: "client.connected" | "client.disconnected";
    routeId: Guid;
    /** Null only for a disconnect on a connection whose registration never completed. */
    client: ClientInfo | null;
}

export interface ChannelEvent {
    name: "channel.registered" | "channel.removed";
    channel: string;
    providerRouteId: Guid;
}

/**
 * Carries the channel alongside the topic for the same reason Destination carries both halves — a
 * topic name alone doesn't identify a topic.
 */
export interface TopicEvent {
    name: "topic.registered" | "topic.subscribed" | "topic.unsubscribed";
    channel: string;
    topic: string;
    topicId: Guid;
    /** Null on topic.registered — the topic exists, nobody subscribed. */
    subscriberRouteId: Guid | null;
}

/**
 * Name is many-to-one onto these shapes — client.connected and client.disconnected are both a
 * ClientEvent — which is why the wire contract is not polymorphic and why this discriminates on the
 * *names*. A subscriber knows the shape from the name it subscribed to.
 */
export type BrokerEvent = ClientEvent | ChannelEvent | TopicEvent;

/**
 * event.subscribe body. The narrowing halves cover disjoint families — channel/topic for the channel
 * and topic events, `where` for the client ones — and a half that cannot apply to an occurrence is
 * ignored rather than treated as unmatched, so a channel-narrowed subscription to client.connected
 * still delivers.
 *
 * Every `where` entry must hold (AND). A subscriber wanting either can subscribe twice, since
 * filters are additive per name — the union is expressible that way, the intersection would not be.
 */
export interface EventSubscription {
    name: string;
    channel?: string;
    topic?: string;
    where?: Record<string, string>;
}

/**
 * event.replay body — the whole subscribe-time snapshot in one frame, because a subscriber's send
 * queue evicts silently and an N-frame replay could be truncated to its tail unnoticed. Exactly one
 * list is populated, determined by name.
 */
export interface EventReplay {
    name: string;
    clients?: ClientEvent[];
    channels?: ChannelEvent[];
    topics?: TopicEvent[];
}

export declare function replayOccurrences(replay: EventReplay): BrokerEvent[];

export type EventHandler = (occurrence: BrokerEvent, origin: EventOriginValue) => void;
