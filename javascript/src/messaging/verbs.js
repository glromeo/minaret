/**
 * The Header.type vocabulary, mirroring Runtime/Messaging/Verbs.cs so the three implementations
 * (Runtime, Broker, this) can't drift on a string literal.
 *
 * Type is the *verb* — what the recipient should do with the message — and nothing else. Topic
 * identity lives in Header.to, so "what is this?" and "which topic is this?" are separate fields:
 * the Broker reads the verb to decide which table to look `to` up in.
 *
 * Control-plane verbs are noun.verb and are addressed to the Broker (Destination.empty); data-plane
 * verbs are bare and carry a topic in `to`. Requests pair with the ack that answers them, and an ack
 * echoes its request's replyTo so a caller with several in flight can tell them apart — except where
 * noted.
 */
export const Verbs = Object.freeze({

    // ── Control plane: `to` is empty (addressed to the Broker) ──────────────────────────────────

    /** A connecting client introducing itself, carrying its ClientInfo as the body. */
    ClientRegister: "client.register",

    /**
     * Answers ClientRegister with the client's ClientInfo enriched by the Broker's clock-offset
     * estimate. Registration isn't complete until this arrives. It is the one request in the
     * protocol not correlated by replyTo — the handshake predates the client having a RouteId the
     * Broker knows — so it is matched by being Broker-originated (from === null) instead.
     */
    ClientRegistered: "client.registered",

    /** A provider claiming a channel name, which it owns for the life of its connection. */
    ChannelRegister: "channel.register",

    /** Answers ChannelRegister — a failure means another live provider holds the name. */
    ChannelRegistered: "channel.registered",

    /**
     * A client asking which RouteId provides a channel name. The Broker holds the request until a
     * provider registers, so it resolves whether the provider arrives before or after the asking.
     */
    ChannelResolve: "channel.resolve",

    /** Answers ChannelResolve with the provider's RouteId, which the asking session pins. */
    ChannelResolved: "channel.resolved",

    /** A provider declaring a topic on its own channel, making it discoverable via TopicList. */
    TopicRegister: "topic.register",

    /** Answers TopicRegister with the topic's Broker-assigned id. */
    TopicRegistered: "topic.registered",

    /** Asks which topics a channel has registered. */
    TopicList: "topic.list",

    /** Answers TopicList with the topic *names* — which is why a first send addresses by path. */
    TopicListed: "topic.listed",

    /** A client asking to receive a topic's Publish fanout from now on. */
    TopicSubscribe: "topic.subscribe",

    /**
     * Answers TopicSubscribe with the topic's id. Fanout can arrive *before* this, so a subscriber
     * arms its handler before sending the request.
     */
    TopicSubscribed: "topic.subscribed",

    /** A client asking to stop receiving a topic's fanout. */
    TopicUnsubscribe: "topic.unsubscribe",

    /** Answers TopicUnsubscribe. A failure means the sender wasn't subscribed. */
    TopicUnsubscribed: "topic.unsubscribed",

    /**
     * Unsolicited ack telling a sender the id of a topic it addressed by path, so it can cache the
     * id and stop sending /channel/topic. Not a reply to any request, so it carries no replyTo.
     */
    TopicResolved: "topic.resolved",

    /** A client subscribing to one of the Broker's observable events, optionally narrowed. */
    EventSubscribe: "event.subscribe",

    /**
     * Answers EventSubscribe, *after* any EventReplay — so awaiting it means "I have the full
     * picture", not merely "I am subscribed".
     */
    EventSubscribed: "event.subscribed",

    /**
     * A Broker-observed occurrence delivered to a subscriber. Broker-originated, so `from` is null
     * and `to` is the subscriber's own RouteId.
     *
     * The verb is "event" for *every* event; which event it is lives in the body's `name`. Same
     * Type-is-the-verb split that keeps topic identity in `to`, and it is what lets an event be
     * *named* after a verb — channel.registered is both — without the two ever colliding.
     */
    Event: "event",

    /**
     * The level-triggered replay preceding an EventSubscribed ack: everything already true at
     * subscribe time, as one frame carrying an EventReplay body. One frame rather than N because a
     * subscriber's send queue evicts silently, and a distinct verb because a replayed occurrence is
     * state rather than news.
     */
    EventReplay: "event.replay",

    // ── Data plane: `to` carries a TopicId (or /channel/topic) ──────────────────────────────────

    /**
     * Point-to-point on a topic, in both directions: to the topic's provider from anyone else, and
     * back to the one awaiting client from the provider. Which it is depends on who sent it, and the
     * reply reaches exactly one connection by matching replyTo.
     */
    Dispatch: "dispatch",

    /** Provider broadcast on a topic, fanned out to every subscriber. Providers only. */
    Publish: "publish",

    /** Answers Publish, confirming it reached the Broker — not that any subscriber received it. */
    Published: "published",

    /**
     * Provider notification aimed at one client rather than at every subscriber: unsolicited, so
     * nothing awaits it and no ack answers it. Header usage is Dispatch's — `to` is the TopicId —
     * with replyTo carrying the *recipient RouteId*, which is free precisely because a notification
     * correlates to nothing.
     */
    Notify: "notify"

});
