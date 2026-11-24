/**
 * The event vocabulary carried in an event body's `name`, mirroring Runtime/Messaging/Events.cs.
 *
 * Several of these read identically to a control-plane verb — channel.registered is both an ack verb
 * and an event name. That is deliberate and cannot collide: an event frame's Header.type is always
 * Verbs.Event, and the name below lives in the *body*. The two vocabularies never share a field, so
 * they are free to share spellings.
 *
 * The names pair up: each one describing something coming into existence has a counterpart for it
 * going away. Only the first of each pair is replayed on subscribe — see EventOrigin.
 */
export const EventNames = Object.freeze({

    /** A client completed the register handshake, whether or not it ever touches a channel. */
    ClientConnected: "client.connected",

    /**
     * A client's session ended, cleanly or otherwise — carrying the info it registered with, so a
     * property-narrowed subscription matches a departure as readily as an arrival.
     */
    ClientDisconnected: "client.disconnected",

    /**
     * A provider claimed a channel name. This and its counterpart are provider presence: a client
     * learns its provider came up or went away without subscribing to any topic.
     */
    ChannelRegistered: "channel.registered",

    /** A channel's provider disconnected, releasing the name for reclaim. */
    ChannelRemoved: "channel.removed",

    /** A provider declared a topic on its channel — nobody subscribed, the topic merely exists. */
    TopicRegistered: "topic.registered",

    /** A client subscribed to a topic's fanout. A provider watching this sees its audience arrive. */
    TopicSubscribed: "topic.subscribed",

    /**
     * A client left a topic's fanout. Fires on an *explicit* unsubscribe only — a subscriber that
     * simply drops is reported by ClientDisconnected instead, since the disconnect path unlinks its
     * subscriptions without emitting per-topic events.
     */
    TopicUnsubscribed: "topic.unsubscribed"

});

/**
 * Whether a handler is seeing something that just happened or something that was already true when
 * it subscribed — the distinction Verbs.EventReplay exists to make visible.
 *
 * A client watching channel.registered to react to a provider *appearing* usually does not want to
 * react identically to the forty that already existed, and without this it cannot tell them apart.
 * Handlers that genuinely don't care can ignore the argument.
 */
export const EventOrigin = Object.freeze({

    /** The occurrence happened after this subscription was established. */
    Live: "live",

    /**
     * State that already existed at subscribe time, delivered in the event.replay frame preceding
     * the ack. A handler sees all of these before Router.on's promise settles.
     */
    Replay: "replay"

});

/**
 * Every occurrence in a replay, whichever shape it took — what a handler is ultimately invoked with,
 * since a subscriber registered for one name and cares about order, not about which list the Broker
 * happened to fill.
 *
 * The lists are split by concrete shape rather than being one list, because System.Text.Json
 * serializes the *static* element type on the emitting side: a base-typed list would have emitted
 * only `name`. Exactly one is populated for any given name.
 */
export function replayOccurrences(replay) {
    return replay.clients ?? replay.channels ?? replay.topics ?? [];
}
