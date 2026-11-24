import { BodyEncoding } from "./utils/wire.js";

/**
 * The structured envelope, mirroring runtime's Message.
 *
 * Destination and Header are frozen plain objects rather than classes, because every consumer of
 * them is either the codec or a `{ ...header, ... }` re-stamp, and relied on for that:
 * a field added later survives every copy site
 * automatically, where a hand-written copy would silently drop it.
 */

/**
 * A message's destination: a resolved id, or an unresolved /channel/topic path the Broker looks up on
 * arrival. Never both — which is why this is a type rather than three Header fields whose mutual
 * exclusion would be only a convention.
 *
 * What id *means* depends on Header.type: a TopicId for dispatch, publish and notify, the
 * recipient's RouteId for anything else. The Broker reads the verb to know which table to look it up
 * in, so the two id namespaces never have to be distinguished on the wire.
 *
 * The path form carries *both* halves of a topic's identity, and that is the point. A topic name
 * alone is meaningless without its channel: two channels may each register a `run`, so a bare name
 * would make the Broker guess, and a guess across the channels a client happens to know is a
 * misroute. The sender always knows its channel, since it is the handle the call was made on.
 *
 * Empty means the message is addressed to the Broker itself, as in the client.register handshake.
 */
export const Destination = Object.freeze({

    /** No destination: the message is addressed to the Broker itself. */
    empty: Object.freeze({ id: null, channel: null, topic: null }),

    fromId(id) {
        return Object.freeze({ id, channel: null, topic: null });
    },

    /**
     * Addresses a topic by name on a named channel, for a sender with no TopicId cached yet.
     *
     * ChannelBase's #destinationFor produces it on a first send. Self-resolving: the Broker resolves
     * it, routes the frame, and acks the id back as topic.resolved, so every later send is a plain
     * id.
     */
    fromPath(channel, topic) {
        return Object.freeze({ id: null, channel, topic });
    },

    isEmpty(destination) {
        return destination.id === null && destination.channel === null;
    },

    /** True for the unresolved path form, in which case channel and topic are both set. */
    isPath(destination) {
        return destination.channel !== null;
    },

    toString(destination) {
        if (destination.id !== null) return destination.id;

        return destination.channel === null ? "(broker)" : `/${destination.channel}/${destination.topic}`;
    }

});

/**
 * Builds a Header with the rarely-set fields defaulted.
 *
 * `time` defaults to 0, not Date.now(). 0 is the unset sentinel and Router.sendMessage is what stamps
 * it. A caller stamping its own clock here would defeat the Broker's clock-offset estimate, computed
 * from the difference between this field and the Broker's own receive time.
 */
export function header(type, to = Destination.empty, overrides = {}) {
    return Object.freeze({
        to,
        type,
        /** Sender's RouteId; null means the message originated from the Broker itself. */
        from: null,
        /** Correlation id — what makes a reply findable by the one connection awaiting it. */
        replyTo: null,
        /** Unix epoch milliseconds. 0 means unstamped. */
        time: 0,
        /** Seconds. null or 0 means the Broker's default lifetime. */
        ttl: null,
        /** How the body accompanying this header is encoded — still compressed when "gzip". */
        encoding: BodyEncoding.Raw,
        /** The body's exact byte count on the wire, or null when the sender didn't stamp it. */
        bodyLength: null,
        /** Trailing header segments a newer wire version added that this one doesn't understand. */
        extras: "",
        ...overrides
    });
}
