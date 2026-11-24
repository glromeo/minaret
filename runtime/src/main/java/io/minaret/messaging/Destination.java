package io.minaret.messaging;

import java.util.UUID;

/**
 * A message's destination: either a resolved id or an unresolved /channel/topic path the Broker looks up
 * on arrival — never both, which is why this is a type rather than two fields on {@link Header} whose
 * mutual exclusion is only a convention.
 *
 * What id means depends on {@link Header#type()}, which names the verb: for dispatch/publish it is a
 * TopicId; for a direct client-to-client message it is the recipient's RouteId. The Broker reads the verb
 * to know which table to look the id up in, so the two id namespaces never need to be distinguished on
 * the wire.
 *
 * The path form carries both halves of a topic's identity, and that is the point: a topic name alone is
 * meaningless without the channel it belongs to. Sending only the name forces the Broker to guess which
 * channel was meant, and a guess across the channels a client happens to know is a misroute waiting to
 * happen — two channels may each register a topic called "run". The sender always knows its channel (it
 * is the handle the call was made on), so it sends it.
 *
 * Empty (neither set) is the control-plane convention — the message is addressed to the Broker itself,
 * e.g. the client.register handshake.
 */
public record Destination(UUID id, String channel, String topic) {

    /**
     * No destination: the message is addressed to the Broker itself.
     */
    public static final Destination EMPTY = new Destination(null, null, null);

    public static Destination ofId(UUID id) {
        return id == null ? EMPTY : new Destination(id, null, null);
    }

    /**
     * Addresses a topic by name on a named channel, for a sender that has no TopicId cached yet. The
     * Broker resolves it, routes the frame, and acks the id (topic.resolved) so every later send on this
     * topic is a plain id.
     */
    public static Destination ofPath(String channel, String topic) {
        return new Destination(null, channel, topic);
    }

    public boolean isEmpty() {
        return id == null && channel == null;
    }

    /**
     * True when this is the unresolved path form, in which case channel and topic are both non-null.
     */
    public boolean isPath() {
        return channel != null;
    }

    @Override
    public String toString() {
        if (id != null) return id.toString();
        return channel == null ? "(broker)" : "/" + channel + "/" + topic;
    }

}
