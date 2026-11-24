package io.minaret.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The Layer-0 event plane: notifications about things the Broker observes.
 *
 * The event's name lives in the body, not in Header.type — which is always the event verb — for exactly
 * the reason topic identity lives in Header.to: "what is this?" and "which one is it?" are separate
 * questions. It also means an event may be named after a verb (channel.registered is both) without the
 * two ever colliding, since they never share a field.
 *
 * The payload shapes are deliberately not polymorphic on a JSON discriminator. Name is many-to-one onto
 * them — connected and disconnected are both a ClientEvent — so a discriminator would force a distinct
 * type per name whose only distinguishing feature is the name the body already carries. The subscriber
 * knows the shape from the name it subscribed to, and Router picks the type from the name in one place
 * rather than in every handler.
 */
public final class Events {

    private Events() {
    }

    /**
     * The event vocabulary, shared with Runtime and the Broker so the three can't drift on a string
     * literal.
     *
     * The names pair an arrival with a departure. Only the first of each pair is replayed on subscribe:
     * the .disconnected/.removed/.unsubscribed counterparts describe a transition away from a state, so
     * there is nothing currently true for them to report.
     */
    public static final class Names {

        /**
         * A client completed the register handshake. Fires for every connection, including ones that
         * never touch a channel.
         */
        public static final String CLIENT_CONNECTED = "client.connected";

        public static final String CLIENT_DISCONNECTED = "client.disconnected";

        /**
         * A provider claimed a channel name. This and its counterpart are provider presence: a client
         * learns its provider came up or went away without subscribing to any topic.
         */
        public static final String CHANNEL_REGISTERED = "channel.registered";

        public static final String CHANNEL_REMOVED = "channel.removed";

        public static final String TOPIC_REGISTERED = "topic.registered";
        public static final String TOPIC_SUBSCRIBED = "topic.subscribed";

        /**
         * Fires on an explicit unsubscribe only. A subscriber that simply drops is reported by
         * CLIENT_DISCONNECTED, since the disconnect path unlinks its subscriptions without emitting
         * per-topic events.
         */
        public static final String TOPIC_UNSUBSCRIBED = "topic.unsubscribed";

        private Names() {
        }

    }

    /**
     * Whether a handler is seeing something that just happened or something that was already true when
     * it subscribed.
     *
     * A client watching channel.registered to react to a provider appearing usually does not want to
     * react identically to the forty that already existed, and without this it cannot tell them apart.
     * Handlers that genuinely do not care can ignore the argument.
     */
    public enum Origin {

        LIVE,

        /**
         * State that already existed at subscribe time, delivered in the replay frame that precedes the
         * ack. A handler sees all of these before the subscribe call completes.
         */
        REPLAY

    }

    /**
     * Base of the event payloads — the name plus whatever that event carries.
     */
    public sealed interface Event permits ClientEvent, ChannelEvent, TopicEvent {

        String name();

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ClientEvent(String name, UUID routeId, ClientInfo client) implements Event {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ChannelEvent(String name, String channel, UUID providerRouteId) implements Event {
    }

    /**
     * Carries the channel alongside the topic because a topic name alone does not identify a topic — the
     * same reason Destination carries both halves. subscriberRouteId is null on topic.registered, which
     * is about the topic itself rather than anyone's interest in it.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TopicEvent(String name, String channel, String topic, UUID topicId, UUID subscriberRouteId) implements Event {
    }

    /**
     * event.subscribe body. One name per request, so a client subscribing to several sends several, each
     * acked independently.
     *
     * This is the wire body itself rather than a parallel options type: it already carries exactly the
     * knobs the Broker understands, so a mirroring type would be one more place a new filter axis has to
     * be added.
     *
     * channel/topic narrow the channel and topic events; where narrows the client events by the
     * properties a client declared at registration. A half that cannot apply to the event in hand is
     * ignored rather than treated as unmatched — a channel-narrowed subscription to client.connected
     * would otherwise silently deliver nothing.
     */
    public record Subscription(String name, String channel, String topic, Map<String, String> where) {

        public static Subscription of(String name) {
            return new Subscription(name, null, null, null);
        }

        public static Subscription onChannel(String name, String channel) {
            return new Subscription(name, channel, null, null);
        }

        public static Subscription onTopic(String name, String channel, String topic) {
            return new Subscription(name, channel, topic, null);
        }

    }

    /**
     * event.replay body — the whole level-triggered replay for one subscription, in one frame.
     *
     * The occurrences are split into a list per concrete shape rather than one list of the base type,
     * because a serializer writes the static element type: a base-typed list would emit only the name
     * and silently drop everything else. Exactly one list is populated for a given name.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Replay(
            String name,
            List<ClientEvent> clients,
            List<ChannelEvent> channels,
            List<TopicEvent> topics
    ) {

        /**
         * Every occurrence in this replay, whichever shape it took — what a handler is ultimately
         * invoked with, since a subscriber registered for one name and cares about order, not about
         * which list the Broker happened to fill.
         */
        public List<? extends Event> occurrences() {
            if (clients != null) return clients;
            if (channels != null) return channels;
            if (topics != null) return topics;

            return List.of();
        }

    }

}
