package io.minaret.broker.contract;

import io.micronaut.serde.annotation.Serdeable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * The JSON bodies the Broker reads or writes, mirroring Runtime/Messaging (Channels.cs, Events.cs,
 * Router.cs's ClientInfo). Each record is @Serdeable, so Micronaut generates its (de)serializer at
 * compile time and nothing here is read by reflection.
 *
 * Bodies are the control plane only. Session reads one per Broker-addressed frame and writes one per
 * ack or event, and never decodes a relayed body. Field names are the C# properties camelCased, as
 * Runtime's Json.Options writes them. Nulls are written, as System.Text.Json writes them, through
 * application.yml's serde inclusion setting.
 *
 * A field C# declares `required` is checked non-null in the canonical constructor. Json.read turns
 * that failure into "no parseable body", the same drop a missing required member gives in C#.
 */
public final class Contract {

    private Contract() {
    }

    /**
     * ClientInfo as registered. HandleRegister fills in routeId and clockOffsetMs, and the enriched
     * copy is echoed in client.registered and carried on every client event.
     */
    @Serdeable
    public record ClientInfo(UUID routeId, String identity, long clockOffsetMs, Map<String, String> properties) {

        public ClientInfo {
            if (properties == null) properties = Map.of();
        }

        public ClientInfo enrich(UUID routeId, long clockOffsetMs) {
            return new ClientInfo(routeId, identity, clockOffsetMs, properties);
        }

    }

    @Serdeable
    public record ChannelName(String channelName) {

        public ChannelName {
            Objects.requireNonNull(channelName);
        }

    }

    /** topic.register, topic.subscribe and topic.unsubscribe: one shape on the wire. */
    @Serdeable
    public record ChannelTopic(String channelName, String topic) {

        public ChannelTopic {
            Objects.requireNonNull(channelName);
            Objects.requireNonNull(topic);
        }

    }

    @Serdeable
    public record BrokerAck(boolean ok, String error, UUID routeId, UUID topicId, List<String> topics, String channel) {

        public static final BrokerAck OK = new BrokerAck(true, null, null, null, null, null);

        public static BrokerAck failed(String error) {
            return new BrokerAck(false, error, null, null, null, null);
        }

    }

    @Serdeable
    public record EventSubscription(String name, String channel, String topic, Map<String, String> where) {

        public EventSubscription {
            Objects.requireNonNull(name);
        }

    }

    /**
     * EventType's three concrete shapes. Each emit site serializes one of these, never the base.
     *
     * EventFilter.matches narrows on channel and topic through this interface, so it needs no
     * cast. The defaults are not record components, so they never reach the JSON.
     */
    public sealed interface Event permits ClientEvent, ChannelEvent, TopicEvent {

        String name();

        default String channel() {
            return null;
        }

        default String topic() {
            return null;
        }

    }

    @Serdeable
    public record ClientEvent(String name, UUID routeId, ClientInfo client) implements Event {
    }

    @Serdeable
    public record ChannelEvent(String name, String channel, UUID providerRouteId) implements Event {
    }

    @Serdeable
    public record TopicEvent(String name, String channel, String topic, UUID topicId, UUID subscriberRouteId) implements Event {
    }

    /** One frame holding the whole level-triggered snapshot; exactly one list is set, per name. */
    @Serdeable
    public record EventReplay(String name, List<ClientEvent> clients, List<ChannelEvent> channels, List<TopicEvent> topics) {
    }

    /** What the discovery pipe serves. */
    @Serdeable
    public record BrokerConnectionInfo(int port) {
    }

}
