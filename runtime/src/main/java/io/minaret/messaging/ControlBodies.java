package io.minaret.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.UUID;

/**
 * Wire bodies for the Layer-1 channel/topic control messages, all Broker-addressed — symmetric with the
 * register/registered handshake Router already uses.
 *
 * Nothing is shared with the Broker but the JSON, so they are gathered into one holder
 * rather than scattered as top-level types nothing else references.
 */
public final class ControlBodies {

    private ControlBodies() {
    }

    /**
     * channel.register body — a provider claiming a channel name.
     */
    public record ChannelRegistration(String channelName) {
    }

    /**
     * channel.resolve body — a client asking for the provider RouteId behind a channel name. The Broker
     * replies once a matching provider is registered, whether that happens before or after this arrives.
     */
    public record ChannelResolution(String channelName) {
    }

    /**
     * topic.register body — a provider declaring a topic it handles on its own channel.
     */
    public record TopicRegistration(String channelName, String topic) {
    }

    public record TopicListRequest(String channelName) {
    }

    public record TopicSubscription(String channelName, String topic) {
    }

    /**
     * topic.unsubscribe body. Deliberately the same shape as {@link TopicSubscription} rather than a
     * bare TopicId: the client works in names throughout, and a subscriber that never observed its
     * subscribe ack has a name but no id.
     */
    public record TopicUnsubscription(String channelName, String topic) {
    }

    /**
     * The shared ack body for every channel/topic control message, so a caller awaiting any of them
     * checks one shape.
     *
     * routeId is populated only for channel.resolved; topicId for the acks that establish a topic's
     * identity; topics for topic.listed, and on topic.resolved the single name that was resolved.
     * channel is set only on topic.resolved: every handle on the connection sees that ack, and only
     * the one on this channel may learn the id (see ChannelBase.onMessage).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BrokerAck(boolean ok, String error, UUID routeId, UUID topicId, List<String> topics, String channel) {
    }

}
