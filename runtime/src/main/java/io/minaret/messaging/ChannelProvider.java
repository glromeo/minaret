package io.minaret.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import io.minaret.messaging.ControlBodies.BrokerAck;
import io.minaret.messaging.ControlBodies.ChannelRegistration;
import io.minaret.messaging.ControlBodies.TopicRegistration;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Provider-side handle: claims a channel name, registers the topics it handles, and can reply to a
 * single client, broadcast to every subscriber, or notify one client unsolicited.
 */
public final class ChannelProvider extends ChannelBase {

    private static final Logger LOG = System.getLogger(ChannelProvider.class.getName());

    private final String channel;
    private final UUID channelRouteId;

    private ChannelProvider(Router router, String channel, UUID channelRouteId) {
        super(router);
        this.channel = channel;
        this.channelRouteId = channelRouteId;
    }

    static CompletableFuture<ChannelProvider> create(Router router, String channelName) {
        UUID routeId = router.routeId();
        if (routeId == null) return CompletableFuture.failedFuture(new IllegalStateException("Router is not connected."));

        ChannelProvider provider = new ChannelProvider(router, channelName, routeId);

        return provider.request(Verbs.CHANNEL_REGISTER, new ChannelRegistration(channelName), Verbs.CHANNEL_REGISTERED)
                .thenApply(ack -> {
                    if (!ack.ok())
                        throw new IllegalStateException("Failed to register channel '" + channelName + "': " + ack.error());

                    LOG.log(Level.INFO, "Channel ''{0}'' registered as {1}", channelName, provider.channelRouteId());
                    return provider;
                });
    }

    /**
     * The RouteId this provider is registered under — also the channel's identity, since there is
     * exactly one provider per channel name.
     */
    public UUID channelRouteId() {
        return channelRouteId;
    }

    @Override
    public String channelName() {
        return channel;
    }

    @Override
    boolean isProvider() {
        return true;
    }


    /**
     * Registers a handler for inbound messages on a topic, and declares the topic to the Broker so it is
     * discoverable. Calling this again for the same topic does not replace the first handler — both are
     * invoked, in registration order.
     */
    @Override
    public CompletableFuture<Void> subscribe(String topic, BiConsumer<Header, JsonNode> handler) {
        addTopicHandler(topic, handler);

        return request(Verbs.TOPIC_REGISTER, new TopicRegistration(channel, topic), Verbs.TOPIC_REGISTERED)
                .thenAccept(ack -> {
                    if (!ack.ok())
                        throw new IllegalStateException("Failed to register topic '" + topic + "': " + ack.error());

                    // The ack carries the topic's Broker-assigned id: cache it so inbound frames reach
                    // the handler just registered, and so sends address the topic by id.
                    if (ack.topicId() != null) learnTopicId(topic, ack.topicId());
                });
    }

    /**
     * Replies to a single client on a topic, in response to a request being handled.
     *
     * The message is addressed to the topic, like all topic traffic; the Broker narrows delivery to the
     * one awaiting connection by matching the correlation id against the request it recorded — which is
     * why no recipient is needed here. The id comes from the given header, and otherwise from the
     * request currently in flight, so a reply sent from inside a handler needs neither argument.
     */
    public CompletableFuture<Void> dispatch(String topic, Object body, Header request) {
        Header inFlight = request != null ? request : IN_FLIGHT_REQUEST.get();
        UUID replyTo = inFlight == null ? null : inFlight.replyTo();

        return sendOnTopic(Verbs.DISPATCH, topic, body, replyTo);
    }

    public CompletableFuture<Void> dispatch(String topic, Object body) {
        return dispatch(topic, body, null);
    }

    /**
     * Broadcasts to every client currently subscribed to the topic. The Broker performs the fanout; this
     * only confirms the publish reached the Broker, not that every subscriber received it.
     */
    public CompletableFuture<Void> publish(String topic, Object body) {
        return publishOnTopic(topic, body);
    }

    /**
     * Sends to one client, unsolicited — the point-to-point counterpart of publish, and the only
     * provider-originated send that names a recipient.
     *
     * Unlike dispatch this answers nothing, so it correlates to nothing and the recipient rides in
     * replyTo instead. No ack comes back: a notification the recipient never asked for has nothing to
     * confirm, and awaiting one would make every notification cost a round trip.
     */
    public CompletableFuture<Void> notify(UUID recipient, String topic, Object body) {
        return sendOnTopic(Verbs.NOTIFY, topic, body, recipient);
    }

    /**
     * Exposed so a caller can check an ack shape without the handle re-wrapping it.
     */
    public CompletableFuture<BrokerAck> requestAck(String type, Object body, String ackType) {
        return request(type, body, ackType);
    }

}
