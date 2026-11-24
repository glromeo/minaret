package io.minaret.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import io.minaret.messaging.ControlBodies.ChannelResolution;
import io.minaret.messaging.ControlBodies.TopicListRequest;
import io.minaret.messaging.ControlBodies.TopicSubscription;
import io.minaret.messaging.ControlBodies.TopicUnsubscription;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Client-side handle: resolves a channel name to its provider's RouteId — blocking Broker-side until a
 * provider registers — then dispatches to it and/or subscribes to its broadcasts.
 */
public final class ChannelClient extends ChannelBase {

    private static final Logger LOG = System.getLogger(ChannelClient.class.getName());

    private final String channel;

    /**
     * Set once resolution completes rather than at construction. The handle has to exist before the
     * resolve round trip, because making that request is something a handle does — and building a
     * throwaway to carry it would leave that throwaway subscribed to handleMessage for the life of the
     * Router, since a channel handle never detaches.
     */
    private volatile UUID channelRouteId;

    private ChannelClient(Router router, String channel) {
        super(router);
        this.channel = channel;
    }

    static CompletableFuture<ChannelClient> create(Router router, String channelName) {
        ChannelClient client = new ChannelClient(router, channelName);

        // Blocks Broker-side until a matching provider registers.
        return client.request(Verbs.CHANNEL_RESOLVE, new ChannelResolution(channelName), Verbs.CHANNEL_RESOLVED)
                .thenApply(ack -> {
                    if (!ack.ok() || ack.routeId() == null)
                        throw new IllegalStateException("Failed to resolve channel '" + channelName + "': " + ack.error());

                    client.channelRouteId = ack.routeId();
                    LOG.log(Level.INFO, "Channel ''{0}'' resolved to {1}", channelName, ack.routeId());
                    return client;
                });
    }

    /**
     * The resolved provider's RouteId for this channel.
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
        return false;
    }


    /**
     * Sends to this channel's provider on a topic.
     *
     * The topic is addressed by id once known and by /channel/topic before that — so a client can
     * dispatch to a provider's topic it never subscribed to, with no resolve round trip.
     */
    public CompletableFuture<Void> dispatch(String topic, Object body) {
        // A correlation id is stamped by default so the provider's reply can be routed back to this
        // connection alone.
        return sendOnTopic(Verbs.DISPATCH, topic, body, UUID.randomUUID());
    }

    /**
     * Dispatches and awaits the provider's reply.
     */
    public CompletableFuture<JsonNode> call(String topic, Object body) {
        return callOnTopic(topic, body);
    }

    /**
     * Subscribes to the provider's broadcasts for a topic.
     *
     * The handler is armed before the request goes out: the Broker marks this client subscribed, and
     * eligible for fanout, before it sends the ack, so a publish can land while the ack is still in
     * flight. Registering first means that frame is handled rather than dropped as an unknown TopicId.
     */
    @Override
    public CompletableFuture<Void> subscribe(String topic, BiConsumer<Header, JsonNode> handler) {
        addTopicHandler(topic, handler);

        return request(Verbs.TOPIC_SUBSCRIBE, new TopicSubscription(channel, topic), Verbs.TOPIC_SUBSCRIBED)
                .thenAccept(ack -> {
                    if (!ack.ok())
                        throw new IllegalStateException("Failed to subscribe to '" + topic + "': " + ack.error());

                    if (ack.topicId() != null) learnTopicId(topic, ack.topicId());
                });
    }

    /**
     * Stops receiving a topic's fanout, then removes the local handlers.
     *
     * The order is the exact mirror of subscribe, and for the mirrored reason: fanout can reach the
     * connection before the ack does, so the handler stays armed until the Broker confirms the unlink,
     * or a publish already in flight is dropped as an unrecognised TopicId. A failed ack leaves the
     * handlers in place — nothing was unsubscribed, so nothing should stop being delivered.
     */
    public CompletableFuture<Void> unsubscribe(String topic) {
        return request(Verbs.TOPIC_UNSUBSCRIBE, new TopicUnsubscription(channel, topic), Verbs.TOPIC_UNSUBSCRIBED)
                .thenAccept(ack -> {
                    if (!ack.ok())
                        throw new IllegalStateException("Failed to unsubscribe from '" + topic + "': " + ack.error());

                    removeTopicHandlers(topic);
                });
    }

    /**
     * The topics this channel's provider has registered, empty if none yet.
     */
    public CompletableFuture<List<String>> listTopics() {
        return request(Verbs.TOPIC_LIST, new TopicListRequest(channel), Verbs.TOPIC_LISTED)
                .thenApply(ack -> ack.topics() == null ? List.of() : ack.topics());
    }

}
