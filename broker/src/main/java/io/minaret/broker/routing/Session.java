package io.minaret.broker.routing;

import io.minaret.broker.contract.Contract.BrokerAck;
import io.minaret.broker.contract.Contract.ChannelEvent;
import io.minaret.broker.contract.Contract.ChannelName;
import io.minaret.broker.contract.Contract.ChannelTopic;
import io.minaret.broker.contract.Contract.ClientEvent;
import io.minaret.broker.contract.Contract.ClientInfo;
import io.minaret.broker.contract.Contract.Event;
import io.minaret.broker.contract.Contract.EventReplay;
import io.minaret.broker.contract.Contract.EventSubscription;
import io.minaret.broker.contract.Contract.TopicEvent;
import io.minaret.broker.contract.EventFilter;
import io.minaret.broker.contract.EventNames;
import io.minaret.broker.contract.Verbs;
import io.minaret.broker.routing.BrokerMetrics.Drop;
import io.minaret.broker.routing.ChannelRegistry.Channel;
import io.minaret.broker.routing.ChannelRegistry.SubscriptionNode;
import io.minaret.broker.routing.ChannelRegistry.Topic;
import io.minaret.broker.wire.Frame;
import io.minaret.broker.wire.Frame.ToKind;
import io.minaret.broker.wire.FrameWriter;
import io.minaret.broker.wire.Json;
import io.minaret.broker.wire.Uuid;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * One accepted /msgbus connection, the port of C#'s ClientSession: the receive path, the per-session
 * state, every verb handler and the teardown. CLAUDE.md is the rationale. This block covers
 * only how the port differs.
 *
 * Every message is handled inline, on the channel's event loop. C# dispatches each frame on its own
 * Task so that a blocking channel.resolve cannot stall the loop. Here resolve is a future that hops
 * back to the loop when it completes, so nothing blocks and inline is safe. Ordering is stronger than
 * in C# too: a connection's frames are handled strictly in arrival order.
 *
 * Because of that, all session state is confined to one thread. resolvedChannels, providedChannels
 * and frame are plain fields. Only clientInfo is volatile, since ConnectionRegistry.registered reads
 * it from other sessions' threads to build a replay.
 *
 * The data plane allocates nothing per frame on the heap:
 * - the header is parsed in place (Frame);
 * - lookups reuse the frame's probe ids;
 * - the relay renders straight into one pooled direct buffer (FrameWriter);
 * - a fanout hands each subscriber a pooled duplicate of that buffer.
 * Allocation is confined to the control plane — handshake, registration, subscription, events —
 * and to the first frame of a /channel/topic path, whose names are decoded once before the client
 * switches to the id from topic.resolved.
 */
public final class Session extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(Session.class);

    private final ConnectionRegistry connections;
    private final ChannelRegistry channels;
    private final EventRegistry events;
    private final IdentityRegistry identities;
    private final Json json;
    private final BrokerMetrics metrics;
    private final ChannelGroup sockets;
    private final Json.Encoded okAck;

    private final Frame frame = new Frame();
    private final Policy policy = new Policy();

    /** channel name → the provider RouteId this connection resolved it to: what it consumes. */
    private final Map<String, Uuid> resolvedChannels = new HashMap<>();

    /** channel name → the Channel this connection provides: what it serves. Not mergeable with the above; see CLAUDE.md. */
    private final Map<String, Channel> providedChannels = new HashMap<>();

    /** channel.resolve waits still parked in ChannelRegistry, cancelled by the teardown. */
    private final List<PendingResolve> pendingResolves = new ArrayList<>();

    private ChannelHandlerContext ctx;
    private SendRing ring;

    private volatile ClientInfo clientInfo;
    private Uuid routeId;

    /** Copies what it needs out of bus rather than holding it, so the hot path skips a hop. */
    Session(Bus bus) {
        this.connections = bus.connections;
        this.channels = bus.channels;
        this.events = bus.events;
        this.identities = bus.identities;
        this.json = bus.json;
        this.metrics = bus.metrics;
        this.sockets = bus.sockets;
        this.okAck = bus.okAck;
    }

    /**
     * The info this connection registered with, enriched with its RouteId and clock offset, or null
     * before the handshake.
     *
     * ConnectionRegistry.registered reads it from other threads to build the client.connected
     * replay, which is why it is the one volatile field.
     */
    ClientInfo clientInfo() {
        return clientInfo;
    }

    Uuid routeId() {
        return routeId;
    }

    /**
     * Queues a finished frame on this connection, taking ownership of the buffer. Every
     * cross-connection write goes through here: forwards, fanouts, events.
     */
    void send(ByteBuf frame) {
        ring.enqueue(frame);
    }

    /**
     * Closes this connection as superseded once its queue drains. handleRegister calls it on the
     * prior session holding an identity's RouteId.
     */
    void requestClose(String reason) {
        ring.close(reason);
    }

    /** Frames queued and not yet handed to the socket. A test seam. */
    int pendingSends() {
        return ring.pending();
    }

    // ---------------------------------------------------------------- transport

    /**
     * Binds the session to its channel, and pins the channel to the pooled allocator.
     *
     * Netty 4.2 defaults to AdaptivePoolingAllocator, whose buffers copy another buffer in through
     * src.nioBuffer(), allocating a DirectByteBuffer per copy. That copy is FrameWriter.relay's body
     * copy, so the default would allocate on every relayed frame. Pooled unsafe buffers copy
     * address to address. Set here rather than in MsgbusUpgrade, so TestBus's EmbeddedChannels
     * relay on the same allocator AllocationTest measures.
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        ctx.channel().config().setAllocator(PooledByteBufAllocator.DEFAULT);
        this.ctx = ctx;
        this.ring = new SendRing(ctx.channel(), metrics);
        sockets.add(ctx.channel());
    }

    /** Only data frames get here: the protocol handler answers pings, drops pongs and handles close. */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (msg instanceof WebSocketFrame data) onMessage(data.content());
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (ctx.channel().isWritable()) ring.schedule();
        ctx.fireChannelWritabilityChanged();
    }

    /**
     * Keepalive, standing in for Kestrel's KeepAliveInterval and KeepAliveTimeout. The first
     * reader-idle period is answered with a ping. A second consecutive one, still with no frame or
     * pong read, closes the connection.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent idle) {
            if (idle.isFirst()) ctx.writeAndFlush(new PingWebSocketFrame(), ctx.voidPromise());
            else ctx.close();
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Connection dropped ({}: {})", cause.getClass().getSimpleName(), cause.getMessage());
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        try {
            teardown();
        } finally {
            ring.schedule();
            ctx.fireChannelInactive();
        }
    }

    private void onMessage(ByteBuf message) {
        long started = System.nanoTime();

        String error = frame.parse(message);
        if (error != null) {
            if (frame.fatal()) {
                log.warn("Connection dropped ({})", error);
                ctx.close();
                return;
            }
            log.warn("Received frame that could not be parsed as a Wire envelope: {}", error);
            metrics.dropped(Drop.MALFORMED);
            return;
        }

        try {
            dispatch();
        } catch (RuntimeException unexpected) {
            log.error("Unhandled exception dispatching {}", frame.type(), unexpected);
            return;
        }

        metrics.dispatchDuration(System.nanoTime() - started);
        metrics.routed();
    }

    // ---------------------------------------------------------------- dispatch

    private void dispatch() {
        if (frame.toKind() == ToKind.EMPTY) {
            if (frame.typeIs(Verbs.CLIENT_REGISTER)) handleRegister();
            else if (frame.typeIs(Verbs.CHANNEL_REGISTER)) handleChannelRegister();
            else if (frame.typeIs(Verbs.CHANNEL_RESOLVE)) handleChannelResolve();
            else if (frame.typeIs(Verbs.TOPIC_REGISTER)) handleTopicRegister();
            else if (frame.typeIs(Verbs.TOPIC_LIST)) handleTopicList();
            else if (frame.typeIs(Verbs.TOPIC_SUBSCRIBE)) handleTopicSubscribe();
            else if (frame.typeIs(Verbs.TOPIC_UNSUBSCRIBE)) handleTopicUnsubscribe();
            else if (frame.typeIs(Verbs.EVENT_SUBSCRIBE)) handleEventSubscribe();
            else {
                log.warn("Received Broker-addressed message of unknown type {}", frame.type());
                metrics.dropped(Drop.UNKNOWN_TYPE);
            }
            return;
        }

        ClientInfo info = clientInfo;
        policy.resolve(frame, info == null ? 0 : info.clockOffsetMs(), System.currentTimeMillis());
        if (policy.expired) {
            log.warn("Message to {} expired (TTL {}s); dropping", frame.to(), policy.ttl);
            metrics.dropped(Drop.EXPIRED);
            return;
        }

        if (frame.typeIs(Verbs.DISPATCH)) handleDispatch();
        else if (frame.typeIs(Verbs.PUBLISH)) handlePublish();
        else if (frame.typeIs(Verbs.NOTIFY)) handleNotify();
        else forwardToRoute();
    }

    /** From as Policy re-stamps it: the sender's registered RouteId, or its own claim when unregistered. */
    private Uuid stampedFrom() {
        return routeId != null ? routeId : frame.from();
    }

    private ByteBuf relay(Uuid to) {
        return FrameWriter.relay(ctx.alloc(), frame, to, policy.time, policy.ttl, stampedFrom());
    }

    /** Point-to-point forward to a RouteId: every verb that is not dispatch, publish or notify. */
    private void forwardToRoute() {
        if (sender() == null) return;

        boolean byId = frame.toKind() == ToKind.ID;
        Session destination = byId ? connections.get(frame.to()) : null;
        if (destination == null) {
            log.warn("No connection registered for RouteId {}; dropping message", byId ? frame.to() : "(path)");
            metrics.dropped(Drop.UNKNOWN_ROUTE);
            return;
        }

        destination.send(relay(frame.to()));
    }

    /**
     * A topic message for one connection. A request travels in to the provider, with its correlation
     * id claimed. A reply travels out from the provider to the one route awaiting that id.
     * Direction comes from who sent the frame, never from a table lookup.
     */
    private void handleDispatch() {
        Uuid senderId = sender();
        if (senderId == null) return;
        Topic topic = resolveDestination(senderId);
        if (topic == null) return;

        Uuid replyTo = frame.replyTo();
        Uuid destinationRoute;
        if (senderId.equals(topic.channel.providerRouteId)) {
            destinationRoute = replyTo != null ? channels.takeReplyRoute(replyTo) : null;
            if (destinationRoute == null) {
                log.warn("Provider reply on '{}' has no awaiting request (ReplyTo {}); dropping", topic.name, replyTo);
                metrics.dropped(Drop.UNKNOWN_ROUTE);
                return;
            }
        } else {
            destinationRoute = topic.channel.providerRouteId;

            if (replyTo != null && !channels.expectReply(replyTo, senderId)) {
                log.warn("Dispatch on '{}' from {} reuses a correlation id another route is awaiting (ReplyTo {}); dropping", topic.name, senderId, replyTo);
                metrics.dropped(Drop.CORRELATION_IN_USE);
                return;
            }
        }

        Session destination = connections.get(destinationRoute);
        if (destination == null) {
            log.warn("No connected recipient for dispatch on '{}' (route {}); dropping", topic.name, destinationRoute);
            metrics.dropped(Drop.UNKNOWN_ROUTE);
            return;
        }

        ackResolvedName(topic);
        destination.send(relay(topic.id));
    }

    /** A provider notification to the one client ReplyTo names. Nothing awaits it and no ack answers it. */
    private void handleNotify() {
        Uuid senderId = sender();
        if (senderId == null) return;
        Topic topic = resolveDestination(senderId);
        if (topic == null) return;

        if (!topic.channel.providerRouteId.equals(senderId)) {
            log.warn("Rejected notify on '{}' from {}: not the channel's provider", topic.name, senderId);
            metrics.dropped(Drop.NOT_PROVIDER);
            return;
        }

        Uuid recipient = frame.replyTo();
        if (recipient == null) {
            log.warn("Notify on '{}' names no recipient; dropping", topic.name);
            metrics.dropped(Drop.NO_RECIPIENT);
            return;
        }

        Session destination = connections.get(recipient);
        if (destination == null) {
            log.warn("No connected recipient {} for notify on '{}'; dropping", recipient, topic.name);
            metrics.dropped(Drop.UNKNOWN_ROUTE);
            return;
        }

        ackResolvedName(topic);
        destination.send(relay(topic.id));
    }

    /**
     * A provider broadcast: one render, then a pooled duplicate of that buffer to each live subscriber,
     * reached through the node's Route with no table lookup.
     * Netty's reference count stands in for C#'s up-front PooledPayload count. Each duplicate is
     * retained as it is handed out, and the original reference is released once the walk ends, so
     * the buffer outlives the walk however many queues have already finished with it.
     */
    private void handlePublish() {
        Uuid senderId = sender();
        if (senderId == null) return;
        Topic topic = resolveDestination(senderId);
        if (topic == null) return;

        if (!topic.channel.providerRouteId.equals(senderId)) {
            log.warn("Rejected publish to '{}' from {}: not the channel's provider", topic.name, senderId);
            sendToSelf(senderId, Verbs.PUBLISHED, BrokerAck.failed("Only a channel's provider may publish to its topics."), frame.replyTo());
            return;
        }

        ackResolvedName(topic);

        ByteBuf rendered = relay(topic.id);
        try {
            for (SubscriptionNode node = topic.head(); node != null; node = node.next()) {
                if (node.unlinked()) continue;
                Session subscriber = node.route.session;
                if (subscriber == null) continue;
                subscriber.send(rendered.retainedDuplicate());
                metrics.routed();
            }
        } finally {
            rendered.release();
        }

        sendToSelf(senderId, Verbs.PUBLISHED, BrokerAck.OK, frame.replyTo());
    }

    /** A topic verb's To, as an id or a /channel/topic path. Logs and counts the drop itself. */
    private Topic resolveDestination(Uuid senderId) {
        if (frame.toKind() == ToKind.ID) {
            Topic topic = channels.topic(frame.to());
            if (topic == null) {
                log.warn("No topic registered for TopicId {}; dropping {}", frame.to(), frame.type());
                metrics.dropped(Drop.UNKNOWN_TOPIC);
            }
            return topic;
        }

        String channelName = frame.channel();
        String topicName = frame.topic();
        Uuid provider = resolveChannel(channelName, senderId);
        Topic topic = provider == null ? null : channels.topicByName(provider, channelName, topicName);
        if (topic != null && channels.topic(topic.id) == topic) return topic;

        log.warn("Could not resolve '/{}/{}' for {}; dropping {}", channelName, topicName, senderId, frame.type());
        metrics.dropped(Drop.UNKNOWN_TOPIC);
        return null;
    }

    /**
     * The provider RouteId a channel name means to this sender. If it provides the channel, that is
     * its own RouteId; otherwise it is the RouteId it resolved. The Broker-wide channel table is
     * never consulted.
     */
    private Uuid resolveChannel(String channelName, Uuid senderId) {
        if (providedChannels.containsKey(channelName) || channels.provides(senderId, channelName)) return senderId;
        return resolvedChannels.get(channelName);
    }

    /** Tells a path-addressed sender the topic's id. Always sent before the frame it resolves is forwarded. */
    private void ackResolvedName(Topic topic) {
        if (frame.toKind() != ToKind.PATH || routeId == null) return;
        sendToSelf(routeId, Verbs.TOPIC_RESOLVED, new BrokerAck(true, null, null, topic.id.toUUID(), List.of(topic.name), topic.channel.name), null);
    }

    // ---------------------------------------------------------------- control plane

    /**
     * The handshake. Between the ack and connections.register nothing may yield, so the
     * registration always lands before the teardown can run. Inline handling on the event loop
     * guarantees it here.
     */
    private void handleRegister() {
        ClientInfo parsed = json.read(frame, ClientInfo.class);
        if (parsed == null) {
            log.warn("Received register frame with no parseable ClientInfo body; dropping");
            metrics.dropped(Drop.MALFORMED);
            return;
        }

        if (parsed.identity() == null || parsed.identity().isEmpty()) {
            log.warn("Received register frame with no Identity; dropping");
            metrics.dropped(Drop.MALFORMED);
            return;
        }

        long clockOffsetMs = System.currentTimeMillis() - frame.time();
        Uuid resolved = identities.resolveOrClaim(parsed.identity(), Uuid.of(parsed.routeId()));
        ClientInfo enriched = parsed.enrich(resolved.toUUID(), clockOffsetMs);

        Session prior = connections.get(resolved);
        if (prior != null && prior != this) prior.requestClose("superseded by reconnect under the same identity");

        sendToSelf(resolved, Verbs.CLIENT_REGISTERED, enriched, null);

        clientInfo = enriched;
        routeId = resolved;
        connections.register(resolved, this);
        log.info("Registered client {} with RouteId {} (clock offset {}ms)", enriched.identity(), resolved, clockOffsetMs);

        emit(EventNames.CLIENT_CONNECTED, new ClientEvent(EventNames.CLIENT_CONNECTED, resolved.toUUID(), enriched));
    }

    private void handleChannelRegister() {
        Uuid senderId = sender();
        if (senderId == null) return;
        ChannelName registration = body(ChannelName.class);
        if (registration == null) return;

        String channelName = registration.channelName();
        Channel channel = channels.registerChannel(channelName, senderId);
        if (channel != null) providedChannels.put(channelName, channel);

        sendToSelf(senderId, Verbs.CHANNEL_REGISTERED,
                channel != null ? BrokerAck.OK : BrokerAck.failed("Channel '" + channelName + "' is already registered."), frame.replyTo());

        if (channel != null) {
            log.info("Channel '{}' registered to RouteId {}", channelName, senderId);
            emit(EventNames.CHANNEL_REGISTERED, new ChannelEvent(EventNames.CHANNEL_REGISTERED, channelName, senderId.toUUID()));
        } else {
            log.warn("Rejected channel.register for '{}' from {}: already claimed", channelName, senderId);
        }
    }

    /**
     * Parks until the name is registered, with no Broker-side timeout. The connection's lifetime is
     * the bound: the teardown cancels whatever is still parked. Completion hops back to this
     * session's event loop, so the pin and the ack run on the thread that owns resolvedChannels.
     */
    private void handleChannelResolve() {
        Uuid senderId = sender();
        if (senderId == null) return;
        ChannelName resolution = body(ChannelName.class);
        if (resolution == null) return;

        String channelName = resolution.channelName();
        Uuid replyTo = frame.replyTo() != null ? frame.replyTo().copy() : null;

        CompletableFuture<Uuid> waiter = channels.resolve(channelName);
        if (waiter.isDone()) {
            onResolved(senderId, channelName, waiter.join(), replyTo);
            return;
        }

        PendingResolve pending = new PendingResolve(channelName, waiter);
        pendingResolves.add(pending);
        waiter.thenAccept(provider -> ctx.executor().execute(() -> {
            if (pendingResolves.remove(pending) && ctx.channel().isActive()) onResolved(senderId, channelName, provider, replyTo);
        }));
    }

    private void onResolved(Uuid senderId, String channelName, Uuid provider, Uuid replyTo) {
        // Pinned before the ack, so a client dispatching the instant it learns the RouteId resolves.
        resolvedChannels.put(channelName, provider);
        sendToSelf(senderId, Verbs.CHANNEL_RESOLVED, new BrokerAck(true, null, provider.toUUID(), null, null, null), replyTo);
    }

    private void handleTopicRegister() {
        Uuid senderId = sender();
        if (senderId == null) return;
        ChannelTopic registration = body(ChannelTopic.class);
        if (registration == null) return;

        String channelName = registration.channelName();
        Uuid topicId = channels.registerTopic(senderId, channelName, registration.topic());
        if (topicId == null) {
            sendToSelf(senderId, Verbs.TOPIC_REGISTERED, BrokerAck.failed("Register channel '" + channelName + "' before registering topics on it."), frame.replyTo());
            log.warn("Rejected topic.register from {}: no channel '{}' registered", senderId, channelName);
            return;
        }

        sendToSelf(senderId, Verbs.TOPIC_REGISTERED, new BrokerAck(true, null, null, topicId.toUUID(), null, null), frame.replyTo());
        log.info("Topic '{}' registered on channel '{}' as {}", registration.topic(), channelName, topicId);

        emit(EventNames.TOPIC_REGISTERED, new TopicEvent(EventNames.TOPIC_REGISTERED, channelName, registration.topic(), topicId.toUUID(), null));
    }

    private void handleTopicList() {
        Uuid senderId = sender();
        if (senderId == null) return;
        ChannelName request = body(ChannelName.class);
        if (request == null) return;

        Uuid provider = resolveChannel(request.channelName(), senderId);
        List<String> topics = provider == null ? List.of() : channels.listTopics(provider, request.channelName());

        sendToSelf(senderId, Verbs.TOPIC_LISTED, new BrokerAck(true, null, null, null, topics, null), frame.replyTo());
    }

    private void handleTopicSubscribe() {
        Uuid senderId = sender();
        if (senderId == null) return;
        ChannelTopic subscription = body(ChannelTopic.class);
        if (subscription == null) return;

        String channelName = subscription.channelName();
        Uuid provider = resolveChannel(channelName, senderId);
        Uuid topicId = provider == null ? null : channels.subscribe(provider, channelName, subscription.topic(), senderId);
        if (topicId == null) {
            sendToSelf(senderId, Verbs.TOPIC_SUBSCRIBED, BrokerAck.failed("No channel '" + channelName + "' registered."), frame.replyTo());
            log.warn("Rejected topic.subscribe from {}: unknown channel '{}'", senderId, channelName);
            return;
        }

        sendToSelf(senderId, Verbs.TOPIC_SUBSCRIBED, new BrokerAck(true, null, null, topicId.toUUID(), null, null), frame.replyTo());
        log.info("RouteId {} subscribed to '{}' ({}) on channel '{}'", senderId, subscription.topic(), topicId, channelName);

        emit(EventNames.TOPIC_SUBSCRIBED, new TopicEvent(EventNames.TOPIC_SUBSCRIBED, channelName, subscription.topic(), topicId.toUUID(), senderId.toUUID()));
    }

    private void handleTopicUnsubscribe() {
        Uuid senderId = sender();
        if (senderId == null) return;
        ChannelTopic unsubscription = body(ChannelTopic.class);
        if (unsubscription == null) return;

        String channelName = unsubscription.channelName();
        Uuid provider = resolveChannel(channelName, senderId);
        Topic topic = provider == null ? null : channels.topicByName(provider, channelName, unsubscription.topic());
        if (topic == null || !channels.unsubscribe(topic.id, senderId)) {
            sendToSelf(senderId, Verbs.TOPIC_UNSUBSCRIBED, BrokerAck.failed("Not subscribed to that topic."), frame.replyTo());
            log.warn("Rejected topic.unsubscribe from {} for '{}': not subscribed", senderId, unsubscription.topic());
            return;
        }

        sendToSelf(senderId, Verbs.TOPIC_UNSUBSCRIBED, new BrokerAck(true, null, null, topic.id.toUUID(), null, null), frame.replyTo());
        log.info("RouteId {} unsubscribed from '{}' ({})", senderId, unsubscription.topic(), topic.id);

        emit(EventNames.TOPIC_UNSUBSCRIBED, new TopicEvent(EventNames.TOPIC_UNSUBSCRIBED, channelName, unsubscription.topic(), topic.id.toUUID(), senderId.toUUID()));
    }

    /**
     * Subscribes to an event name. For a level-triggered name, it first sends one event.replay frame
     * holding what is already true, narrowed by the same filter as the live stream. The snapshot and
     * the subscribe are atomic under ChannelRegistry's lock, and the replay precedes the ack.
     */
    private void handleEventSubscribe() {
        Uuid senderId = sender();
        if (senderId == null) return;
        EventSubscription subscription = body(EventSubscription.class);
        if (subscription == null) return;

        String name = subscription.name();
        EventFilter filter = new EventFilter(subscription.channel(), subscription.topic(), subscription.where());

        List<Channel> snapshot = channels.snapshotChannelsAndSubscribe(() -> events.subscribe(name, senderId, filter));

        EventReplay replay = null;
        switch (name) {
            case EventNames.CLIENT_CONNECTED -> {
                List<ClientEvent> clients = new ArrayList<>();
                for (Session session : connections.registered()) {
                    ClientEvent occurrence = new ClientEvent(name, session.clientInfo().routeId(), session.clientInfo());
                    if (filter.matches(occurrence)) clients.add(occurrence);
                }
                replay = new EventReplay(name, clients, null, null);
            }
            case EventNames.CHANNEL_REGISTERED -> {
                List<ChannelEvent> registered = new ArrayList<>();
                for (Channel channel : snapshot) {
                    ChannelEvent occurrence = new ChannelEvent(name, channel.name, channel.providerRouteId.toUUID());
                    if (filter.matches(occurrence)) registered.add(occurrence);
                }
                replay = new EventReplay(name, null, registered, null);
            }
            case EventNames.TOPIC_REGISTERED -> {
                List<TopicEvent> topics = new ArrayList<>();
                for (Channel channel : snapshot)
                    for (Topic topic : channel.topics.values()) {
                        TopicEvent occurrence = new TopicEvent(name, channel.name, topic.name, topic.id.toUUID(), null);
                        if (filter.matches(occurrence)) topics.add(occurrence);
                    }
                replay = new EventReplay(name, null, null, topics);
            }
            default -> {
                // Edge-triggered, or a name this Broker does not know: acked, nothing to replay.
            }
        }

        if (replay != null) sendToSelf(senderId, Verbs.EVENT_REPLAY, replay, null);
        sendToSelf(senderId, Verbs.EVENT_SUBSCRIBED, BrokerAck.OK, frame.replyTo());

        log.info("RouteId {} subscribed to event '{}' (channel {}, topic {})", senderId, name,
                filter.channel() == null ? "ANY" : filter.channel(), filter.topic() == null ? "ANY" : filter.topic());
    }

    /**
     * Delivers an occurrence to every route subscribed to its name whose filter matches. The frame
     * is rendered once, and each subscriber gets a pooled duplicate. Never throws: an event reports
     * something that already happened, which is what makes this safe to call from the teardown.
     */
    private void emit(String name, Event occurrence) {
        List<Uuid> subscribers = events.subscribersOf(name, occurrence);
        if (subscribers.isEmpty()) return;

        ByteBuf rendered;
        try {
            Json.Encoded body = json.encode(occurrence);
            rendered = FrameWriter.originate(ctx.alloc(), null, Verbs.EVENT, System.currentTimeMillis(), null, body.gzip(), body.bytes());
        } catch (RuntimeException e) {
            log.error("Could not render event '{}'", name, e);
            return;
        }

        try {
            for (Uuid subscriberId : subscribers) {
                Session subscriber = connections.get(subscriberId);
                if (subscriber != null) subscriber.send(rendered.retainedDuplicate());
            }
        } finally {
            rendered.release();
        }
    }

    /** Queues a Broker-originated frame (an ack, topic.resolved, event.replay) on this connection. */
    private void sendToSelf(Uuid to, byte[] type, Object body, Uuid replyTo) {
        Json.Encoded encoded = body == BrokerAck.OK ? okAck : json.encode(body);
        ring.enqueue(FrameWriter.originate(ctx.alloc(), to, type, System.currentTimeMillis(), replyTo, encoded.gzip(), encoded.bytes()));
    }

    /** The registered RouteId of this connection, never Header.From. Null drops the frame. */
    private Uuid sender() {
        if (routeId != null) return routeId;

        log.warn("Received {} from an unregistered connection; dropping", frame.type());
        metrics.dropped(Drop.UNREGISTERED_SENDER);
        return null;
    }

    private <T> T body(Class<T> type) {
        T parsed = json.read(frame, type);
        if (parsed == null) {
            log.warn("Received {} frame with no parseable body; dropping", frame.type());
            metrics.dropped(Drop.MALFORMED);
        }
        return parsed;
    }

    // ---------------------------------------------------------------- teardown

    /**
     * C#'s finally block, in the same order. Every RouteId-keyed table is cleaned only if this
     * session still held the RouteId: a superseded session shares it with its replacement.
     * removeChannel runs regardless, behind its own reference guard. channel.removed is emitted
     * before client.disconnected.
     */
    private void teardown() {
        for (PendingResolve pending : pendingResolves) channels.cancelResolution(pending.channelName, pending.waiter);
        pendingResolves.clear();

        Uuid id = routeId;
        if (id == null) return;

        boolean held = connections.unregister(id, this);
        if (held) {
            channels.removeConnection(id);
            events.removeConnection(id);
        }

        for (Channel channel : providedChannels.values())
            if (channels.removeChannel(channel))
                emit(EventNames.CHANNEL_REMOVED, new ChannelEvent(EventNames.CHANNEL_REMOVED, channel.name, id.toUUID()));

        if (held) emit(EventNames.CLIENT_DISCONNECTED, new ClientEvent(EventNames.CLIENT_DISCONNECTED, id.toUUID(), clientInfo));
    }

    private record PendingResolve(String channelName, CompletableFuture<Uuid> waiter) {
    }

}
