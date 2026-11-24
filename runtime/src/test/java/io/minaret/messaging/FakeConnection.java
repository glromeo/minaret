package io.minaret.messaging;

import io.minaret.messaging.wire.Wire;
import io.minaret.messaging.wire.WireFrame;
import io.minaret.util.Emitter;
import io.minaret.util.Json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A hand-rolled {@link Connection} standing in for the Broker — this repo's convention, in place of a
 * mocking framework.
 *
 * It answers the control plane well enough for a Router and a channel handle to get through their
 * handshakes, and deliberately no further. In particular it has no routing table, so every raised frame
 * reaches every subscriber on this instance, where a real Broker would deliver to one. The consequence
 * is that a test needing a frame to arrive somewhere raises it explicitly, and that dispatch/notify are
 * ignored here rather than pretended at — a fake that half-routes is worse than one that plainly does
 * not.
 *
 * Channel registrations are tracked per instance, so channel.resolve only finds a provider that
 * registered against the same fake.
 */
final class FakeConnection implements Connection {

    private final Emitter<WireFrame> onFrame = new Emitter<>();
    private final Emitter<Integer> onDisconnected = new Emitter<>();

    private final List<Sent> sent = new ArrayList<>();
    private final Map<String, UUID> channels = new HashMap<>();
    private final Map<String, UUID> topicIds = new HashMap<>();

    /**
     * Acks are raised from here rather than inline on the send call stack. A real Broker's ack arrives
     * on the receive loop after the send completes; acking re-entrantly would let the registration
     * future complete before register had returned from its own await, hiding the ack-racing-registration
     * ordering Router has to get right.
     */
    private final ScheduledExecutorService acks = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "fake-broker");
        thread.setDaemon(true);
        return thread;
    });

    private volatile ConnectionState state = ConnectionState.CLOSED;

    boolean suppressRegistrationAck;
    long ackClockOffsetMs;
    volatile boolean closed;

    @Override
    public ConnectionState state() {
        return state;
    }

    @Override
    public Emitter<WireFrame> onFrame() {
        return onFrame;
    }

    @Override
    public Emitter<Integer> onDisconnected() {
        return onDisconnected;
    }

    @Override
    public CompletableFuture<Void> connect() {
        state = ConnectionState.OPEN;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> send(Header header, String body) {
        synchronized (sent) {
            sent.add(new Sent(header, body));
        }

        respond(header, body);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
        closed = true;
        state = ConnectionState.CLOSED;
        acks.shutdownNow();
    }

    List<Sent> sent() {
        synchronized (sent) {
            return List.copyOf(sent);
        }
    }

    void clearSent() {
        synchronized (sent) {
            sent.clear();
        }
    }

    /**
     * Delivers a frame inbound, as the Broker would.
     */
    void raise(Header header, Object body) {
        Wire.Prepared prepared = Wire.prepareBody(header, body);
        WireFrame.Parsed parsed = Wire.parseHeader(Wire.writeHeaderAndBody(prepared.header(), prepared.body()));
        if (!parsed.isOk()) throw new IllegalStateException("fake raised an unparseable frame: " + parsed.error());

        onFrame.emit(parsed.frame());
    }

    /**
     * Delivers a non-envelope string, for testing that garbage cannot break the message loop.
     */
    void raiseRaw(String raw) {
        WireFrame.Parsed parsed = Wire.parseHeader(raw);
        if (parsed.isOk()) onFrame.emit(parsed.frame());
    }

    void raiseRegistrationAck(ClientInfo info) {
        raise(Header.of(Verbs.CLIENT_REGISTERED), info);
    }

    UUID topicId(String topic) {
        synchronized (topicIds) {
            return topicIds.computeIfAbsent(topic, ignored -> UUID.randomUUID());
        }
    }

    private void respond(Header header, String body) {
        String verb = header.type();
        UUID replyTo = header.replyTo();

        switch (verb) {
            case Verbs.CLIENT_REGISTER -> {
                if (suppressRegistrationAck) return;

                ClientInfo sentInfo = Json.tryParse(body, ClientInfo.class);
                ClientInfo enriched = sentInfo == null
                        ? ClientInfo.create()
                        : new ClientInfo(
                        sentInfo.routeId() != null ? sentInfo.routeId() : UUID.randomUUID(),
                        sentInfo.identity(), ackClockOffsetMs, sentInfo.properties()
                );

                later(() -> raiseRegistrationAck(enriched));
            }
            case Verbs.CHANNEL_REGISTER -> {
                ControlBodies.ChannelRegistration registration =
                        Json.tryParse(body, ControlBodies.ChannelRegistration.class);
                synchronized (channels) {
                    channels.put(registration.channelName(), header.from());
                }

                later(() -> ack(Verbs.CHANNEL_REGISTERED, replyTo, true, null, null, null, null));
            }
            case Verbs.CHANNEL_RESOLVE -> {
                ControlBodies.ChannelResolution resolution =
                        Json.tryParse(body, ControlBodies.ChannelResolution.class);

                UUID provider;
                synchronized (channels) {
                    provider = channels.get(resolution.channelName());
                }

                later(() -> {
                    if (provider == null)
                        ack(Verbs.CHANNEL_RESOLVED, replyTo, false, "no provider", null, null, null);
                    else
                        ack(Verbs.CHANNEL_RESOLVED, replyTo, true, null, provider, null, null);
                });
            }
            case Verbs.TOPIC_REGISTER -> {
                ControlBodies.TopicRegistration registration =
                        Json.tryParse(body, ControlBodies.TopicRegistration.class);
                UUID id = topicId(registration.topic());

                later(() -> ack(Verbs.TOPIC_REGISTERED, replyTo, true, null, null, id, null));
            }
            case Verbs.TOPIC_SUBSCRIBE -> {
                ControlBodies.TopicSubscription subscription =
                        Json.tryParse(body, ControlBodies.TopicSubscription.class);
                UUID id = topicId(subscription.topic());

                later(() -> ack(Verbs.TOPIC_SUBSCRIBED, replyTo, true, null, null, id, null));
            }
            case Verbs.TOPIC_UNSUBSCRIBE ->
                    later(() -> ack(Verbs.TOPIC_UNSUBSCRIBED, replyTo, true, null, null, null, null));
            case Verbs.TOPIC_LIST -> {
                List<String> names;
                synchronized (topicIds) {
                    names = List.copyOf(topicIds.keySet());
                }

                later(() -> ack(Verbs.TOPIC_LISTED, replyTo, true, null, null, null, names));
            }
            case Verbs.PUBLISH -> later(() -> ack(Verbs.PUBLISHED, replyTo, true, null, null, null, null));
            case Verbs.EVENT_SUBSCRIBE -> later(() -> ack(Verbs.EVENT_SUBSCRIBED, replyTo, true, null, null, null, null));

            // dispatch and notify are deliberately ignored: this fake has no routing table, and
            // pretending to route would deliver a provider's reply back to the provider.
            default -> {
            }
        }
    }

    private void ack(String type, UUID replyTo, boolean ok, String error, UUID routeId, UUID topicId, List<String> topics) {
        raise(
                Header.of(type, Destination.EMPTY, replyTo),
                new ControlBodies.BrokerAck(ok, error, routeId, topicId, topics, null)
        );
    }

    private void later(Runnable action) {
        if (acks.isShutdown()) return;
        acks.schedule(action, 1, TimeUnit.MILLISECONDS);
    }

    record Sent(Header header, String body) {
    }

}
