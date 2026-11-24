package io.minaret.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import io.minaret.messaging.wire.Wire;
import io.minaret.messaging.wire.WireFrame;
import io.minaret.util.Emitter;
import io.minaret.util.Json;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A client's connection to the Broker — a message bus, not a WebSocket wrapper. No raw byte or string
 * API, only the structured surface below, with the transport a private detail behind
 * {@link Connection}.
 *
 * Frames dispatch on a single-thread executor rather than whichever thread the transport delivers on.
 * Wire order is load-bearing throughout this protocol — a topic.resolved must be learned before the
 * frame it resolves is dispatched, a replay must reach handlers before the ack saying the picture is
 * complete — and the JDK's WebSocket promises nothing about which thread invokes a listener. This is
 * the Java equivalent of the JavaScript port's FIFO promise chain, for the same reason.
 *
 * Handlers are invoked from that thread but their futures are never joined on it, or a handler issuing
 * a request of its own would deadlock the queue against its own reply.
 */
public class Router implements AutoCloseable {

    private static final Logger LOG = System.getLogger(Router.class.getName());

    private static final int INITIAL_RETRY_DELAY_MS = 500;
    private static final int MAX_RETRY_DELAY_MS = 30_000;

    private final Supplier<Connection> connectionFactory;

    /**
     * Every inbound frame is handled here, in arrival order. Single-threaded on purpose — see the class
     * comment.
     */
    private final ExecutorService inbound = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "messagebus-router");
        thread.setDaemon(true);
        return thread;
    });

    /**
     * Event name to handlers, invoked by {@code dispatchEvent}.
     *
     * Keyed by name rather than surfaced through {@link #handleMessage}, because an event frame's type
     * is the event verb for every event — the name that tells them apart is in the body, so dispatch
     * parses before it can route.
     */
    private final Map<String, List<BiConsumer<Events.Event, Events.Origin>>> eventHandlers = new HashMap<>();

    /**
     * Live event subscriptions, re-issued by {@code connectTo} on every connect.
     *
     * A list rather than a map keyed by name, because two {@code on} calls narrowing one name
     * differently are additive and a map would drop one of the narrowings on reconnect.
     *
     * The C# Router re-registers but does not re-subscribe, leaving a reconnected client armed for
     * events it no longer receives — handlers intact, stream gone. The JavaScript port fixed that;
     * this matches it.
     */
    private final List<Events.Subscription> subscriptions = new ArrayList<>();

    private final AtomicReference<CompletableFuture<ClientInfo>> registration = new AtomicReference<>();

    private final Emitter<Message<JsonNode>> handleMessage = new Emitter<>();

    private final Emitter<Boolean> onStateChange = new Emitter<>();

    /**
     * The frame subscription, held as a field rather than written as a method reference at each use
     * site.
     *
     * {@code connectTo} both adds and removes it, and every evaluation of {@code this::onFrame} yields
     * a distinct object — so two method references would leave the old connection's subscription in
     * place, exactly the leak detaching before close exists to prevent.
     */
    private final Consumer<WireFrame> frameHandler = this::onFrame;

    private volatile ClientInfo clientInfo;
    private volatile Connection connection;
    private volatile boolean closed;

    /**
     * The retry policy {@link #connect} runs under, or null for a single attempt whose failure goes
     * straight to the caller.
     *
     * Set by {@link #reconnect}, read by connect's loop and by connectTo, which arms the disconnect
     * handler only when a policy exists. Router state rather than one socket's, so it governs the
     * first connect and every later drop alike.
     */
    private volatile RetryPolicy reconnectPolicy;

    /**
     * What {@link #reconnect} stores and {@link #connect} reads.
     *
     * One value rather than three loose fields, so "no policy armed" is a single null check instead
     * of a sentinel in each of them.
     */
    private record RetryPolicy(int minDelayMs, int maxDelayMs, int maxRetries) {
    }

    public Router(Supplier<Connection> connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * Raised for every inbound message that parses as an envelope.
     *
     * {@code awaitAck} subscribes per request, which is how every correlated exchange completes;
     * ChannelBase subscribes once per handle to route topic traffic by TopicId. Dispatch reaches every
     * subscriber, which is why a correlator must match on replyTo.
     *
     * The registration ack and event frames are handled internally and never surface here.
     */
    public Emitter<Message<JsonNode>> handleMessage() {
        return handleMessage;
    }

    /**
     * Raised with true once registration completes and false when the connection drops.
     *
     * Any client that displays its connection needs it, which is why it exists here and not on the C#
     * side — where a caller watches Connection.state and the disconnect event separately.
     *
     * Router is the right home because it is the only thing that knows when open is true in the sense
     * that matters: after registration, not merely when the socket opened.
     */
    public Emitter<Boolean> onStateChange() {
        return onStateChange;
    }

    /**
     * This client's RouteId — the Broker's enriched value once connected, the value passed to
     * {@link #connect} before that, or null if never connected.
     */
    public UUID routeId() {
        ClientInfo clientInfo = this.clientInfo;
        return clientInfo == null ? null : clientInfo.routeId();
    }

    public ClientInfo clientInfo() {
        return clientInfo;
    }

    /**
     * Arms retrying: every later {@link #connect} retries, and the connection it adopts heals itself.
     *
     * A setter, not an action — the Java shape of Router.cs's {@code Reconnect}. That is what keeps
     * connect the only way to connect, with a reconnect simply connect running again, rather than a
     * {@code connect}/{@code connectPersistent} pair.
     *
     * A failed attempt — socket refused, registration never acked — is retried after minDelayMs,
     * doubling to maxDelayMs. A drop reported through {@link Connection#onDisconnected} reconnects
     * with the same ClientInfo, so the Broker sees a resumed client rather than a new one.
     *
     * maxRetries bounds one {@link #connect} call. Both it and the delay restart from a successful
     * registration, so it bounds a single outage rather than the Router's lifetime. A background
     * reconnect that exhausts it has no caller to fail to, so the Router stays down until something
     * calls connect again.
     */
    public void reconnect(int minDelayMs, int maxDelayMs, int maxRetries) {
        this.reconnectPolicy = new RetryPolicy(minDelayMs, maxDelayMs, maxRetries);
    }

    /**
     * {@link #reconnect(int, int, int)} with C#'s defaults: 500ms floor, 30s ceiling, unbounded
     * retries.
     */
    public void reconnect() {
        reconnect(INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS, Integer.MAX_VALUE);
    }

    /**
     * Connects and completes the registration round-trip, answering the enriched ClientInfo the Broker
     * sends back.
     *
     * The only public way to connect, and connectTo arms the disconnect handler to call it again, so a
     * reconnect re-enters here. With no policy armed it is one attempt whose failure is the caller's;
     * with one, it is the whole retry loop, completing on the first registration that lands and
     * failing only once the policy is exhausted.
     *
     * No implicit timeout — registration waits for the ack indefinitely, deliberately leaving the
     * bound to the caller, spelled {@code orTimeout} on the returned future.
     */
    public CompletableFuture<ClientInfo> connect(ClientInfo clientInfo) {
        LOG.log(Level.INFO, "Connecting router with RouteId {0}", clientInfo.routeId());

        RetryPolicy policy = reconnectPolicy;
        int delayMs = policy == null ? 0 : policy.minDelayMs();
        return attemptConnect(clientInfo, policy, delayMs, 0);
    }

    private CompletableFuture<ClientInfo> attemptConnect(ClientInfo clientInfo, RetryPolicy policy, int delayMs, int retries) {
        if (closed) return CompletableFuture.failedFuture(new IllegalStateException("Router is closed."));

        CompletableFuture<ClientInfo> attempt;
        try {
            attempt = connectTo(clientInfo, connectionFactory.get());
        } catch (RuntimeException e) {
            attempt = CompletableFuture.failedFuture(e);
        }

        return attempt
                .thenApply(registered -> {
                    LOG.log(Level.INFO, "Connected to Broker with RouteId {0}", registered.routeId());
                    return registered;
                })
                .handle((registered, failure) -> failure == null
                        ? CompletableFuture.completedFuture(registered)
                        : retryOrFail(clientInfo, policy, delayMs, retries, failure))
                .thenCompose(Function.identity());
    }

    private CompletableFuture<ClientInfo> retryOrFail(ClientInfo clientInfo, RetryPolicy policy, int delayMs, int retries, Throwable failure) {
        if (policy == null || retries >= policy.maxRetries() || closed) return CompletableFuture.failedFuture(failure);

        LOG.log(Level.WARNING, "Connect attempt failed; retrying in {0}ms", delayMs, failure);
        int nextDelayMs = Math.min(delayMs * 2, policy.maxDelayMs());

        return CompletableFuture.runAsync(() -> { }, CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS))
                .thenCompose(ignored -> attemptConnect(clientInfo, policy, nextDelayMs, retries + 1));
    }

    /**
     * Adopts a connection and registers over it.
     *
     * connect reaches it once the factory has produced a connection, and it is also the seam tests use
     * to drive a Router against a fake, bypassing the factory and discovery entirely.
     *
     * The previous connection is detached before being closed, so a late frame on the stale socket
     * cannot complete — or be mistaken for — the new connection's registration. An ack from the
     * previous session completing the new one would report the wrong clock offset.
     *
     * Arms the reconnect handler only once registration has landed, and only under a policy, so a
     * connect that never got that far is retried by its own loop rather than by a handler on a
     * connection that never worked. Fire-and-forget, since nobody awaits a drop, so its failure is
     * only logged.
     */
    CompletableFuture<ClientInfo> connectTo(ClientInfo clientInfo, Connection connection) {
        Connection previous = this.connection;
        if (previous != null) {
            previous.onFrame().remove(frameHandler);
            previous.close();
        }

        this.clientInfo = clientInfo;
        this.connection = connection;
        connection.onFrame().add(frameHandler);
        connection.onDisconnected().add(ignored -> onStateChange.emit(false));

        return connection.connect()
                .thenCompose(ignored -> register(clientInfo))
                .thenCompose(registered -> resubscribe().thenApply(ignored -> registered))
                .thenApply(registered -> {
                    onStateChange.emit(true);

                    if (reconnectPolicy != null)
                        connection.onDisconnected().add(closeCode -> {
                            // The one ending that is not retried. Another connection registered under
                            // this client's identity and took its RouteId, so reconnecting would take
                            // it straight back and evict that one — the two trading it at socket
                            // speed. The policy is cleared rather than this attempt skipped, so a
                            // later connection cannot resume retrying for an identity this Router no
                            // longer holds. An explicit connect still works; see CloseCodes.
                            if (closeCode != null && closeCode == CloseCodes.SUPERSEDED) {
                                reconnectPolicy = null;
                                LOG.log(Level.WARNING,
                                        "Connection superseded by a newer registration under identity {0}; not reconnecting",
                                        clientInfo.identity());
                                return;
                            }

                            connect(clientInfo).exceptionally(failure -> {
                                LOG.log(Level.ERROR, "Reconnect abandoned", failure);
                                return null;
                            });
                        });

                    return registered;
                });
    }

    private CompletableFuture<ClientInfo> register(ClientInfo clientInfo) {
        CompletableFuture<ClientInfo> pending = new CompletableFuture<>();
        registration.set(pending);

        Header header = Header.of(Verbs.CLIENT_REGISTER).withFrom(clientInfo.routeId());

        return sendMessage(header, clientInfo)
                .thenCompose(ignored -> pending)
                .whenComplete((registered, failure) -> {
                    // Cleared however this ends, so a later registered frame is treated as ordinary
                    // traffic instead of being swallowed by the ack branch in onFrame. Compare-and-set,
                    // so a concurrent reconnect's newer slot is not the one nulled.
                    registration.compareAndSet(pending, null);
                    if (failure == null) this.clientInfo = registered;
                });
    }

    /**
     * Re-issues every event subscription against the new connection.
     *
     * Called from connectTo rather than the disconnect handler, so a connect — first or reconnect —
     * counts as finished only once the picture is whole again. Handlers survive in memory; the
     * Broker-side subscriptions do not.
     */
    private CompletableFuture<Void> resubscribe() {
        List<Events.Subscription> pending;
        synchronized (eventHandlers) {
            if (subscriptions.isEmpty()) return CompletableFuture.completedFuture(null);
            pending = List.copyOf(subscriptions);
        }

        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (Events.Subscription subscription : pending)
            chain = chain.thenCompose(ignored -> sendSubscribe(subscription));

        return chain;
    }

    /**
     * Registers a handler for one of the Broker's well-known events, completing when the Broker acks.
     *
     * Layer 0: it hangs off Router rather than a channel handle, so a client can watch the Broker
     * without having created or resolved any channel.
     *
     * Awaiting it means "I have the full picture", not merely "I am subscribed": a presence-shaped
     * event replays what is already true before the ack resolves. The handler is armed before the
     * request goes out for that reason.
     */
    public CompletableFuture<Void> on(String name, BiConsumer<Events.Event, Events.Origin> handler) {
        return on(Events.Subscription.of(name), handler);
    }

    public CompletableFuture<Void> on(Events.Subscription subscription, BiConsumer<Events.Event, Events.Origin> handler) {
        synchronized (eventHandlers) {
            eventHandlers.computeIfAbsent(subscription.name(), ignored -> new ArrayList<>()).add(handler);
            subscriptions.add(subscription);
        }

        return sendSubscribe(subscription);
    }

    private CompletableFuture<Void> sendSubscribe(Events.Subscription subscription) {
        UUID correlationId = UUID.randomUUID();
        CompletableFuture<ControlBodies.BrokerAck> ack = new CompletableFuture<>();

        Consumer<Message<JsonNode>> onAck = message -> {
            if (!Verbs.EVENT_SUBSCRIBED.equals(message.header().type())) return;
            if (!correlationId.equals(message.header().replyTo())) return;

            ControlBodies.BrokerAck parsed = Json.convert(message.body(), ControlBodies.BrokerAck.class);
            if (parsed != null) ack.complete(parsed);
        };

        handleMessage.add(onAck);

        return sendMessage(Header.of(Verbs.EVENT_SUBSCRIBE, Destination.EMPTY, correlationId), subscription)
                .thenCompose(ignored -> ack)
                .whenComplete((ignored, failure) -> handleMessage.remove(onAck))
                .thenAccept(result -> {
                    if (!result.ok())
                        throw new IllegalStateException("Failed to subscribe to event '" + subscription.name() + "': " + result.error());
                });
    }

    /**
     * Sends a message, filling in the header fields Router owns and leaving anything already set
     * alone.
     *
     * The single send path: every request helper and every fire-and-forget send goes through it, so
     * from and time are stamped in one place. from defaults to this client's RouteId when null, time
     * to now when 0. The caller owns to, type, replyTo and ttl.
     */
    public CompletableFuture<Void> sendMessage(Header header, Object body) {
        ClientInfo clientInfo = this.clientInfo;
        Connection connection = this.connection;

        if (clientInfo == null || connection == null || connection.state() != ConnectionState.OPEN)
            return CompletableFuture.failedFuture(new IllegalStateException("Router is not connected."));

        Header enriched = header;
        if (enriched.from() == null) enriched = enriched.withFrom(clientInfo.routeId());
        if (enriched.time() == 0) enriched = enriched.withTime(System.currentTimeMillis());

        Wire.Prepared prepared = Wire.prepareBody(enriched, body);
        return connection.send(prepared.header(), prepared.body());
    }

    /**
     * Queues an inbound frame onto the ordering thread.
     *
     * Subscribed to Connection.onFrame by connectTo, and the reason wire order survives a transport
     * that promises nothing about delivery threads.
     *
     * The frame's body is a plain String, so unlike the C# byte path nothing here has a lifetime that
     * ends when this returns.
     */
    private void onFrame(WireFrame frame) {
        inbound.execute(() -> {
            try {
                dispatch(frame);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Handler threw while dispatching a " + frame.header().type() + " frame", e);
            }
        });
    }

    private void dispatch(WireFrame frame) {
        Header header = frame.header();

        // The Broker's registration ack completes the pending register call, handing back the enriched
        // ClientInfo. Only while a registration is actually pending — the slot is cleared when register
        // settles, so a later ack surfaces as ordinary traffic.
        if (header.from() == null && Verbs.CLIENT_REGISTERED.equals(header.type())) {
            CompletableFuture<ClientInfo> pending = registration.get();
            if (pending != null) {
                ClientInfo enriched = Wire.parseBodyAs(frame, ClientInfo.class);
                if (enriched != null) pending.complete(enriched);
                return;
            }
        }

        // Events route by the name in their body, not by the verb, so they go to the name-keyed table
        // instead of handleMessage — whose subscribers would each have to re-parse the body to tell
        // one event from another.
        if (header.from() == null && (Verbs.EVENT.equals(header.type()) || Verbs.EVENT_REPLAY.equals(header.type()))) {
            dispatchEvent(frame);
            return;
        }

        // Unlike the Broker, a client is its body's actual consumer, so it is parsed eagerly and handed
        // to subscribers as a tree to read as whatever their verb implies.
        JsonNode body = Wire.parseBodyAsJson(frame);
        if (body == null) {
            LOG.log(Level.WARNING, "Received frame whose body could not be parsed as JSON; dropping");
            return;
        }

        handleMessage.emit(new Message<>(header, body));
    }

    /**
     * Deserializes an event frame's body into the shape its name implies and invokes the handlers
     * registered for it — the one extra parse the "name in the body" design costs, which is why the
     * shape is chosen by the name here, once, rather than in every handler.
     */
    private void dispatchEvent(WireFrame frame) {
        JsonNode body = Wire.parseBodyAsJson(frame);
        if (body == null) {
            LOG.log(Level.WARNING, "Received event frame whose body could not be parsed as JSON; dropping");
            return;
        }

        JsonNode nameNode = body.get("name");
        if (nameNode == null || !nameNode.isTextual()) {
            LOG.log(Level.WARNING, "Received event frame with no name; dropping");
            return;
        }

        String name = nameNode.asText();

        List<BiConsumer<Events.Event, Events.Origin>> handlers;
        synchronized (eventHandlers) {
            List<BiConsumer<Events.Event, Events.Origin>> registered = eventHandlers.get(name);
            if (registered == null || registered.isEmpty()) return;
            handlers = List.copyOf(registered);
        }

        // The replay verb carries the whole subscribe-time snapshot in one frame, so it fans out to the
        // handler occurrence by occurrence — the handler's contract is one occurrence at a time either
        // way, and only the origin differs.
        if (Verbs.EVENT_REPLAY.equals(frame.header().type())) {
            Events.Replay replay = Json.convert(body, Events.Replay.class);
            if (replay == null) {
                LOG.log(Level.WARNING, "Received event.replay for ''{0}'' whose body did not parse; dropping", name);
                return;
            }

            for (Events.Event occurrence : replay.occurrences())
                for (BiConsumer<Events.Event, Events.Origin> handler : handlers)
                    handler.accept(occurrence, Events.Origin.REPLAY);

            return;
        }

        Events.Event occurrence = switch (name) {
            case Events.Names.CLIENT_CONNECTED, Events.Names.CLIENT_DISCONNECTED ->
                    Json.convert(body, Events.ClientEvent.class);
            case Events.Names.CHANNEL_REGISTERED, Events.Names.CHANNEL_REMOVED ->
                    Json.convert(body, Events.ChannelEvent.class);
            case Events.Names.TOPIC_REGISTERED, Events.Names.TOPIC_SUBSCRIBED, Events.Names.TOPIC_UNSUBSCRIBED ->
                    Json.convert(body, Events.TopicEvent.class);
            default -> null;
        };

        if (occurrence == null) {
            LOG.log(Level.WARNING, "Received event ''{0}'' whose body did not parse; dropping", name);
            return;
        }

        for (BiConsumer<Events.Event, Events.Origin> handler : handlers)
            handler.accept(occurrence, Events.Origin.LIVE);
    }

    @Override
    public void close() {
        closed = true;

        Connection connection = this.connection;
        if (connection != null) {
            connection.onFrame().remove(frameHandler);
            connection.close();
        }

        inbound.shutdownNow();
    }

}
