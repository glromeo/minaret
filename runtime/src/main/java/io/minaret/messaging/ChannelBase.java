package io.minaret.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import io.minaret.messaging.ControlBodies.BrokerAck;
import io.minaret.util.Json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Base for {@link ChannelProvider} and {@link ChannelClient}: a connected {@link Router} plus the topic
 * dispatch table both sides share. One handleMessage subscription however many topics are registered.
 *
 * Handlers are keyed by TopicId, the Broker-assigned identity in the header's destination, not by the
 * verb — which is identical for every topic.
 *
 * Callers still work in names. topicIds maps name to id as the Broker reveals it, and pendingByName
 * parks handlers registered before their id is known so nothing is missed in between.
 */
public abstract class ChannelBase {

    /**
     * The request currently being handled.
     *
     * The message pump sets it around every handler call; ChannelProvider.dispatch reads it when no
     * header is passed. That is what makes a bare dispatch from inside a handler route back to the one
     * client that asked, with nothing threaded through by hand.
     *
     * A ThreadLocal, so valid only for the *synchronous* part of a handler — matching the JavaScript
     * port's plain variable rather than C#'s AsyncLocal, which has no portable equivalent here. A
     * handler replying after an async hop must pass the header it received, which the binding layer
     * does anyway: resting a reply's correctness on where a handler yields is a poor bargain even when
     * the language would carry it.
     */
    static final ThreadLocal<Header> IN_FLIGHT_REQUEST = new ThreadLocal<>();

    private final Map<String, List<BiConsumer<Header, JsonNode>>> pendingByName = new HashMap<>();
    private final Map<UUID, List<BiConsumer<Header, JsonNode>>> topicHandlers = new HashMap<>();
    private final Map<String, UUID> topicIds = new HashMap<>();
    private final Object tableLock = new Object();

    private final Router router;

    ChannelBase(Router router) {
        this.router = router;
        router.handleMessage().add(this::onMessage);
    }

    public Router router() {
        return router;
    }

    /**
     * The channel this handle addresses: its own name on a provider, the resolved name on a client.
     *
     * destinationFor needs it because a topic name is unambiguous only alongside its channel — and the
     * handle a call was made on is exactly what says which channel that is, so the sender states it
     * rather than making the Broker guess.
     */
    public abstract String channelName();

    /**
     * Whether this handle *is* the channel (a provider) or merely resolved it (a client).
     *
     * TopicCall reads it when planning a proxy, which is how the same annotated interface means "what
     * I send to the provider" on one handle and "what I send to clients" on the other. Direction comes
     * from the handle, never from the annotation.
     */
    abstract boolean isProvider();

    /**
     * Registers a handler for a topic the way *this* handle does it: topic.register on a provider,
     * topic.subscribe on a client.
     *
     * Declared here rather than on each subclass because the two share a signature exactly, which is
     * what lets TopicBinding stay direction-agnostic — the handle decides the verb, the caller only
     * names the topic.
     *
     * Calling it twice for one topic keeps both handlers, invoked in registration order.
     */
    public abstract CompletableFuture<Void> subscribe(String topic, BiConsumer<Header, JsonNode> handler);

    /**
     * Builds an implementation of the annotated interface whose every call sends on this handle.
     *
     * The handle fixes the direction and the signature picks the delivery, so one annotation pair
     * covers all four flows: on a client, calls dispatch to the provider, awaiting a reply when the
     * method returns a future; on a provider they publish to every subscriber, or notify one client
     * when the method leads with a recipient.
     *
     * The whole contract is planned here, so an ill-formed method throws from this call rather than
     * from whichever later call first reaches it.
     */
    public <T> T proxy(Class<T> contract) {
        return ChannelProxy.create(this, contract);
    }

    /**
     * Registers each annotated method on handlers and declares it to the Broker the way this handle
     * does.
     *
     * Goes through {@link #subscribe}, so it is direction-agnostic: the same call registers topics on
     * a provider and subscribes to them on a client.
     *
     * A method returning a value replies with it, correlated to the request it answered; one returning
     * void answers nothing.
     */
    public CompletableFuture<Void> bind(Object handlers) {
        return TopicBinding.bindAll(this, handlers);
    }

    /**
     * Registers a handler for a topic, locally — the wire half is the caller's, in
     * {@link #subscribe}.
     *
     * Handlers combine rather than replace, so several independent subscribers on one handle can each
     * react to a topic without stepping on each other. They run in registration order.
     *
     * The topic's id may not be known yet, since it arrives on the ack, so the handler is parked by
     * name and promoted by {@code learnTopicId} when it lands.
     */
    void addTopicHandler(String topic, BiConsumer<Header, JsonNode> handler) {
        synchronized (tableLock) {
            UUID topicId = topicIds.get(topic);
            if (topicId != null) topicHandlers.computeIfAbsent(topicId, ignored -> new ArrayList<>()).add(handler);
            else pendingByName.computeIfAbsent(topic, ignored -> new ArrayList<>()).add(handler);
        }
    }

    /**
     * Removes every handler registered for a topic, from both tables.
     *
     * ChannelClient.unsubscribe calls it, after its ack.
     *
     * All of them go, not one. The caller names a topic rather than passing a handler to subtract,
     * which matches the wire operation this accompanies: the Broker unlinks the connection's one
     * subscription node, so any handler left armed could no longer receive anything.
     */
    void removeTopicHandlers(String topic) {
        synchronized (tableLock) {
            pendingByName.remove(topic);

            UUID topicId = topicIds.get(topic);
            if (topicId != null) topicHandlers.remove(topicId);

            // The name-to-id mapping deliberately stays: the topic still exists Broker-side and this
            // handle may still publish, dispatch, or re-subscribe.
        }
    }

    /**
     * Records the id the Broker assigned to a topic and promotes any handlers parked under its name.
     *
     * Three callers, all acting on an ack that carries a topic id: ChannelProvider.subscribe,
     * ChannelClient.subscribe, and the topic.resolved branch of the message pump — the last being how
     * a handle that addressed a topic by path learns the id for later sends.
     */
    void learnTopicId(String topic, UUID topicId) {
        synchronized (tableLock) {
            topicIds.put(topic, topicId);

            List<BiConsumer<Header, JsonNode>> parked = pendingByName.remove(topic);
            if (parked != null) topicHandlers.computeIfAbsent(topicId, ignored -> new ArrayList<>()).addAll(parked);
        }
    }

    /**
     * The cached id for a topic, or the /channel/topic path when the Broker hasn't revealed one yet.
     *
     * Every send site goes through it — dispatch, publish, notify, call — which is what keeps
     * id-vs-path addressing internal while every public signature takes a name.
     *
     * The path form is self-resolving: the Broker acks the id back as topic.resolved, so the next send
     * carries it.
     */
    Destination destinationFor(String topic) {
        synchronized (tableLock) {
            UUID topicId = topicIds.get(topic);
            return topicId != null ? Destination.ofId(topicId) : Destination.ofPath(channelName(), topic);
        }
    }

    private void onMessage(Message<JsonNode> message) {
        Header header = message.header();

        // An unsolicited topic.resolved tells us the id of a topic we addressed by name; cache it so
        // subsequent sends carry the id, and so its handlers become reachable by id — but only on the
        // handle for that channel, since every handle on the connection sees the ack.
        if (Verbs.TOPIC_RESOLVED.equals(header.type())) {
            BrokerAck resolved = Json.convert(message.body(), BrokerAck.class);
            if (resolved != null && resolved.ok() && resolved.topicId() != null
                    && channelName().equals(resolved.channel())
                    && resolved.topics() != null && resolved.topics().size() == 1)
                learnTopicId(resolved.topics().get(0), resolved.topicId());

            return;
        }

        UUID topicId = header.to().id();
        if (topicId == null) return;

        List<BiConsumer<Header, JsonNode>> handlers;
        synchronized (tableLock) {
            List<BiConsumer<Header, JsonNode>> registered = topicHandlers.get(topicId);
            if (registered == null || registered.isEmpty()) return;
            handlers = List.copyOf(registered);
        }

        // Publish the request being handled so a dispatch made from inside the handler correlates to it
        // automatically. Restored afterwards rather than cleared, so nested handling does not lose an
        // outer request's correlation.
        Header previous = IN_FLIGHT_REQUEST.get();
        IN_FLIGHT_REQUEST.set(header);
        try {
            for (BiConsumer<Header, JsonNode> handler : handlers) handler.accept(header, message.body());
        } finally {
            if (previous == null) IN_FLIGHT_REQUEST.remove();
            else IN_FLIGHT_REQUEST.set(previous);
        }
    }

    /**
     * Sends a Broker-addressed control message and awaits the ack of the named type.
     *
     * Every control-plane round trip on a handle goes through it: channel register and resolve, topic
     * register, subscribe, unsubscribe and list. The ack type is named by the caller rather than
     * derived, since acks follow no mechanical convention — channel.resolve acks as channel.resolved.
     *
     * Stamps a fresh correlation id, which the Broker echoes. Dispatch reaches every subscriber, so
     * without it two concurrent requests of one ack type could complete each other's future.
     */
    CompletableFuture<BrokerAck> request(String type, Object body, String ackType) {
        return awaitAck(Header.of(type, Destination.EMPTY, UUID.randomUUID()), body, ackType);
    }

    private CompletableFuture<BrokerAck> awaitAck(Header header, Object body, String ackType) {
        UUID correlationId = header.replyTo();
        CompletableFuture<BrokerAck> ack = new CompletableFuture<>();

        Consumer<Message<JsonNode>> onAck = message -> {
            if (!ackType.equals(message.header().type())) return;
            if (!correlationId.equals(message.header().replyTo())) return;

            BrokerAck parsed = Json.convert(message.body(), BrokerAck.class);
            if (parsed != null) ack.complete(parsed);
        };

        router.handleMessage().add(onAck);

        return router.sendMessage(header, body)
                .thenCompose(ignored -> ack)
                .whenComplete((ignored, failure) -> router.handleMessage().remove(onAck));
    }

    /**
     * Sends one topic-addressed frame and awaits nothing.
     *
     * The catch-all behind the annotated surface's three fire-and-forget kinds, which differ only in
     * the verb and in what replyTo carries:
     * - a client's fire-and-forget dispatch sends none at all, since a correlation id the Broker
     *   records and nobody consumes leaks an entry in its pending-reply table;
     * - a notify puts the recipient there;
     * - a reply puts the id it is answering.
     */
    CompletableFuture<Void> sendOnTopic(String verb, String topic, Object body, UUID replyTo) {
        return router.sendMessage(Header.of(verb, destinationFor(topic), replyTo), body);
    }

    /**
     * Dispatches a body on a topic and completes with the provider's reply.
     *
     * What an annotated answering method compiles down to, and the only request/reply path on a handle.
     *
     * Correlated through handleMessage rather than a topic handler. The Broker routes a reply by
     * correlation id regardless of subscription, so a caller doing request/reply must not have to join
     * the topic's fanout just to hear its own answer.
     *
     * No implicit timeout — bounding the wait is the caller's choice, spelled {@code orTimeout}.
     */
    CompletableFuture<JsonNode> callOnTopic(String topic, Object body) {
        UUID correlationId = UUID.randomUUID();
        CompletableFuture<JsonNode> reply = new CompletableFuture<>();

        Consumer<Message<JsonNode>> onReply = message -> {
            if (!Verbs.DISPATCH.equals(message.header().type())) return;
            if (!correlationId.equals(message.header().replyTo())) return;

            // A frame this client sent is never a reply to it. The request carries the same verb and the
            // same correlation id as the answer it waits for, so origin is the only thing separating
            // them — and anything looping a send back inbound would otherwise complete the call with its
            // own argument.
            if (message.header().from() != null && message.header().from().equals(router.routeId())) return;

            reply.complete(message.body());
        };

        router.handleMessage().add(onReply);

        return router.sendMessage(Header.of(Verbs.DISPATCH, destinationFor(topic), correlationId), body)
                .thenCompose(ignored -> reply)
                .whenComplete((ignored, failure) -> router.handleMessage().remove(onReply));
    }

    /**
     * ChannelProvider.publish's body.
     *
     * Lives here so the annotated surface can reach it without knowing which subclass the handle is.
     * TopicCall checks the shape when the proxy is planned, so only a provider handle ever arrives.
     */
    CompletableFuture<Void> publishOnTopic(String topic, Object body) {
        Header header = Header.of(Verbs.PUBLISH, destinationFor(topic), UUID.randomUUID());

        return awaitAck(header, body, Verbs.PUBLISHED).thenAccept(ack -> {
            if (!ack.ok()) throw new IllegalStateException("Failed to publish '" + topic + "': " + ack.error());
        });
    }

}
