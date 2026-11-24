import { resolveBrokerUrl } from "@messagebus/client/discovery";
import { createDeferred } from "../utils/async.js";
import { consoleLogger } from "../utils/logger.js";
import { Destination, header } from "./message.js";
import { Router } from "./router.js";
import { newGuid } from "./utils/guid.js";
import { Verbs } from "./verbs.js";
import { WebSocketConnection } from "./websocket-connection.js";

/**
 * The Layer-1 client surface over Router, mirroring runtime's Channels.
 *
 * The wire bodies for the channel/topic control plane are not declared here as types — they are
 * plain object literals at the call sites below (`{ channelName }`, `{ channelName, topic }`), and
 * the shapes live in channels.d.ts. Nothing is shared with the Broker except the JSON itself.
 *
 * Not ported: Layer 2's annotated surface (runtime's Channel, Topic, ChannelProxy). It is built on
 * annotations and a reflective proxy, and the JavaScript equivalents — decorators and metadata —
 * need a transpile step this package exists to avoid. See CLAUDE.md.
 */

/**
 * The request currently being handled.
 *
 * The message pump sets it around every handler call; `dispatch` reads it when no header is passed.
 * That is what makes a bare `provider.dispatch(topic, body)` from inside a handler route back to the
 * one client that asked, with nothing threaded through by hand.
 *
 * Valid only for the *synchronous* part of a handler. There is no portable async-context carrier,
 * since AsyncLocalStorage is Node-only, so this is a plain variable saved and restored around the
 * call. A handler replying after an await must pass the header it received — which runtime's binding
 * layer does anyway, for the related reason that resting a reply's correctness on where a handler yields is a poor bargain even when the language carries it.
 */
let inFlightRequest = null;

/**
 * Base for ChannelProvider and ChannelClient: a connected Router plus the topic dispatch table both
 * sides share. One handleMessage subscription however many topics are registered.
 *
 * Handlers are keyed by TopicId, the Broker-assigned identity in Header.to, not by Header.type —
 * which names the verb and is identical for every topic.
 *
 * Callers still work in names. #topicIds maps name to id as the Broker reveals it, on a
 * subscribe/register ack or a topic.resolved after a path send, and #pendingByName parks handlers
 * registered before their id is known so nothing is missed in between.
 */
export class ChannelBase {

    #pendingByName = new Map();

    #topicHandlers = new Map();

    #topicIds = new Map();

    constructor(router, log) {
        this.router = router;
        this.log = log;
        router.handleMessage.add(message => this.#onMessage(message));
    }

    /**
     * The channel this handle addresses: its own name on a provider, the resolved name on a client.
     *
     * #destinationFor needs it because a topic name is unambiguous only alongside its channel — and
     * the handle a call was made on is exactly what says which channel that is, so the sender states
     * it rather than making the Broker guess.
     */
    get channelName() {
        throw new Error("channelName is abstract");
    }

    /**
     * Whether this handle *is* the channel (a provider) or merely resolved it (a client).
     */
    get isProvider() {
        throw new Error("isProvider is abstract");
    }

    /**
     * Records the id the Broker assigned to topic and promotes any handlers registered against the
     * name before the id was known.
     */
    learnTopicId(topic, topicId) {
        this.#topicIds.set(topic, topicId);

        const pending = this.#pendingByName.get(topic);
        if (pending === undefined) return;

        this.#pendingByName.delete(topic);
        const existing = this.#topicHandlers.get(topicId);
        if (existing) existing.push(...pending);
        else this.#topicHandlers.set(topicId, pending);
    }

    /**
     * Registers handler for topic, locally — the wire half belongs to the caller.
     *
     * Handlers combine rather than replace, so several independent subscribers on one handle can each
     * react to a topic without stepping on each other. They run in registration order.
     *
     * The topic's id may not be known yet, since it arrives on the ack, so the handler is parked by
     * name and promoted to the id-keyed table by learnTopicId when it lands.
     */
    addTopicHandler(topic, handler) {
        const topicId = this.#topicIds.get(topic);
        const table = topicId === undefined ? this.#pendingByName : this.#topicHandlers;
        const key = topicId ?? topic;

        const existing = table.get(key);
        if (existing) existing.push(handler);
        else table.set(key, [handler]);
    }

    /**
     * Removes every handler registered for topic, from both tables — the mirror of addTopicHandler,
     * which may have put them in either.
     *
     * `unsubscribe` calls it, after its ack.
     *
     * All of them go, not one. The caller names a topic rather than passing a handler to subtract,
     * which matches the wire operation this accompanies: the Broker unlinks the connection's one
     * subscription node, so any handler left armed could no longer receive anything.
     */
    removeTopicHandlers(topic) {
        this.#pendingByName.delete(topic);

        const topicId = this.#topicIds.get(topic);
        if (topicId !== undefined) this.#topicHandlers.delete(topicId);

        // The name->id mapping deliberately stays: the topic still exists Broker-side and this
        // handle may publish or dispatch to it, or re-subscribe. Dropping it would only force a
        // needless path round-trip to relearn an id that hasn't changed.
    }

    /**
     * The cached id for topic, or the /channel/topic path when the Broker hasn't revealed one yet.
     *
     * Every send site goes through it — dispatch, publish, notify, call — which is what keeps
     * id-vs-path addressing internal while every public signature takes a name.
     *
     * The path form is self-resolving: the Broker acks the id back as topic.resolved, so the next
     * send carries it.
     */
    destinationFor(topic) {
        const topicId = this.#topicIds.get(topic);

        return topicId === undefined ? Destination.fromPath(this.channelName, topic) : Destination.fromId(topicId);
    }

    /**
     * Sends one topic-addressed frame and awaits nothing.
     *
     * The catch-all behind the three fire-and-forget kinds, which differ only in the verb and in what
     * replyTo carries:
     * - a client's fire-and-forget dispatch sends none at all, since a correlation id the Broker
     *   records and nobody consumes leaks an entry in its pending-reply table;
     * - a provider's notify puts the *recipient* there;
     * - a provider's reply puts the id it is answering.
     */
    sendOnTopic(verb, topic, body, replyTo, { signal } = {}) {
        return this.router.sendMessage(header(verb, this.destinationFor(topic), { replyTo }), body, { signal });
    }

    /**
     * Dispatches body on topic and completes with the provider's reply — the only request/reply path
     * on a handle.
     *
     * Correlated by replyTo through Router.handleMessage rather than a topic handler. The Broker
     * routes a reply by correlation id regardless of subscription, so a caller doing request/reply
     * must not have to join the topic's fanout just to hear its own answer.
     *
     * No implicit timeout, matching Router.connect — pass a signal to bound the wait.
     */
    async callOnTopic(topic, body, { signal } = {}) {
        const correlationId = newGuid();
        const reply = createDeferred(signal);

        const onReply = message => {
            if (message.header.type !== Verbs.Dispatch || message.header.replyTo !== correlationId) return;

            // A frame this client sent is never a reply to it. The request carries the same verb and
            // the same correlation id as the answer it awaits, so the two are told apart only by
            // origin — and anything that loops a send back inbound (a shared transport, a provider
            // subscribed to its own topic) would otherwise complete the call with its own argument.
            if (message.header.from === this.router.routeId) return;

            reply.resolve(message.body);
        };

        this.router.handleMessage.add(onReply);
        try {
            await this.router.sendMessage(
                header(Verbs.Dispatch, this.destinationFor(topic), { replyTo: correlationId }),
                body,
                { signal }
            );

            return await reply.promise;
        } finally {
            this.router.handleMessage.remove(onReply);
        }
    }

    /**
     * ChannelProvider.publish's body, here so both halves of the request/ack pair live beside the
     * other topic-addressed helpers.
     */
    async publishOnTopic(topic, body, { signal } = {}) {
        const ack = await this.requestOnTopic(
            Verbs.Publish,
            this.destinationFor(topic),
            body,
            Verbs.Published,
            newGuid(),
            signal
        );

        if (!ack.ok) throw new Error(`Failed to publish '${topic}': ${ack.error}`);
    }

    /**
     * Sends a Broker-addressed control message and awaits the ack of ackType.
     *
     * Every control-plane round trip on a handle goes through it: channel register and resolve, topic
     * register, subscribe, unsubscribe and list. ackType is named by the caller rather than derived,
     * since acks follow no mechanical convention — channel.resolve acks as channel.resolved.
     *
     * Stamps a fresh replyTo, which the Broker echoes. handleMessage dispatch is multicast, so
     * without it two concurrent requests of one ackType could complete each other's promise.
     */
    request(type, body, ackType, signal) {
        return this.#awaitAck(header(type, Destination.empty, { replyTo: newGuid() }), body, ackType, signal);
    }

    /**
     * `request`'s data-plane counterpart: sends a topic-addressed message and awaits its ack.
     *
     * `publish` is the caller. The body is the payload itself — no request envelope wraps it, unlike
     * the control plane, since the topic rides in the header.
     */
    requestOnTopic(type, to, body, ackType, correlationId, signal) {
        return this.#awaitAck(header(type, to, { replyTo: correlationId }), body, ackType, signal);
    }

    async #awaitAck(request, body, ackType, signal) {
        const ack = createDeferred(signal);

        const onAck = message => {
            if (message.header.type !== ackType || message.header.replyTo !== request.replyTo) return;
            ack.resolve(message.body);
        };

        this.router.handleMessage.add(onAck);
        try {
            await this.router.sendMessage(request, body, { signal });
            return await ack.promise;
        } finally {
            this.router.handleMessage.remove(onAck);
        }
    }

    #onMessage(message) {
        // An unsolicited topic.resolved names the id of a topic this handle addressed by path. Cached,
        // so later sends carry the id and the topic's handlers become reachable by it — but only by the
        // handle on that channel, since every handle on the connection sees the ack.
        if (message.header.type === Verbs.TopicResolved) {
            const resolved = message.body;
            if (resolved?.ok && resolved.topicId && resolved.channel === this.channelName && resolved.topics?.length === 1)
                this.learnTopicId(resolved.topics[0], resolved.topicId);

            return;
        }

        const topicId = message.header.to.id;
        if (topicId === null) return;

        const handlers = this.#topicHandlers.get(topicId);
        if (handlers === undefined) return;

        // Publishes the request being handled, so a dispatch made from inside the handler correlates
        // to it with nothing passed. Restored afterwards rather than cleared, so nested handling
        // doesn't lose an outer request's correlation.
        const previous = inFlightRequest;
        inFlightRequest = message.header;
        try {
            for (const handler of [...handlers]) handler(message.body, message.header);
        } finally {
            inFlightRequest = previous;
        }
    }

}

/**
 * Provider-side handle for a channel: claims the name with channel.register, declares the topics it
 * handles with topic.register, and sends on them.
 *
 * Three ways to send, differing in who receives: `dispatch` answers the one client that asked,
 * `publish` broadcasts to every subscriber, `notify` reaches one named client unsolicited.
 *
 * Constructed only via MessageBus.createChannel.
 */
export class ChannelProvider extends ChannelBase {

    #channel;

    #channelRouteId;

    constructor(router, log, channel) {
        super(router, log);
        this.#channel = channel;
        this.#channelRouteId = router.routeId;
    }

    /**
     * The RouteId this provider is registered under — also the channel's identity, since there is
     * exactly one provider per channel name.
     */
    get channelRouteId() {
        return this.#channelRouteId;
    }

    /** The name this provider claimed, as sent in the /channel/topic form of a first publish. */
    get channel() {
        return this.#channel;
    }

    get channelName() {
        return this.#channel;
    }

    get isProvider() {
        return true;
    }

    static async create(router, log, channelName, { signal } = {}) {
        if (router.routeId === null) throw new Error("Router is not connected.");

        const provider = new ChannelProvider(router, log, channelName);

        const ack = await provider.request(Verbs.ChannelRegister, { channelName }, Verbs.ChannelRegistered, signal);
        if (!ack.ok) throw new Error(`Failed to register channel '${channelName}': ${ack.error}`);

        log.info(`Channel '${channelName}' registered as ${provider.channelRouteId}`);
        return provider;
    }

    /**
     * Registers a handler for inbound messages on topic, and declares the topic to the Broker with
     * topic.register — which also makes it discoverable through ChannelClient.listTopics.
     *
     * Calling it again for one topic keeps both handlers, invoked in registration order.
     */
    async subscribe(topic, handler, { signal } = {}) {
        this.addTopicHandler(topic, handler);

        const ack = await this.request(
            Verbs.TopicRegister,
            { channelName: this.channel, topic },
            Verbs.TopicRegistered,
            signal
        );

        if (!ack.ok) throw new Error(`Failed to register topic '${topic}': ${ack.error}`);

        // The ack carries the topic's Broker-assigned id. Cached, so inbound frames — always
        // id-addressed — reach the handler just registered, and so sends carry the id.
        if (ack.topicId) this.learnTopicId(topic, ack.topicId);
    }

    /**
     * Replies to a single client on topic, answering a request this provider is handling.
     *
     * Takes no recipient, and needs none: the message is addressed to the *topic* like all topic
     * traffic, and the Broker narrows delivery to the one awaiting connection by matching replyTo
     * against the request it recorded.
     *
     * The correlation id comes from `requestHeader` when given and otherwise from the request being
     * handled, so a reply sent from inside a handler needs neither argument.
     */
    dispatch(topic, body, { header: requestHeader, signal } = {}) {
        const replyTo = requestHeader?.replyTo ?? inFlightRequest?.replyTo ?? null;

        return this.sendOnTopic(Verbs.Dispatch, topic, body, replyTo, { signal });
    }

    /**
     * Broadcasts body on topic to every client currently subscribed to it.
     *
     * The Broker performs the fanout, so the awaited ack confirms only that the publish reached the
     * Broker — not that every subscriber received it. An unreachable subscriber is logged and
     * dropped, as in ordinary routing.
     */
    publish(topic, body, { signal } = {}) {
        return this.publishOnTopic(topic, body, { signal });
    }

    /**
     * Sends body on topic to one client, unsolicited.
     *
     * The point-to-point counterpart of `publish`, and the only provider-originated send naming a
     * recipient.
     *
     * Unlike `dispatch` it answers nothing, so it correlates to nothing and the recipient rides in
     * replyTo instead. No ack comes back either: a notification the recipient never asked for has
     * nothing to confirm, and awaiting one would cost every notification a round trip.
     */
    notify(recipient, topic, body, { signal } = {}) {
        return this.sendOnTopic(Verbs.Notify, topic, body, recipient, { signal });
    }

}

/**
 * Client-side handle for a channel: resolves the name to its provider's RouteId with channel.resolve,
 * then sends to it (`dispatch`, `call`) and subscribes from it (`subscribe`).
 *
 * The resolve blocks Broker-side until a provider registers, so a client may legitimately start
 * before its provider.
 *
 * Constructed only via MessageBus.connectChannel.
 */
export class ChannelClient extends ChannelBase {

    #channel;

    #channelRouteId = null;

    constructor(router, log, channel) {
        super(router, log);
        this.#channel = channel;
    }

    /** The resolved provider's RouteId for this channel. */
    get channelRouteId() {
        return this.#channelRouteId;
    }

    /**
     * The channel name this handle resolved, as sent in the /channel/topic form of a first dispatch.
     * This is the channel context the Broker would otherwise have to guess at.
     */
    get channel() {
        return this.#channel;
    }

    get channelName() {
        return this.#channel;
    }

    get isProvider() {
        return false;
    }

    static async create(router, log, channelName, { signal } = {}) {
        const client = new ChannelClient(router, log, channelName);

        // Blocks Broker-side (bounded only by the signal) until a matching provider registers.
        const ack = await client.request(Verbs.ChannelResolve, { channelName }, Verbs.ChannelResolved, signal);
        if (!ack.ok || !ack.routeId) throw new Error(`Failed to resolve channel '${channelName}': ${ack.error}`);

        log.info(`Channel '${channelName}' resolved to ${ack.routeId}`);
        client.#channelRouteId = ack.routeId;
        return client;
    }

    /**
     * Sends body to this channel's provider on topic, awaiting nothing.
     *
     * A correlation id is stamped by default, so the provider's reply *could* route back to this
     * connection alone. Pass `replyTo: null` for a genuinely fire-and-forget send, which is what
     * keeps an id nobody will consume out of the Broker's pending-reply table.
     *
     * Addressed by id once known and by /channel/topic before that, so a client can dispatch to a
     * provider's topic it never subscribed to with no resolve round-trip.
     */
    dispatch(topic, body, { replyTo, signal } = {}) {
        return this.sendOnTopic(Verbs.Dispatch, topic, body, replyTo === undefined ? newGuid() : replyTo, { signal });
    }

    /** Dispatches body on topic and resolves with the provider's reply. */
    call(topic, body, { signal } = {}) {
        return this.callOnTopic(topic, body, { signal });
    }

    /**
     * Subscribes to the provider's `publish` broadcasts for topic: arms handler locally, then
     * topic.subscribe puts this connection in the Broker's fanout list.
     *
     * Calling it again for one topic keeps both handlers, invoked in registration order.
     */
    async subscribe(topic, handler, { signal } = {}) {
        // Armed before the request goes out. The Broker marks this client eligible for fanout before
        // it acks, so a publish can land while the ack is still in flight — registering first means
        // that frame is handled instead of dropped as an unrecognised TopicId.
        this.addTopicHandler(topic, handler);

        const ack = await this.request(
            Verbs.TopicSubscribe,
            { channelName: this.channel, topic },
            Verbs.TopicSubscribed,
            signal
        );

        if (!ack.ok) throw new Error(`Failed to subscribe to '${topic}': ${ack.error}`);

        if (ack.topicId) this.learnTopicId(topic, ack.topicId);
    }

    /**
     * Stops receiving topic's fanout: topic.unsubscribe to the Broker, then the local handlers go.
     *
     * The order mirrors `subscribe` exactly, for the mirrored reason. Subscribe arms before the
     * request because fanout can arrive before the ack; unsubscribe therefore keeps the handler armed
     * *until* the ack, so a publish already in flight when the Broker unlinks is still handled rather
     * than dropped as an unrecognised TopicId.
     *
     * A failed ack leaves the handlers in place: nothing was unsubscribed, so nothing should stop
     * being delivered.
     */
    async unsubscribe(topic, { signal } = {}) {
        const ack = await this.request(
            Verbs.TopicUnsubscribe,
            { channelName: this.channel, topic },
            Verbs.TopicUnsubscribed,
            signal
        );

        if (!ack.ok) throw new Error(`Failed to unsubscribe from '${topic}': ${ack.error}`);

        this.removeTopicHandlers(topic);
    }

    /** The topics this channel's provider has registered, empty if none yet. */
    async listTopics({ signal } = {}) {
        const ack = await this.request(Verbs.TopicList, { channelName: this.channel }, Verbs.TopicListed, signal);

        return ack.topics ?? [];
    }

}

/**
 * Convenience facade over Router: `await MessageBus.connect(newClientInfo({ role: "…" }))`
 * encapsulates the discovery/transport boilerplate every call site otherwise repeats, and answers a
 * connected handle for creating or connecting to channels.
 *
 * What it does is the part that actually matters — resolving where the Broker is and
 * standing up a Router over a WebSocket to it.
 */
export class MessageBus {

    constructor(router, log) {
        this.router = router;
        this.log = log;
    }

    /**
     * Connects to the Broker and completes registration.
     *
     * `url` addresses a Broker directly; without it the URL is resolved per environment — named-pipe
     * discovery under Node, same-origin under a browser (see discovery/broker-discovery). Discovery
     * runs inside the connection factory rather than once up front, so a reconnect finds a Broker
     * that came back on a different port.
     *
     * `persistent` reconnects indefinitely on any drop, re-registering under the same RouteId and
     * re-issuing every event subscription. It defaults to false, matching MessageBus.Connect; a
     * long-lived client generally wants it true.
     */
    static async connect(clientInfo, { url, realm, signal, log = consoleLogger, persistent = false } = {}) {
        const factory = url === undefined
            ? async () => new WebSocketConnection(await resolveBrokerUrl({ realm, signal }), log)
            : () => new WebSocketConnection(url, log);

        const router = new Router(factory, { log });
        if (persistent) router.reconnect();
        await router.connect(clientInfo, { signal });

        return new MessageBus(router, log);
    }

    /**
     * Claims channelName as this connection's provider identity and answers a handle for registering
     * topics and dispatching/publishing to clients. Fails if the name is already claimed by a
     * different, still-registered provider.
     */
    createChannel(channelName, { signal } = {}) {
        return ChannelProvider.create(this.router, this.log, channelName, { signal });
    }

    /**
     * Resolves channelName to its provider — blocking Broker-side until one registers — and answers
     * a handle for dispatching to / subscribing from it.
     */
    connectChannel(channelName, { signal } = {}) {
        return ChannelClient.create(this.router, this.log, channelName, { signal });
    }

    dispose() {
        this.router.connection?.dispose();
    }

}
