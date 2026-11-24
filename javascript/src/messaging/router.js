import { createDeferred, delay } from "../utils/async.js";
import { Emitter } from "../utils/emitter.js";
import { consoleLogger } from "../utils/logger.js";
import { CloseCodes, ConnectionState } from "./connection.js";
import { EventOrigin, replayOccurrences } from "./events.js";
import { Destination, header } from "./message.js";
import { newGuid } from "./utils/guid.js";
import { prepareBody, tryParseBody } from "./utils/wire.js";
import { Verbs } from "./verbs.js";

/**
 * A client's connection to the Broker, mirroring Runtime/Messaging/Router.cs.
 *
 * A message bus, not a WebSocket wrapper: no raw string API, only the structured surface below, with
 * the transport a private detail behind a Connection.
 *
 * Takes a *factory* rather than a connection, because it is constructed once but may connect many
 * times and every reconnect needs a socket that has never been opened.
 */
export class Router {

    #connectionFactory;

    #clientInfo = null;

    #connection = null;

    /** The register ack's resolver — see #register for why this one request isn't correlated. */
    #registration = null;

    /**
     * The retry policy `connect` runs under, or null for a single attempt that fails straight to the
     * caller. Set by `reconnect`; Router state rather than a `connect` argument, so it governs the
     * first connect and every later drop alike.
     */
    #reconnectPolicy = null;

    /**
     * Event name -> handlers. Keyed by name rather than surfaced through handleMessage because an
     * event frame's Header.type is Verbs.Event for *every* event — the name that distinguishes them
     * is in the body, so dispatch has to parse before it can route.
     */
    #eventHandlers = new Map();

    /**
     * Every subscription as it was made, so a reconnect can re-issue them verbatim. An array rather
     * than a map keyed by name: two `on` calls narrowing the same name differently are additive, and
     * a map would silently drop one of the two narrowings on reconnect.
     */
    #subscriptions = [];

    /**
     * Inbound frames are handled one at a time, in arrival order, through this chain.
     *
     * It exists because decoding a body can be asynchronous — a gzipped one goes through
     * DecompressionStream — while wire order is load-bearing throughout this protocol: a
     * topic.resolved must be learned before the frame it resolves is dispatched, a replay must reach
     * a handler before the ack saying the picture is complete, a subscribe ack must land before the
     * fanout after it.
     *
     * Every frame is queued, not just compressed ones, because a mixed sync/async path is exactly
     * what reorders them: a small raw frame would overtake a large compressed one that arrived first.
     *
     * User handlers are invoked from the chain but never awaited, or a handler awaiting a request of
     * its own would deadlock the queue against its own reply.
     */
    #queue = Promise.resolve();

    #onFrameBound = frame => this.#onFrame(frame);

    constructor(connectionFactory, { log = consoleLogger, initialRetryDelayMs = 500, maxRetryDelayMs = 30_000 } = {}) {
        this.#connectionFactory = connectionFactory;
        this.log = log;
        this.initialRetryDelayMs = initialRetryDelayMs;
        this.maxRetryDelayMs = maxRetryDelayMs;

        /**
         * Raised for every inbound message that parses as an envelope. Non-enveloped frames, the
         * registration ack, and events are handled internally and never surface here.
         */
        this.handleMessage = new Emitter(log, "handleMessage");

        /**
         * Raised as the connection moves through connecting -> open -> closed, and again on every
         * reconnect.
         *
         * The Monitoring app is what needs it: a client whose point is to *show* the connection needs
         * the transitions, not a pull-only state.
         *
         * No counterpart on the C# Router, which leaves a caller to watch IConnection.State and
         * OnDisconnected separately. Router is the right home because it is the only thing that knows
         * when "open" is true in the sense that matters — after registration, not merely when the
         * socket opened.
         */
        this.onStateChange = new Emitter(log, "onStateChange");
    }

    /**
     * This client's RouteId — the Broker's enriched value once connected, the value passed to
     * `connect` before that, or null if never connected.
     */
    get routeId() {
        return this.#clientInfo?.routeId ?? null;
    }

    get connection() {
        return this.#connection;
    }

    /**
     * Arms retrying: every later `connect` retries, and the connection it adopts heals itself.
     *
     * A setter, not an action — the JavaScript shape of Router.cs's `Reconnect(min, max, maxRetries)`.
     * That is what keeps `connect` the only way to connect, with a reconnect simply `connect` running
     * again, rather than a `connect`/`connectPersistent` pair.
     *
     * A failed attempt — socket refused, registration never acked — is retried after minDelayMs,
     * doubling to maxDelayMs. A drop reported through `connection.onDisconnected` reconnects with the
     * same clientInfo, so the Broker sees a resumed client rather than a new one.
     *
     * maxRetries bounds one `connect` call and defaults to unbounded. Both it and the delay restart
     * from a successful registration, so it bounds a single outage rather than the Router's lifetime.
     * A background reconnect that exhausts it has nothing to throw to, so the Router stays down until
     * something calls `connect` again.
     */
    reconnect(minDelayMs = this.initialRetryDelayMs, maxDelayMs = this.maxRetryDelayMs, maxRetries = Infinity) {
        this.#reconnectPolicy = { minDelayMs, maxDelayMs, maxRetries };
    }

    /**
     * Connects and completes the registration round-trip, answering the enriched ClientInfo the
     * Broker sends back — so once this resolves, this Router reflects the Broker's view of it.
     *
     * The only public way to connect. With no policy armed it is one attempt whose failure is the
     * caller's; with one, it is the whole retry loop, returning on the first registration that lands
     * and throwing only once the policy is exhausted.
     *
     * The retry covers the *first* attempt too, so a client started before its Broker waits instead
     * of giving up. Every reconnect after the first is silent, observable only through onStateChange.
     *
     * No implicit timeout — with no signal this waits for the ack indefinitely, deliberately leaving
     * the bound to the caller. Aborting the signal stops an in-flight attempt and any pending retry,
     * here and in the background reconnects this arms.
     */
    async connect(clientInfo, { signal } = {}) {
        this.log.info(`Connecting router with RouteId ${clientInfo.routeId}`);

        const policy = this.#reconnectPolicy;
        let delayMs = policy?.minDelayMs ?? 0;

        for (let retries = 0; ; retries++) {
            try {
                const connection = await this.#connectionFactory();
                const registered = await this.connectTo(clientInfo, connection, { signal });

                this.log.info(`Connected to Broker with RouteId ${registered.routeId}`);
                return registered;
            } catch (error) {
                if (!policy || retries >= policy.maxRetries || signal?.aborted) throw error;

                this.log.warn(`Connect attempt failed; retrying in ${delayMs}ms`, error);
                await delay(delayMs, signal);
                delayMs = Math.min(delayMs * 2, policy.maxDelayMs);
            }
        }
    }

    /**
     * Adopts a connection and registers over it.
     *
     * `connect` reaches it once the factory has produced a connection, and it is also the seam a test
     * drives to connect a Router to a fake Connection, bypassing the factory entirely.
     *
     * The outgoing connection is detached *before* being disposed, so a late frame on the old socket
     * can neither complete nor be mistaken for the new connection's registration.
     *
     * Re-issues every event subscription once registered. The Broker's subscription state dies with
     * the connection, so without this a reconnect would leave a client silently armed for events it
     * no longer receives — handlers intact, stream gone. A no-op on a first connect.
     *
     * Arms the reconnect handler only once registration has landed, and only under a policy, so a
     * connect that never got that far is retried by its own loop rather than by a handler on a
     * connection that never worked. Fire-and-forget, since nobody awaits a drop, so its failure is
     * only logged.
     */
    async connectTo(clientInfo, connection, { signal } = {}) {
        if (this.#connection !== null) {
            this.#connection.onFrame.remove(this.#onFrameBound);
            this.#connection.dispose();
        }

        this.#clientInfo = clientInfo;
        this.#connection = connection;
        connection.onFrame.add(this.#onFrameBound);
        connection.onDisconnected.add(() => this.onStateChange.emit(ConnectionState.Closed));

        this.onStateChange.emit(ConnectionState.Connecting);
        await connection.connect({ signal });

        const registered = await this.#register(clientInfo, signal);
        this.onStateChange.emit(ConnectionState.Open);
        await this.#resubscribeAll(signal);

        if (this.#reconnectPolicy !== null)
            connection.onDisconnected.add(closeCode => {
                // The one ending that is not retried. Another connection registered under this
                // client's identity and took its RouteId, so reconnecting would take it straight
                // back and evict that one — the two trading it at socket speed. The policy is
                // cleared rather than this attempt skipped, so a later connection cannot resume
                // retrying for an identity this Router no longer holds. An explicit connect still
                // works; see CloseCodes.Superseded.
                if (closeCode === CloseCodes.Superseded) {
                    this.#reconnectPolicy = null;
                    this.log.warn(`Connection superseded by a newer registration under identity ${clientInfo.identity}; not reconnecting`);
                    return;
                }

                void this.connect(clientInfo, { signal }).catch(error => this.log.error("Reconnect abandoned", error));
            });

        return registered;
    }

    /**
     * Registers a handler for one of the Broker's well-known events, resolving when the Broker acks.
     *
     * Layer 0: it hangs off Router rather than a channel handle, so a client can watch the Broker
     * without having created or resolved any channel.
     *
     * Awaiting it means "I have the full picture", not merely "I am subscribed": a presence-shaped
     * event replays what is already true before the ack. The handler is armed before the request goes
     * out for that reason. Replayed occurrences carry EventOrigin.Replay, so a handler can tell
     * inherited state from news.
     *
     * Calling it twice for one name keeps both handlers, invoked in registration order.
     *
     * The argument may be a bare name or an EventSubscription narrowing by channel, topic or where —
     * the *wire body itself* rather than a parallel options type, so a new filter axis stays a
     * property on one shape instead of another parameter here.
     */
    async on(subscription, handler, { signal } = {}) {
        const options = typeof subscription === "string" ? { name: subscription } : subscription;

        const armed = this.#eventHandlers.get(options.name);
        if (armed) armed.push(handler);
        else this.#eventHandlers.set(options.name, [handler]);

        this.#subscriptions.push(options);

        await this.#subscribe(options, signal);
    }

    /**
     * Sends a message, filling in the header fields Router owns and leaving anything already set
     * alone.
     *
     * The single send path: every request helper and every fire-and-forget send goes through it, so
     * `from` and `time` are stamped in one place. `from` defaults to this client's RouteId when null,
     * `time` to now when 0. The caller owns to, type, replyTo and ttl.
     */
    async sendMessage(head, body, { signal } = {}) {
        if (this.#clientInfo === null || this.#connection?.state !== ConnectionState.Open)
            throw new Error("Router is not connected.");

        const enriched = {
            ...head,
            from: head.from ?? this.#clientInfo.routeId,
            time: head.time === 0 ? Date.now() : head.time
        };

        const prepared = await prepareBody(enriched, body);
        signal?.throwIfAborted();

        await this.#connection.send(prepared.header, prepared.body);
    }

    // ── Registration ────────────────────────────────────────────────────────────────────────────

    /**
     * Sends the register handshake and awaits its ack.
     *
     * The Broker replies with a ClientInfo enriched by its clock-offset estimate, awaited here so
     * that once `connect` returns this Router already holds the Broker's view.
     *
     * `#handleFrame` matches the ack by it being Broker-originated with the registered verb, not by a
     * correlation id: the handshake predates having a RouteId the Broker knows, which makes it the
     * one request in the protocol not correlated by replyTo.
     */
    async #register(clientInfo, signal) {
        const registration = createDeferred(signal);
        this.#registration = registration;

        try {
            await this.sendMessage(
                header(Verbs.ClientRegister, Destination.empty, { from: clientInfo.routeId }),
                clientInfo,
                { signal }
            );

            this.#clientInfo = await registration.promise;
            return this.#clientInfo;
        } finally {
            // Cleared however this ends, so a later client.registered frame is treated as ordinary
            // traffic instead of being swallowed by the ack branch. Guarded so a concurrent
            // reconnect can't have its own newer slot nulled out by this one's unwinding.
            if (this.#registration === registration) this.#registration = null;
        }
    }

    // ── Event subscriptions ─────────────────────────────────────────────────────────────────────

    /**
     * Re-issues every subscription against the new connection.
     *
     * Called from `connectTo` rather than the disconnect handler, so every connect — first or
     * reconnect — counts as finished only once the picture is whole again.
     *
     * No counterpart in Router.cs, which re-registers on reconnect but does not re-subscribe. See
     * CLAUDE.md's divergence list.
     */
    async #resubscribeAll(signal) {
        await Promise.all(this.#subscriptions.map(options => this.#subscribe(options, signal)));
    }

    /**
     * Sends one event.subscribe and awaits its ack.
     *
     * Called by `on` and again by `#resubscribeAll` on every reconnect, which is why it takes the
     * subscription options rather than reading them back off the handler table.
     *
     * Keeps its own small request/ack path rather than reusing ChannelBase's: the two carry different
     * body types, and hoisting the Layer-1 helper would drag BrokerAck into Layer 0's surface.
     */
    async #subscribe(options, signal) {
        const correlationId = newGuid();
        const ack = createDeferred(signal);

        const onAck = message => {
            if (message.header.type !== Verbs.EventSubscribed || message.header.replyTo !== correlationId) return;
            ack.resolve(message.body);
        };

        this.handleMessage.add(onAck);
        try {
            await this.sendMessage(
                header(Verbs.EventSubscribe, Destination.empty, { replyTo: correlationId }),
                options,
                { signal }
            );

            const result = await ack.promise;
            if (!result.ok) throw new Error(`Failed to subscribe to event '${options.name}': ${result.error}`);
        } finally {
            this.handleMessage.remove(onAck);
        }
    }

    // ── Inbound frames ──────────────────────────────────────────────────────────────────────────

    #onFrame(frame) {
        this.#queue = this.#queue
            .then(() => this.#handleFrame(frame))
            .catch(error => this.log.warn("Frame handling threw", error));
    }

    async #handleFrame(frame) {
        const head = frame.header;

        if (head.from === null && head.type === Verbs.ClientRegistered && this.#registration !== null) {
            const enriched = await tryParseBody(frame);
            if (enriched !== undefined) this.#registration.resolve(enriched);

            return;
        }

        // Events route by the name in their body, not by Header.type — "event" for every live event
        // and "event.replay" for the snapshot — so they go to the name-keyed table instead of
        // handleMessage, whose subscribers would each have to re-parse the body to tell them apart.
        if (head.from === null && (head.type === Verbs.Event || head.type === Verbs.EventReplay)) {
            await this.#dispatchEvent(frame);
            return;
        }

        // Unlike the Broker, a client is the body's actual consumer, so it is parsed eagerly here
        // and handed to subscribers already decoded.
        const body = await tryParseBody(frame);
        if (body === undefined) {
            this.log.warn("Received frame whose body could not be parsed as JSON; dropping");
            return;
        }

        this.handleMessage.emit({ header: head, body });
    }

    /**
     * Parses an event frame and invokes the handlers registered for its name.
     *
     * `#handleFrame` routes every event and event.replay frame here. This is the one extra parse the
     * "name in the body" design costs — the body must be read to learn which event this is — and what
     * it buys is Header.type staying purely the verb.
     *
     * A replay frame carries the whole subscribe-time snapshot and is fanned out occurrence by
     * occurrence, so a handler's contract is one occurrence at a time however the Broker framed it.
     * Only the origin differs.
     */
    async #dispatchEvent(frame) {
        const body = await tryParseBody(frame);
        if (body === undefined || typeof body.name !== "string") {
            this.log.warn("Received event frame with no name; dropping");
            return;
        }

        const handlers = this.#eventHandlers.get(body.name);
        if (handlers === undefined) return;

        const occurrences = frame.header.type === Verbs.EventReplay ? replayOccurrences(body) : [body];
        const origin = frame.header.type === Verbs.EventReplay ? EventOrigin.Replay : EventOrigin.Live;

        for (const occurrence of occurrences)
            for (const handler of [...handlers])
                try {
                    handler(occurrence, origin);
                } catch (error) {
                    // One handler throwing must not cost the others their event.
                    this.log.error(`Event handler for '${body.name}' threw`, error);
                }
    }

}

/**
 * A ClientInfo for a new connection, mirroring C#'s `new ClientInfo()` defaults: routeId left null
 * so the Broker assigns one (its "client.registered" ack always carries a concrete value), a stable
 * identity so a reconnect from a fresh process gets the same RouteId back (see IdentityRegistry on
 * the Broker side), a clock offset of 0 — the client leaves it there, it is the Broker's to stamp —
 * and whatever the client wants to say about itself.
 */
export function newClientInfo({identity = defaultIdentity(), ...properties} = {}) {
    return { routeId: null, identity, clockOffsetMs: 0, properties };
}

/**
 * "{program}@{version}" as a best-effort stand-in for C#'s entry-assembly name/version — there is no
 * single JS equivalent across Node and the browser. Coarse on purpose, matching the other runtimes:
 * it identifies "this program", not "this running instance of it" — a caller running more than one
 * logical client in one process (a demo pairing a provider with its client, a test simulating
 * several) must override identity per instance, or they collide under the Broker's "newer wins" rule
 * and only the last one stays connected — each registration evicts the one before it with
 * CloseCodes.Superseded, which is terminal.
 */
function defaultIdentity() {
    const proc = globalThis.process;
    if (proc?.argv?.[1]) {
        const name = proc.argv[1].replace(/\\/g, "/").split("/").pop().replace(/\.[^.]+$/, "");
        return `${name}@${proc.env?.npm_package_version ?? "0"}`;
    }

    if (typeof globalThis.location !== "undefined") return `${globalThis.location.pathname}@0`;

    return `pid-${proc?.pid ?? newGuid()}`;
}
