import type { Logger } from "../utils/logger.js";
import type { ClientInfo } from "./events.js";
import type { Guid } from "./utils/guid.js";
import type { DestinationValue, Header } from "./message.js";
import type { Router } from "./router.js";

/**
 * The shared ack body for every channel/topic control message (channel.registered,
 * channel.resolved, topic.registered, topic.listed, topic.subscribed, topic.unsubscribed,
 * topic.resolved, published), so callers awaiting any of them check one shape. Every field but `ok`
 * is request-specific and absent for the rest.
 */
export interface BrokerAck {
    ok: boolean;
    error?: string | null;
    /** The provider's RouteId, on channel.resolved. */
    routeId?: Guid | null;
    /** The Broker-assigned topic id, on the acks that establish a topic's identity. */
    topicId?: Guid | null;
    /** The topic names, on topic.listed — and the single resolved name on topic.resolved. */
    topics?: string[] | null;
    /** The resolved topic's channel, on topic.resolved — only the handle on it learns the id. */
    channel?: string | null;
}

/** Receives one inbound frame on a topic: the decoded body and the header it arrived under. */
export type TopicHandler = (body: any, header: Header) => void;

export interface SignalOptions {
    signal?: AbortSignal;
}

export declare abstract class ChannelBase {
    protected constructor(router: Router, log: Logger);

    readonly router: Router;
    readonly log: Logger;

    /** Its own name for a provider, the resolved name for a client. */
    readonly channelName: string;

    /**
     * Whether this handle *is* the channel (a provider) or merely resolved it (a client). Declared
     * as a literal on each subclass, so a `ChannelProvider | ChannelClient` narrows on it — the
     * direction is what decides whether a send is a publish or a dispatch, and a caller holding
     * either handle should be able to ask rather than track it alongside.
     */
    readonly isProvider: boolean;

    /** topic.register for a provider, topic.subscribe for a client — the direction-agnostic seam. */
    abstract subscribe(topic: string, handler: TopicHandler, options?: SignalOptions): Promise<void>;

    learnTopicId(topic: string, topicId: Guid): void;

    addTopicHandler(topic: string, handler: TopicHandler): void;

    removeTopicHandlers(topic: string): void;

    /** The cached TopicId, or the /channel/topic path the Broker resolves and acks back. */
    destinationFor(topic: string): DestinationValue;

    sendOnTopic(
        verb: string,
        topic: string,
        body: unknown,
        replyTo: Guid | null,
        options?: SignalOptions
    ): Promise<void>;

    callOnTopic<T = unknown>(topic: string, body: unknown, options?: SignalOptions): Promise<T>;

    publishOnTopic(topic: string, body: unknown, options?: SignalOptions): Promise<void>;

    request(type: string, body: unknown, ackType: string, signal?: AbortSignal): Promise<BrokerAck>;

    requestOnTopic(
        type: string,
        to: DestinationValue,
        body: unknown,
        ackType: string,
        correlationId: Guid,
        signal?: AbortSignal
    ): Promise<BrokerAck>;
}

export declare class ChannelProvider extends ChannelBase {
    static create(
        router: Router,
        log: Logger,
        channelName: string,
        options?: SignalOptions
    ): Promise<ChannelProvider>;

    /** Also the channel's identity — there is exactly one provider per channel name. */
    readonly channelRouteId: Guid;

    readonly channel: string;

    readonly isProvider: true;

    /** Declares topic on this provider's channel and arms handler for inbound traffic on it. */
    subscribe(topic: string, handler: TopicHandler, options?: SignalOptions): Promise<void>;

    /**
     * Replies to the one client whose request this provider is handling. The correlation id comes
     * from `header` when given, and otherwise from the request in flight — which is only readable
     * from the synchronous part of a handler, so a reply sent after an await must pass it.
     */
    dispatch(topic: string, body: unknown, options?: SignalOptions & { header?: Header }): Promise<void>;

    /** Broadcasts to every subscriber; the ack confirms the Broker received it, not the subscribers. */
    publish(topic: string, body: unknown, options?: SignalOptions): Promise<void>;

    /** Unsolicited send to one client — the only provider-originated send that names a recipient. */
    notify(recipient: Guid, topic: string, body: unknown, options?: SignalOptions): Promise<void>;
}

export declare class ChannelClient extends ChannelBase {
    static create(
        router: Router,
        log: Logger,
        channelName: string,
        options?: SignalOptions
    ): Promise<ChannelClient>;

    readonly channelRouteId: Guid | null;

    readonly channel: string;

    readonly isProvider: false;

    /** Pass `replyTo: null` for a genuinely fire-and-forget send. */
    dispatch(topic: string, body: unknown, options?: SignalOptions & { replyTo?: Guid | null }): Promise<void>;

    /** Dispatches and resolves with the provider's reply. */
    call<T = unknown>(topic: string, body: unknown, options?: SignalOptions): Promise<T>;

    subscribe(topic: string, handler: TopicHandler, options?: SignalOptions): Promise<void>;

    unsubscribe(topic: string, options?: SignalOptions): Promise<void>;

    listTopics(options?: SignalOptions): Promise<string[]>;
}

export interface MessageBusOptions extends SignalOptions {
    /** Addresses a Broker directly, bypassing discovery. */
    url?: string;
    /** Which realm to discover a Broker in; defaults to MESSAGEBUS_REALM, then "Default". */
    realm?: string;
    log?: Logger;
    /** Reconnect indefinitely on any drop, re-registering and re-subscribing. Default false. */
    persistent?: boolean;
}

export declare class MessageBus {
    constructor(router: Router, log: Logger);

    readonly router: Router;
    readonly log: Logger;

    static connect(clientInfo: ClientInfo, options?: MessageBusOptions): Promise<MessageBus>;

    createChannel(channelName: string, options?: SignalOptions): Promise<ChannelProvider>;

    connectChannel(channelName: string, options?: SignalOptions): Promise<ChannelClient>;

    dispose(): void;
}
