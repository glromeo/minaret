import type { Emitter } from "../utils/emitter.js";
import type { Logger } from "../utils/logger.js";
import type { Connection, ConnectionFactory, ConnectionStateValue } from "./connection.js";
import type { ClientInfo, EventHandler, EventSubscription } from "./events.js";
import type { Guid } from "./utils/guid.js";
import type { Header, Message } from "./message.js";

export interface RouterOptions {
    log?: Logger;
    /** Reconnect backoff floor; doubles from here on every failed attempt. Default 500ms. */
    initialRetryDelayMs?: number;
    /** Reconnect backoff ceiling. Default 30s. */
    maxRetryDelayMs?: number;
}

export declare class Router {
    constructor(connectionFactory: ConnectionFactory, options?: RouterOptions);

    readonly log: Logger;
    readonly initialRetryDelayMs: number;
    readonly maxRetryDelayMs: number;

    /** Raised for every inbound message that parses as an envelope; events never surface here. */
    readonly handleMessage: Emitter<[Message]>;

    /** connecting -> open -> closed, and again on every reconnect. "open" means *registered*. */
    readonly onStateChange: Emitter<[ConnectionStateValue]>;

    readonly routeId: Guid | null;
    readonly connection: Connection | null;

    /** Arms a retry policy `connect` runs under — see Router.reconnect in the source for the contract. */
    reconnect(minDelayMs?: number, maxDelayMs?: number, maxRetries?: number): void;

    connect(clientInfo: ClientInfo, options?: { signal?: AbortSignal }): Promise<ClientInfo>;

    connectTo(
        clientInfo: ClientInfo,
        connection: Connection,
        options?: { signal?: AbortSignal }
    ): Promise<ClientInfo>;

    on(
        subscription: EventSubscription | string,
        handler: EventHandler,
        options?: { signal?: AbortSignal }
    ): Promise<void>;

    sendMessage(header: Header, body: unknown, options?: { signal?: AbortSignal }): Promise<void>;
}

export declare function newClientInfo(properties?: Record<string, string>): ClientInfo;
