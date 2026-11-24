import type { Emitter } from "../utils/emitter.js";
import type { Logger } from "../utils/logger.js";
import type { Connection, ConnectionStateValue } from "./connection.js";
import type { Header } from "./message.js";
import type { WireFrame } from "./utils/wire.js";

export declare class WebSocketConnection implements Connection {
    constructor(url: string, log?: Logger);

    readonly url: string;
    readonly log: Logger;
    readonly state: ConnectionStateValue;
    readonly onFrame: Emitter<[WireFrame]>;
    readonly onDisconnected: Emitter<[number | undefined]>;

    connect(options?: { signal?: AbortSignal }): Promise<void>;

    send(header: Header, body: string): Promise<void>;

    dispose(): void;
}
