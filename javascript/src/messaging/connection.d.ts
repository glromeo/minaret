import type { Emitter } from "../utils/emitter.js";
import type { Header } from "./message.js";
import type { WireFrame } from "./utils/wire.js";

export declare const ConnectionState: {
    readonly Connecting: "connecting";
    readonly Open: "open";
    readonly Closed: "closed";
};

export type ConnectionStateValue = (typeof ConnectionState)[keyof typeof ConnectionState];

export declare const CloseCodes: {
    /** Replaced by a newer registration under the same identity. Terminal: no reconnect. */
    readonly Superseded: 4001;
};

export interface Connection {
    readonly state: ConnectionStateValue;

    /** Raised for every inbound message that parses as a Wire header; body still encoded. */
    readonly onFrame: Emitter<[WireFrame]>;

    /**
     * Raised when the connection ends on its own — never for a caller-driven dispose(). Carries the
     * peer's close code, which Router reads only to spot `CloseCodes.Superseded`.
     */
    readonly onDisconnected: Emitter<[number | undefined]>;

    connect(options?: { signal?: AbortSignal }): Promise<void>;

    /** header.encoding must already describe body; the connection never re-encodes it. */
    send(header: Header, body: string): Promise<void>;

    dispose(): void;
}

/**
 * How Router obtains a Connection. A factory rather than an instance because Router is constructed
 * once but may connect several times — every reconnect needs a socket that has never been opened.
 */
export type ConnectionFactory = () => Connection | Promise<Connection>;
