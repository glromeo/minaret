/**
 * The connection-discovery contract, mirroring Runtime/Discovery/BrokerConnectionInfo.cs — currently
 * just the port the Broker listens on.
 */
export interface BrokerConnectionInfo {
    port: number;
}

/** Node only — throws in a browser, where there are no named pipes and none are needed. */
export declare function pipeName(realm: string): string;

/** Node only — the `\\.\pipe\…` path form of the same name. */
export declare function pipePath(realm: string): string;

/** Node only — reads the info the Broker publishes on its realm-scoped pipe. */
export declare function discover(
    realm?: string,
    options?: { signal?: AbortSignal }
): Promise<BrokerConnectionInfo>;

/**
 * Where to connect a WebSocket: the discovered `wss://localhost:{port}/msgbus` under Node, the
 * same-origin `/msgbus` under a browser. The realm is ignored in the browser — a page is served by
 * exactly one Broker.
 */
export declare function resolveBrokerUrl(
    options?: { realm?: string; signal?: AbortSignal }
): Promise<string>;
