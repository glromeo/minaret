import { connect } from "node:net";
import { parse } from "../utils/json.js";
import { DEFAULT_REALM, resolveRealm } from "./realm.js";

/**
 * Broker discovery over the realm-scoped named pipe the Broker publishes on, mirroring
 * runtime's BrokerDiscovery. This is the **Node** half of the module; the browser gets
 * broker-discovery.browser.js instead, selected by the `browser` export condition — which is why
 * everything importing this does so through the package's own name rather than a relative path, so
 * the condition actually applies.
 *
 * The split is not an implementation detail of two ways to do one thing: a browser has no named
 * pipes and needs none, because the Broker serves the page — the socket is same-origin by
 * construction. Discovery is the question only a standalone process has to ask.
 */
const PIPE_NAME_PREFIX = "MessageBus.Broker.ConnectionInfo";

/**
 * The realm-scoped pipe name, uppercased exactly as the Broker uppercases it — the two must agree
 * character for character or a client silently fails to find a Broker that is running.
 */
export function pipeName(realm) {
    return `${PIPE_NAME_PREFIX}.${realm.toUpperCase()}`;
}

/**
 * The Windows path form of that pipe. Node addresses a named pipe as a filesystem-shaped path.
 */
export function pipePath(realm) {
    return `\\\\.\\pipe\\${pipeName(realm)}`;
}

/**
 * Connects to the pipe and reads the BrokerConnectionInfo the Broker publishes on it — currently
 * just the port it listens on. The Broker writes the whole document and closes, so the read ends at
 * `end` rather than at any framing of its own.
 */
export function discover(realm = resolveRealm(), { signal } = {}) {
    return new Promise((resolve, reject) => {
        const socket = connect({ path: pipePath(realm) });
        let json = "";

        const fail = error => {
            socket.destroy();
            reject(error);
        };

        signal?.addEventListener("abort", () => fail(signal.reason ?? new Error("discovery aborted")), { once: true });

        socket.setEncoding("utf8");
        socket.on("data", chunk => (json += chunk));
        socket.on("error", error => fail(new Error(`Could not reach a Broker in realm '${realm}': ${error.message}`, { cause: error })));
        socket.on("end", () => {
            const info = parse(json);
            if (info === undefined || typeof info.port !== "number")
                fail(new Error(`Broker in realm '${realm}' published unreadable connection info: ${json}`));
            else resolve(info);
        });
    });
}

/**
 * Where to connect a WebSocket for this realm. The Broker listens on localhost over TLS, so a
 * discovered port is all that is needed to build the URL — the same one runtime's Channels.connect
 * builds.
 */
export async function resolveBrokerUrl({ realm = resolveRealm() ?? DEFAULT_REALM, signal } = {}) {
    const info = await discover(realm, { signal });

    return `wss://localhost:${info.port}/msgbus`;
}
