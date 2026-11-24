import { Emitter } from "../utils/emitter.js";
import { consoleLogger } from "../utils/logger.js";
import { ConnectionState } from "./connection.js";
import { tryParseHeader, writeHeaderAndBody } from "./utils/wire.js";

/**
 * The real Connection, mirroring runtime's WebSocketConnection over the platform's own
 * global WebSocket — the same class in a browser and in Node, which is why this package needs no
 * transport abstraction beyond the Connection contract itself.
 *
 * Like runtime's it does not hold a single socket for life: a WebSocket can only be opened once, so
 * `connect` disposes any previous socket and constructs a fresh one, which is what makes calling it
 * again (reconnecting) actually work.
 *
 * There is no receive *loop* here. A WebSocket delivers whole messages through an event, so fragments
 * are already stitched by the time `message` fires. The one thing that does
 * carry across is the contract on a bad frame: parse failures are logged and dropped, never thrown
 * out of the callback, because a malformed frame is not connection-fatal.
 */
export class WebSocketConnection {

    #socket = null;

    /** Set by dispose(), so the close handler can tell a caller-driven end from an actual drop. */
    #disposed = false;

    constructor(url, log = consoleLogger) {
        this.url = url;
        this.log = log;
        this.onFrame = new Emitter(log, "onFrame");
        this.onDisconnected = new Emitter(log, "onDisconnected");
    }

    get state() {
        if (this.#socket === null) return ConnectionState.Closed;

        switch (this.#socket.readyState) {
            case WebSocket.CONNECTING:
                return ConnectionState.Connecting;
            case WebSocket.OPEN:
                return ConnectionState.Open;
            default:
                return ConnectionState.Closed;
        }
    }

    /**
     * Resolves once the socket is open. The `close` listener is attached before the open/error race
     * so that a failed open — which fires `error` then `close` per the WebSocket spec — still routes
     * through the same disconnect path a later drop takes, leaving exactly one place a reconnect
     * policy has to watch.
     */
    connect({ signal } = {}) {
        this.#closeCurrent();
        this.#disposed = false;

        const socket = new WebSocket(this.url);
        this.#socket = socket;

        socket.addEventListener("message", event => this.#onMessage(event));
        socket.addEventListener("close", event => this.#onClose(socket, event.code));

        return new Promise((resolve, reject) => {
            const onAbort = () => {
                socket.close();
                reject(signal.reason ?? new Error("connect aborted"));
            };

            socket.addEventListener("open", () => {
                signal?.removeEventListener("abort", onAbort);
                resolve();
            }, { once: true });

            // The event carries nothing describing the failure in either runtime, so the message is
            // all there is to say; `close` follows it and drives the reconnect.
            socket.addEventListener("error", () => {
                signal?.removeEventListener("abort", onAbort);
                reject(new Error(`WebSocket failed to open: ${this.url}`));
            }, { once: true });

            if (signal?.aborted) onAbort();
            else signal?.addEventListener("abort", onAbort, { once: true });
        });
    }

    async send(header, body) {
        if (this.#socket === null || this.#socket.readyState !== WebSocket.OPEN)
            throw new Error("Connection is not open.");

        this.#socket.send(writeHeaderAndBody(header, body));
    }

    dispose() {
        this.#disposed = true;
        this.#closeCurrent();
    }

    #closeCurrent() {
        const socket = this.#socket;
        this.#socket = null;
        socket?.close();
    }

    #onClose(socket, code) {
        // A close arriving from a socket already replaced by a reconnect says nothing about the
        // current one, so it neither reports state nor triggers another retry. This is also what
        // keeps a client's *own* superseded close — the Broker evicting the session it just
        // reconnected away from — from disarming the connection that replaced it.
        if (this.#socket !== socket) return;

        this.#socket = null;
        // The code travels because it is the only thing separating a drop worth retrying from the
        // Broker saying this client has been replaced; see CloseCodes.
        if (!this.#disposed) this.onDisconnected.emit(code);
    }

    #onMessage(event) {
        // Binary frames are not part of the protocol — the format is text precisely so browsers are
        // first-class clients — so anything else is not ours to interpret.
        if (typeof event.data !== "string") return;

        const { frame, error } = tryParseHeader(event.data);
        if (error !== null) {
            this.log.warn(`Received frame that could not be parsed as a Wire envelope: ${error}`);
            return;
        }

        this.onFrame.emit(frame);
    }

}
