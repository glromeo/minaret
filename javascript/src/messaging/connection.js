/**
 * The transport contract Router depends on instead of a WebSocket directly, so Router can be driven
 * against a fake. Mirrors runtime's Connection and ConnectionState, merged into one module for the
 * one reason a merge is justified here: an interface has no runtime existence in JavaScript, so
 * Connection's whole content is the type declaration next door and only
 * ConnectionState survives into the emitted code.
 *
 * A Connection exposes:
 * - `state` — Connecting/Open/Closed, coarser than the WebSocket's own readyState.
 * - `onFrame` — an Emitter raised for every inbound message that parses as a Wire header. The frame
 *   carries the parsed header and the body *still encoded*, so a subscriber decodes only what it
 *   actually needs and never pays for a body it drops.
 * - `onDisconnected` — an Emitter raised when the connection ends on its own, as opposed to a
 *   caller-driven `dispose()`. This is the one signal a reconnect policy needs and the transport
 *   doesn't otherwise expose: `state` is pull-only, so without it nothing would notice a drop
 *   happening between sends. It carries the close code the peer sent, which Router reads for one
 *   decision only — see `CloseCodes.Superseded`.
 * - `connect(options)` / `send(header, body)` / `dispose()`.
 *
 * `send`'s header.encoding must already describe body as it is — the connection never re-encodes
 * what it is given, only renders the header around it. Wire.prepareBody is what produces the pair.
 */
export const ConnectionState = Object.freeze({
    Connecting: "connecting",
    Open: "open",
    Closed: "closed"
});

/**
 * WebSocket close codes the Broker uses to say why it closed a connection. Mirrors
 * runtime's CloseCodes.
 *
 * `Superseded` (4001) means another connection registered under this client's identity and took its
 * RouteId. It is **terminal**: `Router` disarms its reconnect policy rather than retrying, because a
 * reconnect would claim the identity straight back and evict the connection that replaced it, which
 * would reconnect in turn — the two trading it at socket speed, every round emitting presence events.
 */
export const CloseCodes = Object.freeze({
    Superseded: 4001
});
