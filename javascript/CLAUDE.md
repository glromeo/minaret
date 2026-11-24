# CLAUDE.md — javascript

`@messagebus/client`, the JavaScript client runtime for Node and the browser: the counterpart of the
Java `runtime` module. Read `runtime/CLAUDE.md` first: the design rationale for Router, ChannelBase, the
handles and the transport is written there and is **not** repeated here. The shared wire contract is
the `wire-contract` skill; the codec is the `wire-protocol` skill. This file covers only what is local
to this package.

`README.md` has the module-for-module map to `runtime/` and the usage surface. The package was first
ported from the C# `Runtime`, and its module doc blocks still name the `.cs` file each mirrors. Read
those as pointing at the same-named Java type in `runtime/`.

## Constraints that shaped it

- **No build step.** The package ships its source. Types are hand-written `.d.ts` siblings, resolved
  automatically because they sit next to the `.js` they describe. So: no decorators, no TypeScript
  syntax, no bundling, nothing to keep in sync but the `.d.ts` itself.
- **One implementation for Node and the browser.** Both supply `WebSocket`, `crypto`, `TextEncoder`,
  `CompressionStream` and `Response`, so there is no per-environment transport. The *only* split is
  discovery, and it splits because the two environments ask different questions, not because one
  primitive is missing (see below).
- **Zero dependencies.** What the JDK gives the Java side and JavaScript does not (`UUID`, base64url,
  `CompletableFuture`) is a small module here rather than a package.

## Kept identical to the other runtimes, deliberately

These are the things a reader might assume are free to differ, and they are not:

- **Frame bytes.** `bodyLength` is stamped even though nothing here reads it (the WebSocket already
  delivers whole messages), so a frame from this client is byte-identical to the Java one's for the
  same message. That is what makes the two comparable at all.
- **Guid encoding.** base64url of the 16 raw bytes in RFC-4122 **big-endian** order, pinned by the
  same literal the Java `WireTest`s pin: `00112233-4455-6677-8899-aabbccddeeff` → `ABEiM0RVZneImaq7zN3u_w`.
- **The compression threshold is on the *input* size**, never the compressed output. Two deflate
  implementations that disagree on output size would otherwise frame the same message differently.
- **Handler-arming order.** `subscribe` arms before sending, `unsubscribe` removes after the ack, `on`
  arms before the request. Each ordering exists for a race documented in `runtime/CLAUDE.md`.
- **Reconnect shape.** `router.reconnect(minDelayMs, maxDelayMs, maxRetries)` arms a policy and a single
  `connect` runs under it, as in Java. `MessageBus.connect`'s `persistent` option calls
  `router.reconnect()` before `router.connect(...)`. Event subscriptions are re-issued on reconnect, and
  `Router.onStateChange` reports open-after-registration, both as in Java.

## Where this port differs, and why

Each is a place the two runtimes are *not* line-for-line, so a change on one side needs a thought
rather than a transcription.

- **The body half of the codec is async; the header half is not.** gzip goes through
  `DecompressionStream`, the only inflate available without a dependency, and it is stream-shaped.
  Header parsing stays synchronous, which is the half that carries the broker's read-order rationale.
  - **Consequence: `Router` handles inbound frames through a FIFO promise chain**, the counterpart of
    Java's single-thread dispatch executor. Wire order is load-bearing (a `topic.resolved` must be
    learned before the frame it resolves is dispatched, a replay must reach handlers before the ack
    that says the picture is complete), and a mixed sync/async path would let a small raw frame
    overtake a large compressed one that arrived first. Every frame is queued, not just compressed
    ones, for that reason. User handlers are *invoked* from the chain but never awaited, or a handler
    awaiting a request of its own would deadlock the queue against its own reply.
  - An earlier copy of this codec (in monitoring's `src/wire/`) rejected gzip bodies outright. That
    was a live bug, not a simplification: the broker gzips at 1024 bytes, and an `event.replay` on a
    broker with a few dozen clients clears that easily.
- **`AbortSignal` is the cancellation handle**, with the same contract as Java's `orTimeout`: *no*
  signal means no implicit timeout, and bounding a registration or a request is the caller's choice.
- **The in-flight request is a plain variable**, valid only for the *synchronous* part of a handler,
  like Java's `ThreadLocal` (`AsyncLocalStorage` would be Node-only). A handler replying after an
  `await` must pass the header it received.
- **Discovery splits by environment, via the `browser` export condition.** A browser has no named pipes
  and needs none, because the broker serves the page: the socket is same-origin by construction.
  Discovery is the question only a standalone process asks. Everything importing it does so through
  the package's own name (`@messagebus/client/discovery`) rather than a relative path, which is what
  makes the condition apply; a relative import would bypass it and drag `node:net` into a browser
  bundle.
- **`newClientInfo`'s `identity` default is sourced per environment**: `process.argv[1]`'s basename and
  `npm_package_version` under Node, `location.pathname` in a browser, falling back to `pid-{pid}` (or a
  fresh Guid with no `process` at all). Same "identifies the program, not the instance" contract as
  `runtime/CLAUDE.md` "Identity". `routeId` is left `null`: the broker assigns one per identity.
- **The control-plane bodies are object literals, not declared types.** Nothing is shared across the
  language boundary except the JSON, so the shapes live in `channels.d.ts` and the call sites write
  `{ channelName }`.

## Not ported

- **Layer 2's annotated surface** (Java's `@Channel`/`@Topic`, `ChannelProxy`, `TopicBinding`). The
  JavaScript equivalents are decorators and metadata, which need the transpile step this package exists
  to avoid. A descriptor-object surface (`proxy(spec)` / `bind(handlers, spec)`) would express the same
  four call kinds without decorators, but it is a design decision rather than a transcription, so it is
  left open.
- **The JSON-IDL half of Layer 2**, which is design-only everywhere.
