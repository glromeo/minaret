# CLAUDE.md — runtime

The Java client runtime: broker discovery, `Router` (Layer 0), the channel handles (Layer 1), the
annotated surface (Layer 2) and the `M01` codec. The shared wire contract is the `wire-contract`
skill; the codec itself is the `wire-protocol` skill.

**Every type here carries its own `/** */` block, and that is where per-member detail lives.** This
file holds only what no single type can own: cross-cutting invariants, orderings two types must agree
on, and the "why not X" arguments that would otherwise be re-litigated. To know what `reconnect`
does, read `Router.java`. To know why a reconnect is not a second connect method, read on.

`broker`'s tests use this codec to build and read every frame.

## Constraints that shaped it

- **One dependency: Jackson.** `UUID`, base64url, gzip, WebSocket and named pipes are all in the JDK.
  No Netty, no Guava, no mocking framework.
- **Java 17.** Records carry the value types. Nothing here needs virtual threads.
- **No DI container.** `Channels.connect` wires a `Router` by hand. A deployment that wants a
  container already has one and should own the wiring. Logging goes through `System.Logger`, so a
  consumer can bridge it to whatever it uses without this package depending on a logging API.

## Layer 0 — Router

- **Router is a message bus, not a WebSocket wrapper.** It exposes no raw byte or string API, only
  `sendMessage(Header, Object)` and the `handleMessage()` emitter of `Message<JsonNode>`. The
  transport is a private detail behind `Connection`.
- **Router never sees a realm or a URI.** Both are resolved inside the injected
  `Supplier<Connection>` factory (`Channels.connect` builds one). Discovery running *inside* the factory
  rather than once up front is what lets a reconnect find a broker that came back on a different port.
- **`connect` is the only public entry point, and a reconnect is the same code path.**
  `reconnect(min, max, maxRetries)` is a *setter*: it stores a policy and returns. Unset, `connect`
  makes one attempt and fails. Set, `connect` *is* the retry loop, and `connectTo` arms the
  connection's disconnect handler to call it again. One loop, one arming site, with backoff and retry
  count both restarting from a successful registration. A background reconnect that exhausts
  `maxRetries` has no caller to fail, so it logs and leaves the Router down until someone calls
  `connect` again.
- **Registration is a round trip with no implicit timeout.** `connectTo` does not complete until the
  broker's `client.registered` ack arrives, so once `connect` completes, `clientInfo()` is the
  broker's *enriched* copy. Bounding the wait is the caller's choice, spelled `orTimeout`. The same
  contract holds for `on`, `callOnTopic` and every request helper.
- **`connectTo` unsubscribes before it closes.** The previous `Connection` is detached from `onFrame`
  *and then* closed, so a late frame on the stale socket can neither leak nor complete the *new*
  registration and report the wrong `clockOffsetMs`.
- **The client parses bodies eagerly; the broker does not.** The client is the body's actual consumer,
  so `handleMessage` carries a materialized `Message<JsonNode>` rather than the lazy `WireFrame`. Parse
  failures are logged and swallowed, never thrown, so garbage traffic can't break the message loop.
- **`Router` re-subscribes every event subscription on reconnect** (`subscriptions`, re-issued by
  `connectTo`). Re-registering alone would leave a reconnected client armed for events it no longer
  receives: handlers intact, stream gone.

### Events do not surface through `handleMessage`

`onFrame` routes a `{from: null, type: event | event.replay}` frame to `dispatchEvent`, which reads the
body's `name` and invokes the handlers keyed on it. A `handleMessage` subscriber would otherwise have
to re-parse every event body to tell one event from another. The name is in the *body*, so dispatch
necessarily parses before it can route. That extra parse is what "name in the body" costs, and it
buys the "`type` is the verb" split.

`Events.Origin` is why the handler signature is `BiConsumer<Event, Origin>`: an `event.replay` frame
carries the whole subscribe-time snapshot and is fanned out to the handler one occurrence at a time,
so the handler's contract is one occurrence at a time regardless of framing.

## Request/reply correlation

Three correlators, one shape: `ChannelBase.awaitAck` (every control-plane round trip on a handle),
`ChannelBase.callOnTopic` (data-plane request/reply) and `Router.sendSubscribe` (`on`'s ack). Each
arms a `handleMessage` handler, sends, completes on the first matching frame and removes the handler
however it ends. Collapsing them into one predicate-taking `awaitReply` is in `TODO.md`; a fourth copy
belongs in that primitive instead.

- **They match on a predicate, not a bare correlation id**, because the callers filter on different
  things: an ack on `type` + `replyTo`, a dispatch reply on those *plus origin*. A frame a correlator
  rejects is left to the other `handleMessage` subscribers, so none consumes what it didn't ask for.
- **Correlating on `replyTo` is load-bearing.** `handleMessage` is multicast, so two concurrent
  requests of the same ack type on one connection see each other's acks. Without the id they would
  complete each other's future.
- **`callOnTopic` ignores frames whose `from` is its own RouteId.** A request carries the same verb
  *and* the same correlation id as the answer it awaits, so origin is the only thing separating them.
  Anything looping a send back inbound (a shared transport, a provider subscribed to its own topic)
  would otherwise complete the call with its own argument.

## Layer 1 — the channel handles

- **Handlers are keyed by TopicId, not by `Header.type`.** `type` is the verb and is identical for
  every topic. Callers still work in *names*: `topicIds` maps name → id as the broker reveals it, and
  `pendingByName` parks handlers registered before their id is known. That matters because `subscribe`
  deliberately arms its handler *before* sending its request.
- **The handle is the channel context, and it travels.** `destinationFor` answers the cached id or the
  `/channel/topic` self-resolving path. The channel half comes from `channelName()`, which each
  subclass answers with the channel it claimed or resolved. The sender always knows its channel, so it
  sends it rather than making the broker guess. Id-vs-name addressing stays internal: every public
  signature takes a name.
- **`subscribe` is abstract on `ChannelBase`** and means "register this handler the way *this* handle
  does it": `topic.register` on a provider, `topic.subscribe` on a client. The two share a signature
  exactly, which is what lets `bind` stay direction-agnostic.
- **Handler-arming order is a race fix on both ends, and the two are mirrors.** `subscribe` arms
  *before* the request, because the broker marks the client eligible for fanout before it acks, so a
  publish can land while the ack is in flight. `unsubscribe` therefore removes *after* the ack, so a
  publish already in flight when the broker unlinks is still handled rather than dropped as an
  unrecognised TopicId. A failed unsubscribe ack leaves handlers in place: nothing was unsubscribed,
  so nothing should stop being delivered. `on` arms before its request for the same reason.
- **`IN_FLIGHT_REQUEST` is a `ThreadLocal`**, valid only for the *synchronous* part of a handler. It
  is what makes a reply sent from inside a handler inherit that request's `replyTo` with nothing
  threaded through by hand. A handler replying after an async hop must pass the header it received
  (`ChannelProvider.dispatch(topic, body, request)`), which the binding layer does anyway.
- **`ChannelProvider.dispatch` takes no recipient.** A reply is addressed to the *topic* and narrowed
  to one connection by `replyTo`, so a recipient RouteId would have nothing to route.

## Layer 2 — the annotated surface

`proxy(Class<T>)` builds a `java.lang.reflect.Proxy` over a `@Channel` interface (`ChannelProxy`), and
`bind(handlers)` registers each `@Topic` method (`TopicBinding`). Both live on `ChannelBase`, so both
exist on **both** handles: `proxy` is always "what I send", `bind` always "what I handle".

- **The annotations carry a name and nothing else.** Direction comes from the *handle*, delivery from
  the *signature*, so one annotation pair covers all four flows with nothing stated twice and nothing
  that can disagree with the call site:

  | Handle   | Signature                        | Kind    |
  |----------|----------------------------------|---------|
  | client   | `CompletableFuture<T> m(args…)`  | Request |
  | client   | `void m(args…)`                  | Send    |
  | provider | `void m(UUID to, args…)`         | Notify  |
  | provider | `void m(args…)`                  | Publish |

- **`TopicCall.plan` plans the whole contract up front**, when the proxy is built, so an ill-formed
  interface throws from `proxy(...)` rather than on whichever later call first reaches the bad
  method. Two shapes are rejected: a recipient on a client (a client may only address its provider),
  and a `CompletableFuture<T>` on a provider. The broker reads every provider frame on its own topic
  as a reply, so a provider-originated *request* has no wire representation and would hang forever.
- **Argument packing and `TopicBinding`'s unpacking must agree.** A leading recipient is stripped. One
  remaining argument is the body itself; several pack positionally as a JSON array. Wrapping the
  single case in a one-element array would make every handler unwrap a shape that exists only for the
  rarer one.
- **A leading `UUID` on a *bound* method receives the sender's RouteId**, mirroring the outbound
  "leading UUID means recipient" rule. That lets a handler key per-client state without reaching for
  the header.
- **A bound reply takes its correlation id from the received `Header` explicitly**, not from
  `IN_FLIGHT_REQUEST`, so its correctness does not depend on where the handler happens to go async.
- `Channels.createChannel(handlers)` / `connectChannel(Class)` are pure sugar over the handle methods,
  which is why the handle methods are what tests drive: a handle is constructible against a
  `FakeConnection`, while `Channels.connect` runs real discovery.

## Transport

- **Frames dispatch on a single-thread executor.** Wire order is load-bearing (a `topic.resolved` must
  be learned before the frame it resolves is dispatched, and a replay must reach handlers before the
  ack that says the picture is complete), and the JDK's WebSocket makes no promise about which thread
  invokes a listener. Handlers are *invoked* from that thread, but their futures are never joined on
  it, or a handler issuing a request would deadlock the queue against its own reply.
- **The transport stitches text fragments itself.** `java.net.http.WebSocket` delivers `CharSequence`
  with an `isLast` flag. There is no relay path and no buffer pooling, because a client is always its
  body's consumer. The exception is `wire/Gzip`, which pools Deflaters/Inflaters: built per message,
  each one goes through the JDK Cleaner's single global lock, and 2000 Routers in one process
  serialized on it.
- **Sends are chained through one future.** `WebSocket.sendText` must not be called again before the
  previous send completes. Without the chain, two threads publishing at once is an
  `IllegalStateException` from the JDK.
- **A `WebSocketConnection` does not hold one socket for life.** Each connect builds a fresh socket,
  which is what makes reconnecting work.
- **`Emitter` iterates a copy-on-write snapshot**, because every correlator removes its own handler
  from inside the emit that resolved it.
- **`WebSocketConnection.trustingLocalhostClient()` disables certificate validation** so a local dev
  certificate is reachable. It is a development affordance and is named like one. A deployment with a
  real certificate passes a plain `HttpClient` through `Channels.connect(URI, ClientInfo)`.

## Discovery

**Windows-only, and it says so.** The broker publishes on a named pipe, which the JDK opens as an
ordinary file. No JNA, no Unix-socket fallback: elsewhere `discover` fails and the caller passes an
explicit broker URI to `Channels.connect(URI, ClientInfo)`.

**The pipe must be opened `"r"`, not `"rw"`.** The broker creates it outbound-only, so asking for
write access fails with `FileNotFoundException: … (Access is denied)`. That reads like the broker is
missing when it is running and the pipe is right there.

## Identity

**`ClientInfo.identity` is mandatory, and its default is coarse on purpose**: `"{main class}@{version}"`
(read from `sun.java.command` and this package's manifest, falling back to `pid-{pid}`) identifies
*this program*, not this instance. Any process hosting more than one logical client (a demo pairing a
provider with its client, a test simulating several) **must** set it per instance, or they collide in
the broker's `IdentityRegistry`, where "newer wins" leaves only the last one connected. Two processes
with the same main class collide too, which is why each example side sets its own identity.

`create()` leaves `routeId` null. The broker assigns one (a time-ordered v7) on first contact per
identity and hands the same one back on every later reconnect, so nothing here mints a UUID.

**A superseded close is the one drop `reconnect` does not answer.** The broker evicts the prior holder
of an identity with `CloseCodes.SUPERSEDED`, and `connectTo`'s disconnect handler *clears* the policy
on it rather than skipping one attempt. Both halves matter. Retrying would take the RouteId straight
back and evict the connection that replaced it, which would retry in turn: the pair would trade the
identity at socket speed, every round emitting presence events. Clearing rather than skipping means a
*later* connection cannot resume retrying for an identity this Router no longer holds. An explicit
`connect` still works: what is disarmed is the automatic retry, not the Router.

## Kept byte-identical to the other implementations

- **Frame bytes.** `bodyLength` is stamped even though nothing here reads it (a WebSocket already
  delivers whole messages), so a frame from this client is byte-identical to the others'.
- **Guid encoding.** base64url of the 16 raw bytes in RFC-4122 **big-endian** order, which is already
  `getMostSignificantBits`/`getLeastSignificantBits` order. Pinned by
  `00112233-4455-6677-8899-aabbccddeeff` → `ABEiM0RVZneImaq7zN3u_w`.
- **The compression threshold is on the *input* size**, never the compressed output. Deflate
  implementations do not produce identical bytes, and they don't have to. An output-size comparison is
  what would make two of them frame the same message differently.
- **Text WebSocket frames.** The whole bus is UTF-8 text with a `\0` body separator. Binary payloads
  ride as base64 inside a JSON string, not as binary frames.

`WireTest` asserts whole frames with string equality, never a prefix match.

## Tests

`FakeConnection` is a hand-rolled fake (no mocking library anywhere). It acks registration
asynchronously, never inline on the send call stack, as a real broker's ack arrives after the send
completes. Acking re-entrantly would hide the ack-racing-registration ordering `Router` has to get
right. It has no routing table: a test needing a frame somewhere raises it explicitly.
