# CLAUDE.md — broker

The broker: Micronaut 4 + raw Netty on JDK 17, serving `/msgbus` (WSS), `/idl` (REST) and static
`wwwroot` on port 5443. The shared wire contract (`Header`, `Message`, `Destination`, `Verbs`) is the
`wire-contract` skill; the `M01` codec is the `wire-protocol` skill.

**Every type here carries its own `/** */` block, and that is where per-member detail lives.** This
file holds only what no single type can own: invariants two types must agree on, orderings that are
load-bearing, and arguments that would otherwise be re-litigated.

It is wire-identical to the C# Broker it was ported from (still in the old MessageBus repo), on the
same port, realm and discovery pipe, so a C# or JS client connects unchanged. Several doc blocks still
describe a type as "the port of C#'s X"; read that as "same design, differences listed here".

## Constraints that shaped it

- **No allocation per relayed frame.** Frames are pooled direct `ByteBuf`s from socket to socket:
  - the header is parsed in place by `wire/Frame` (a flyweight, one per session, ids into mutable
    `Uuid` probes);
  - verbs are compared as pre-encoded bytes;
  - `wire/FrameWriter` renders the re-stamped header and body into one buffer.

  `routing/AllocationTest` measures forward, dispatch, request/reply and 8-way fanout at
  **0 bytes/frame** and fails past 16. Serde (`micronaut-serde-jackson`, compile-time `@Serdeable`)
  runs only for control bodies, acks and events.
- **JDK 17** (no FFM, no virtual threads). Off-heap is Netty's pooled allocator. The one native call,
  creating the named pipe, goes through JNA (`discovery/ConnectionInfoPublisher`): a server must
  *create* the pipe, which the JDK cannot. `runtime`'s "no JNA" rule is about *opening* one.

## The pipeline

**`/msgbus` bypasses Micronaut's HTTP stack.** `routing/MsgbusUpgrade` sits after the HTTP codec. On
an upgrade it strips Micronaut's handlers and installs, in order:
- `IdleStateHandler`;
- `WebSocketServerProtocolHandler` (UTF-8 validation and extensions off);
- `WebSocketFrameAggregator`;
- `Session`.

Two traps:
- **`setAutoRead(true)` after the rewire.** Micronaut's HTTP/1 pipeline runs autoRead off and pulls
  each request from a handler that was just removed. Without it the handshake completes and nothing
  is read again. Only the e2e tests catch this; `EmbeddedChannel` never reads.
- **`Session.handlerAdded` pins `PooledByteBufAllocator`.** Netty 4.2's default
  `AdaptivePoolingAllocator` copies buffer-to-buffer through `nioBuffer()`, allocating per copy. That
  is the relay's body copy.

**One buffer per outbound frame, with the WebSocket header back-filled.** `FrameWriter` reserves 10
bytes, writes the M01 frame, then writes the unmasked `0x81` + length header in front and moves
`readerIndex` back. The pipeline's encoder is bypassed: the ring writes finished socket bytes. The
body is copied once. Fanout renders once and hands each subscriber a `retainedDuplicate()`.

**The keepalive** pings after 30s of reader idle and closes on the *second* idle event, so after 60s
of silence. (The C# Broker closes after 40s.)

## The session

`routing/Session` is one accepted connection: the receive path, its `SendRing`, the per-session
state, every verb handler and the teardown. `Bus` holds what every session shares, and
`MsgbusUpgrade` builds one `Session` per connection from it.

- **`dispatch()` switches on `Header.type` as a verb.** An empty `to` means the frame is addressed to
  the broker and goes to a control-plane handler. Otherwise the verb decides which table `to` is
  looked up in: `dispatch`/`publish`/`notify` read it as a **TopicId**, anything else as a recipient
  **RouteId** (`forwardToRoute`). This split keeps topic addressing and control-message naming from
  colliding: `type` answers only "what should the recipient do?", never "which topic?".
- **The body is opaque on every path but the register handshake.** It is relayed without being
  decoded to text, so a compressed body stays compressed for the recipient to inflate.
- **Frames are handled inline on the event loop**, with no task per frame. The one asynchronous verb,
  `channel.resolve`, has no broker-side timeout by design, so it completes a `CompletableFuture` that
  hops back with `ctx.executor().execute` instead of blocking. That keeps all session state confined
  to one thread (plain fields, only `clientInfo` is `volatile` since `ConnectionRegistry.registered`
  reads it from other sessions), and a connection's frames are handled strictly in arrival order.
  Teardown cancels parked resolves (`pendingResolves`) rather than waiting on them: waiting for a
  resolve with no provider would move the hang to disconnect.

### What is per-connection lives on the connection

`providedChannels` (name → `Channel`, what this session **serves**) and `resolvedChannels`
(name → provider RouteId, what it **consumes**) both live on the session: no registry-wide lock, and
they die with the connection with no disconnect sweep to remember.

**They are near-inverses and cannot be merged.** Every `Channel` in the first has
`providerRouteId ==` this session's own RouteId (which is why `resolveChannel` tests membership and
then answers the sender's id). Every RouteId in the second belongs to someone else. Teardown feeds
`providedChannels` straight to `removeChannel`, whose guard is reference identity, so a resolved
channel sitting in it would let a departing *client* free a live provider's name claim.

`providedChannels` is a **cache of this session's own registrations, not a second authority**:
`resolveChannel` falls back to `ChannelRegistry.provides`, so a channel registered outside this
session still resolves.

### Registration ordering (read before reordering anything in `handleRegister`)

The ack precedes `connections.register`, and **nothing between them may yield.** Registration runs
inline on the event loop, and that is what guarantees it lands before the teardown can run. Anything
that let it land *after* teardown would strand a route pointing at a dead socket for good, with no
later sweep to catch it. The ack itself cannot fail the handshake: it only enqueues, and a dead peer
surfaces in the ring. What keeps a dead socket out of the registry is that **teardown unregisters on
every exit path**.

`SessionTest` drives this as a unit test over `TestBus`: teardown after a connection drops
mid-handshake, which no real client can be made to do on cue.

## Identity

`IdentityRegistry` maps `ClientInfo.identity → RouteId`. It is what verifies that a client
re-presenting a RouteId is the same client that held it. Every registration goes through
`resolveOrClaim(identity, proposed)`, one `computeIfAbsent`. A known identity's remembered RouteId
wins over whatever the client proposed. An unknown one trusts the proposal, or mints a fresh
`UuidV7` if there wasn't one.

Each branch buys something. Minting when nothing was proposed is what lets the RouteId be fully
broker-assigned by default. Trusting the proposal is what lets a client's cached RouteId re-establish
continuity after a **broker restart**, when the broker's own table was wiped with every other
in-memory registry. (The layer0 example relies on it to register under a well-known id.)

- **Entries are never removed on disconnect.** Unlike `ConnectionRegistry`, an identity's claim must
  outlive the connection that made it, or a later reconnect would have nothing to resolve against.
  Its lifetime is the broker process.
- **Collisions are "newer wins":** `handleRegister` asks the prior session to close
  (`requestClose`), and its own teardown then runs as usual. This is why every process hosting more
  than one logical client must give each a distinct identity (see `runtime/CLAUDE.md`).
- **The two sessions overlap, and the superseded one must not tear down tables it shares.** The
  replacement is registered and serving from the moment `handleRegister` returns. The evicted socket
  tears down whenever it notices, which is however long a close takes. Both hold the **same RouteId**
  (that is the point of the identity rule), so every table addressed by RouteId alone cannot tell them
  apart: the subscriber lists, the event table, the reply table. `connections.unregister` reports
  whether this session was still the registered one, and teardown gates all three on that answer,
  plus the `client.disconnected` emission, which would otherwise announce a departure *after* the
  replacement's `client.connected`. Only `removeChannel` runs unconditionally: it carries its own
  reference guard, and a superseded session's channels are still its own to free. Pinned by
  `SessionTest.aTornDownSupersededSession_leavesTheReplacementsSubscriptionsIntact`.
- **That close carries `CloseCodes.SUPERSEDED` (4001), and the code is what makes the eviction
  settle.** Every client runtime reconnects on a drop, so a generic close would have the evicted client
  re-register, take the RouteId back, and evict its replacement, which would reconnect in turn. The two
  would trade the identity at socket speed, and since each registration and teardown emits presence
  events, an operator would see an event storm rather than either client failing. The code is the only
  thing distinguishing "you dropped, come back" from "you were replaced", so it is a **transport close
  code rather than a control frame**: the message is that no further frames are coming. Pinned end to
  end by `MessageRoutingTest.register_underAnIdentityAlreadyHeld_closesThePriorConnection_asSuperseded`.

## Topic routing

`dispatch`, `publish` and `notify` all read `Header.to` as a TopicId (or a `/channel/topic` path)
through `resolveDestination`, so none carries a recipient RouteId: **the socket is the addressing.**

**Path resolution is one exact lookup, never a search.** `resolveChannel` turns the channel half into
a provider RouteId: the sender's own if it provides that channel, otherwise the RouteId it pinned at
`channel.resolve` time. The broker-wide channel table is deliberately **not** consulted, so a
replacement provider can never inherit frames aimed at the one a client actually resolved. A miss
drops the frame, which the sender can observe and re-resolve.

- **`dispatch` decides direction by WHO SENT IT, never by a table lookup.** A provider is subscribed
  to its own topic (that is how it serves it), so resolving the correlation id first and deciding
  afterwards would route a provider's reply back to the provider: an infinite echo loop. Pinned by
  `RoutingTest.dispatch_providerReply_goesToTheAwaitingClient_notBackToTheProvider`.

  The consequence is a real limit, not an oversight: **a provider cannot send a *request* on its own
  topic.** Every frame it sends there is read as a reply. That is correct for the one-provider-per-
  channel model, and is the thing to revisit if provider→client `DispatchMode.Any`/`All` lands
  (`TODO.md`): direction would then have to come from the verb.
- **A correlation id is claimed, not assigned, so the claim must be exclusive.** `replyTo` is an id the
  *client* picks, the reply table is broker-wide, and a reply carries nothing but that id. An
  unconditional write would let any client redirect another's pending answer to itself by reusing an
  id it had seen, and a provider sees every id it is asked to echo. `expectReply` (`ReplyTable.claim`)
  answers whether the caller got the claim, and `handleDispatch` drops the frame when it didn't. A
  re-claim by the same route is idempotent, so a retry is unaffected.
- **`notify` is the case `dispatch` structurally cannot carry**, which is why it is a verb rather than
  a fallback. Reading a correlation-id miss as "then it must be a RouteId" would make the broker guess
  between two meanings of one field, and would turn a late or duplicate reply (today a logged "no
  awaiting request" drop, a real detector for that bug class) into a send to whatever route the stale
  id names.
- **The resolved-name ack precedes the forward, on all three paths** (`ackResolvedName`). The sender
  must hold the TopicId before any frame carrying it can come back, or it drops that frame as an
  unrecognised id. Publish is the sharpest case: a provider may subscribe to its own topic.

## The tables

**The per-frame tables are `wire/UuidTable`: open addressing over raw `(msb, lsb)` longs**, so a lookup
with a parsed probe `Uuid` follows no node or key object and calls no `equals`.
- `ConnectionRegistry`'s routes and `ChannelRegistry`'s topics are copy-on-write behind a
  `volatile`, replaced under a lock on the rare write and read lock-free.
- The reply table is `ReplyTable`, 64 lock-striped tables, since claim and take run on different
  event loops. A request/reply round trip allocates nothing.

**A RouteId's `Route` is permanent and holds its current `Session`.** RouteIds are stable per
identity, so routes are never removed and a reference to one never goes stale. Each
`SubscriptionNode` holds its subscriber's `Route`, and fanout reads `node.route.session` instead of
hashing per subscriber. The subscriber's own node list hangs off the `Route` too.

## ChannelRegistry

- **A channel is addressed by (RouteId, name) everywhere, never by RouteId alone.** One connection is
  one session, but nothing limits a session to one channel name, and a second `channel.register` from
  the same connection must *add* a channel rather than evict the first.
- **Every table is written only under the registry lock**, which orders the intrusive list splices.
  Tables read lock-free on the frame path are copy-on-write (above); the rest are plain and touched
  only under the lock.
- **`getOrAddTopic` stays inside the lock *and* re-checks that its `Channel` is still registered.**
  `subscribe` and `registerTopic` look the provider's channel up lock-free first, and a
  `removeChannel` can land between that read and the lock, handing a stale `Channel` to the locked
  section.
  - **The lock** orders the subscribe after the remove. A remove that runs after the subscribe sees
    the new topic on the channel and cleans it up.
  - **The re-check** covers the stale case: it returns null when the name no longer maps to this
    instance. Without it the topic lands on a channel nothing will clean up: an id that resolves
    forever to a provider that is gone.

  An earlier version of this note argued the re-check was unnecessary, which was wrong.
  `ChannelRegistryTest.subscribe_racingProviderDisconnect_leavesNoTopicBehind` asserts that no topic
  outlives its channel. Without the re-check it finds about 170 orphans in 200 rounds.
- **Subscribers are an intrusive doubly-linked list, and each node is in *two* lists**: its `Topic`'s
  (walked by fanout) and its subscriber's (walked by disconnect). Holding both is the point: without
  the owner link, dropping a connection would scan every topic in the broker.
- **`unlink` clears the removed node's `prev` but deliberately leaves its `next`.** This is the one
  non-obvious invariant in the file. The walk only goes forward, so a walker parked on the removed node
  still needs `next` to find its way on. Nulling it would strand the walk and silently truncate a
  fanout mid-list. `prev` has no reader outside `unlink`, and keeping it would retain an arbitrarily
  long tail of unlinked predecessors. `unlinkFromOwner` *does* clear the owner link, because no
  lock-free walker follows the owner list.
- **The lock-free links are `volatile`** (`Topic.head`, `SubscriptionNode.next`, `unlinked`). Java's
  memory model otherwise lets a walker see a half-built node.

## SendRing

One connection's outbound half: an MPSC queue plus one long-lived drain task writing into that
connection's channel. It exists because a socket has exactly **one** writer while the broker has many
producers: a session's own acks, plus every *other* session's forwards and fanouts.

- **A queue rather than a lock, because that is what makes a slow consumer a *local* problem.** A lock
  would hold the *sending* connection hostage to the *receiving* connection's TCP window, so one
  stalled client would stall every publisher fanning out to it. `enqueue` never touches the socket.
- **Bounded (1024) with drop-oldest**, so a consumer that stops reading sheds its own backlog rather
  than growing the broker's memory or blocking the publisher. The queue's own `size()` is the backlog
  count.
- **`Session.send` reports only that the frame was queued.** A sender has no useful response to
  another connection's socket dying. A write failure fires `exceptionCaught` on the *destination*,
  which closes that connection.
- **Ownership of each buffer passes to the ring on enqueue.** The ring releases it by writing it,
  dropping it, or discarding it once the channel is gone. Fanout takes every subscriber's reference
  (`retainedDuplicate`) up front, so the last ring cannot return the buffer mid-fanout.
- The drain is submitted once per burst, writes with the void promise, and flushes once.

## Events

`EventRegistry` maps `name → routeId → set of EventFilter`. A **set** per route rather than one
filter, because subscribing is additive: a second subscription narrowing differently must widen what
the route receives, not silently redefine what the first asked for. `EventFilter` is a record, so two
subscriptions narrowing the same way collapse by structural equality and re-subscribing is
idempotent. `subscribersOf` yields a route **at most once** however many filters match: the client
holds one handler per name.

- **Level-triggered replay is atomic against channel mutation.** `handleEventSubscribe` takes the
  snapshot and adds the subscriber under `ChannelRegistry`'s lock
  (`snapshotChannelsAndSubscribe`). This is the whole correctness argument: reading state and
  subscribing are otherwise two steps, and a channel registering *between* them lands in neither the
  snapshot nor the live stream, a silent gap in a picture the client is told is complete. The other
  order trades the gap for a duplicate. Under one lock there is no between. **The sends stay outside
  the lock**, or every channel mutation would block on one client's connection.
- **The replay goes out as one `event.replay` frame, not N `event` frames.** Drop-oldest is silent,
  so N frames could be truncated to their tail unnoticed, and replayed state isn't news. A name with
  nothing to replay sends no frame.
- **The replay is narrowed by the same filter as the live stream**, necessarily: its contract is that
  it holds exactly what the subscription would have delivered had it existed all along.
- **`emit` never throws.** A failed delivery is logged and counted, never propagated, which is what
  makes it safe from teardown. An event reports something that *already happened*, so failing to
  deliver it must not fail the thing itself.
- **Emission points all sit on the observation already**: `handleRegister` (after
  `connections.register`, so a subscriber reacting can immediately reach the route),
  `handleChannelRegister`, `handleTopicRegister`, `handleTopicSubscribe`/`Unsubscribe`, and teardown
  for `channel.removed` then `client.disconnected` (*after* the session is out of every table, so a
  subscriber looking the route up finds it already gone).

## The IDL repository

`idl/` is a REST content-management surface over `.idl` documents at `/idl`, sharing nothing with the
`/msgbus` socket. **An `.idl` is a design-time artifact**, fetched by a browser, a build step or curl,
none of which should have to register on the bus and speak `M01` to read a file. A document naming a
channel says nothing about whether that channel exists.

- **One file per document, and the Etag is a hash of its bytes.** Nothing about a revision is stored
  beside the document, so a file edited on disk by hand behaves correctly: it hashes differently next
  time, and an editor holding the old Etag is told it is stale. The corollary is that re-saving
  identical text is *not* a new revision and invalidates nobody.
- **The precondition is evaluated inside the same lock that writes.** A read-then-write in the
  controller would be two steps with a competing save free to land between them, so the precondition
  travels into `IdlRepository`. One lock for the whole repository, since a write is an operator
  saving an editor, not a message path. Reads stay outside it, which is sound only because a write
  lands via a temp file and an atomic move.
- **Names are validated, never sanitized**, and the leading character is alphanumeric, so traversal
  and dotfiles are excluded by construction. Windows device names (`CON`, `NUL`, `LPT1`, …) are
  rejected on every platform: they resolve as devices whatever the extension.
- **The repository validates the envelope; the editor validates the grammar.** `IdlValidator` checks
  that a document parses and is an array of channels that each declare a `name` and some `topics`,
  the grammar's own requiredness, so it stays a prefix of the full check. Everything below a channel
  (type expressions, reference resolution) is `monitoring/src/idl/idl.grammar`. Half-duplicating it
  here would be a second, weaker definition of the same language.
- A leading UTF-8 BOM is stripped on write, the one place the author's bytes are not kept verbatim:
  every conforming JSON parser rejects one.

## BrokerPolicy

`Policy.resolve` resolves both wire sentinels once per data-plane frame, so the frame path reads TTL
and time as always-present values. **Defaults are the broker's concern, not the router's**: the
session **enforces** TTL (the broker is the only party holding a message between send and delivery,
so nothing else can bound undelivered traffic), while `Policy` decides what the values mean.

Two sentinel readings worth knowing: an explicit `ttl = 0` gets the **default**, not immediate expiry.
An unstamped `time` (`0`) means "sender left it unset", not 1970: it is substituted with now *in the
sender's clock*, or the message would read as ~56 years old and be dropped.

**`from` is re-stamped too** (`stampedFrom`), on the single re-stamp every forwarded frame already
passes through, so no forward path can forget it. Routing has always used the registry entry for the
arriving socket rather than the claimed header. Re-stamping means a recipient reading `from` reads
the sender, not whatever the sender wrote. An unregistered sender keeps its claim, since there is
nothing to replace it with, and every path drops those frames before delivery anyway.

**Clock offset is applied in both directions and neither leaks.** `time` is translated into broker
time for the expiry check (`time + senderOffsetMs`), using the connection's own offset, not one looked
up by the claimed `from`. The resolved `time` written back onto the wire is in the *sender's* clock
(`brokerNow − senderOffsetMs`), since leaking broker time would double-apply the offset at a recipient
doing its own correction.

**Re-emitting rather than relaying verbatim** makes the resolved TTL a fact the message carries,
instead of a default every downstream party re-derives, possibly differently.

## Discovery and shutdown

- **The discovery port comes from the running server** (`ServerStartupEvent`), not from a configured
  URL. The publisher is a `@Context` bean whose constructor creates the realm's pipe, so a second
  broker in the same realm fails at startup.
- **`Bus.closeSockets` (`@PreDestroy`) closes every open `/msgbus` socket.** A WebSocket never
  completes as an HTTP request would, and its pipeline no longer holds the Micronaut handlers that
  graceful shutdown waits on. Without the close, one connected client (the monitor page holds one and
  reconnects) would hold shutdown open.

## Known quirks, kept for parity with the C# Broker

- `broker.connections.active` drifts upward when a session is superseded.
- A same-route `channel.register` emits `channel.registered` again.
- A dispatch with no `replyTo` is forwarded with no correlation recorded.

## Tests

- **`routing/TestBus` is the in-memory harness**: real registries, real `Session`s, each on an
  `EmbeddedChannel`. `settle()` runs pending tasks until nothing moves, so the ring drain and the
  resolve hop are deterministic. `runtime`'s codec (test scope) builds and reads every frame, so each
  assertion is also a cross-implementation check.
- **e2e** (`e2e/`) is `@MicronautTest` with SSL off, a random port, and a `TEST<uuid>` realm per class,
  because the `Default` realm's pipe is already held by any broker the developer is running. It drives
  raw JDK WebSockets and the JDK `HttpClient`.
- `e2e/StaticContentTest` skips `/monitor/` until monitoring has been built.
- **Surefire runs paranoid leak detection.** `AllocationTest` turns it off while measuring and runs on
  a `FastThreadLocalThread`, because Netty 4.2 keeps Recycler pools and allocator caches only on those
  threads, and every event loop is one.

## Run

Run `io.minaret.broker.Application` from the IDE with `broker/` as the working directory.
`application.yml` resolves `devcert.pfx`, `wwwroot` and `idl` relative to it. Only one broker can hold
a realm's pipe, so stop any other broker in the same realm (including a C# one) first.
