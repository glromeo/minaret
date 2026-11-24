---
name: wire-contract
description: The shared client/server wire contract - Message and Header field-by-field, the Destination record (resolved id vs the /channel/topic self-resolving path, and how the verb decides which table an id names), the Verbs vocabulary including notify, the Layer-0 Events plane (event payloads, EventFilter narrowing, level- vs edge-triggered replay, the event.replay verb) and CloseCodes. Read this before changing Header.java, Destination.java, Verbs.java, Events.java or CloseCodes.java in runtime or their broker/contract copies, adding a header field or a verb or an event name, touching topic addressing or channel resolution, or porting any of it to another language such as the JS client.
---

# Shared wire contract

The types both ends depend on. In this repo they are written **twice in Java**:
- `runtime/src/main/java/io/minaret/messaging/` — `Header`, `Message`, `Destination`, `Verbs`,
  `Events`, `CloseCodes`, `ControlBodies`, `ClientInfo` (Jackson);
- `broker/src/main/java/io/minaret/broker/contract/` — `Verbs`, `EventNames`, `CloseCodes`,
  `EventFilter`, `Contract` (Micronaut Serde, compile-time `@Serdeable`), with the header parsed in
  place by `broker/.../wire/Frame` rather than into a record.

They are kept identical by hand and pinned by `broker/.../contract/ContractParityTest`, which compares
`Verbs`, event names and `CloseCodes` constant by constant, and each shared body's record components (its
JSON properties). Pair a new body there when you add one. Broker tests also build and read every frame
with runtime's codec (`TestBus`, `e2e/`). A third copy is
`javascript/src/messaging/` (`verbs.js`, `events.js`, `connection.js`'s `CloseCodes`). Adding a verb,
event name or field means all of them.

Broker-side internals are in `broker/CLAUDE.md`, client-side in `runtime/CLAUDE.md`, and the `M01`
codec that carries all of this is the `wire-protocol` skill. The types carry `/** */` blocks of their
own; what is here is the cross-cutting rationale.

## Header and Message

`Header` is a record:
- `from` (`UUID`, **null means the broker sent it**, e.g. the `client.registered` ack);
- `to` (a `Destination`, **`Destination.EMPTY` means it is addressed to the broker**, e.g. the
  `client.register` handshake). The two are **symmetric** control-plane conventions;
- `type` (required, the **verb**, see Verbs);
- `replyTo` (`UUID`, a correlation id; on a topic `dispatch` it is also what the broker uses to route
  the reply back to the one connection awaiting it);
- `time` (Unix epoch **milliseconds**; `0` means unset, and `Router.sendMessage` stamps it);
- `ttl` (`Integer`, seconds; `null` or `0` means the broker's default lifetime, see `broker/CLAUDE.md`
  "BrokerPolicy");
- `encoding` (`BodyEncoding.RAW` or `GZIP`, the frame's `enc` segment; `GZIP` means still compressed);
- `bodyLength` (the body's exact byte count on the wire when the sender knew it; `null` when not
  stamped);
- `extras` (trailing header segments a later wire version added that this one doesn't understand,
  `|`-joined; `""` when none).

`encoding`, `bodyLength` and `extras` live on `Header` rather than as parameters threaded alongside
every send and parse, because they describe *this message*, not the call handling it. A header built to
send a body already knows how that body is encoded and how long it is, and a received header already
carries both facts about how its body arrived.

**Re-stamping goes through `withX` methods**, one per field something actually re-stamps, never a
general builder. A copy through the canonical constructor carries every component, so a field added
later survives every re-stamp site. A hand-rolled builder would invite copies that silently drop it.

`Message<T>` pairs a `Header` with a body. **`Message` is the client-facing shape**; the wire format is
`Wire`'s `M01` framing. `Router` hands subscribers `Message<JsonNode>` so they needn't know the payload
type up front: callers convert the body to whatever they expect for that `type`
(`Json.convert(body, Expected.class)`).

## Destination

A record holding `Header.to`: either a resolved `id` or an unresolved `channel` + `topic` path (set
together), never both. A type rather than three `Header` fields so the mutual exclusion is enforced,
not conventional. `Destination.EMPTY` (neither set) is the broker-addressed control plane.

- **What `id` *means* depends on `Header.type`.** For `dispatch`/`publish`/`notify` it is a TopicId.
  For a direct client-to-client message it is a recipient RouteId. The verb tells the broker which
  table to look it up in, so the two id namespaces never need distinguishing on the wire, and `to` is a
  plain 22-char base64url segment.
- **The path is the `/channel/topic` self-resolving form.** A client dispatching to a provider's topic
  has no id for it: it never subscribed, and `topic.list` returns names only. Rather than add a resolve
  round trip, the first send carries the *path* (wire-encoded `/LAYER_1/echo`). The broker resolves
  it, routes normally, **and** sends the sender a `topic.resolved` ack carrying the id, which the
  client caches so every later send is a plain id. The ack is required rather than inferred from a
  reply, because a topic may be fire-and-forget. Two sends racing ahead of the ack both send the path
  and resolve identically: the operation is idempotent.

  The ack names the topic's **channel** too (`BrokerAck.channel`), and a handle learns the id only
  when that is its own channel: the ack goes to the *connection*, and every handle on it sees it.
  Without the check, a provider serving several channels with same-named topics would cache the first
  channel's id under every handle and send the other channels' traffic to it.

  The ack is sent **before** the frame it resolves is forwarded. A provider subscribed to its own
  topic would otherwise receive a frame addressed by an id it has not learned, and a handle drops any
  frame whose `to.id` is not in its handler table.
- **Both halves travel because a topic name alone does not identify a topic.** Two channels may each
  register a topic called `run`, so a bare name forces the broker to *guess*, and any guess is a
  misroute. The sender always knows its channel (it is the handle the call was made on), so it sends
  it; `ChannelBase.destinationFor` builds the path.
- **Resolving the channel half depends on the sender's role**, in one step (`Session.resolveChannel`),
  with no search. A **provider** *is* the channel it publishes on, so the named channel must be its own
  and the RouteId is the sender's. A **client** resolved the channel earlier via `channel.resolve`, and
  the session pins that result in a per-session `channelName → providerRouteId` map. Either way the
  topic is then one exact lookup by (channel RouteId, channel name, topic name).
- **The pin is a RouteId, and the broker-wide channel table is deliberately never consulted here.**
  Re-resolving the name at dispatch time would let a *replacement* provider inherit frames aimed at
  the one the client resolved: talking to a process it never chose, silently. Holding the RouteId
  means that once that provider dies, the dispatch drops as `unknown-topic`. Nothing has to sweep the
  pin on disconnect: a stale RouteId simply misses. The map lives on the session, so it needs no lock
  and dies with the connection.
- **The `/` delimiters exist only at the wire boundary** and are never part of `channel`/`topic`. `/`
  is a safe discriminator because base64url's alphabet (`A-Z a-z 0-9 - _`) excludes it. The split is on
  the **first** `/` after the marker, so a `/` inside the topic half is kept (leaving a later
  `/channel/topic/sub` form parseable by the same rule), while a channel name containing one is
  rejected on serialize. Both names are validated free of `|`/`\0` on serialize, as `type` is. An empty
  half is rejected rather than round-tripped: `//echo` or `/LAYER_1/` would parse back as `""`.

## Verbs

The `Header.type` vocabulary as `static final String`s, one class per implementation so no side
writes a literal. **`type` is the verb (what the recipient should do with the message) and nothing
else.** Topic identity lives in `to`, so "what is this?" and "which topic is this?" are separate fields.

- **Control plane**, `noun.verb`, broker-addressed (`Destination.EMPTY`):
  `client.register`/`client.registered`, `channel.register`/`channel.registered`,
  `channel.resolve`/`channel.resolved`, `topic.register`/`topic.registered`, `topic.list`/`topic.listed`,
  `topic.subscribe`/`topic.subscribed`, `topic.unsubscribe`/`topic.unsubscribed`, and `topic.resolved`
  (the unsolicited `/channel/topic` resolution ack: a reply to no request, so no `replyTo`).
- **Data plane**, bare, with a topic in `to`: `dispatch`, `publish`, `published`, `notify`.
- **Events**: `event.subscribe`/`event.subscribed`, plus the delivery verbs `event` and
  `event.replay`.

**`notify` is the provider's unsolicited send to *one* client.** `publish` reaches every subscriber,
`dispatch` answers someone who asked, and this is neither. It uses `dispatch`'s header exactly: `to` is
the TopicId (so the recipient routes it through the same id-keyed handler table), with **`replyTo`
carrying the recipient RouteId**. A notification correlates to nothing, so `replyTo` is free, and the
verb says so: the same rule that lets `to.id` mean a TopicId for a topic verb and a RouteId otherwise.

**It is a verb of its own rather than a `dispatch` whose `replyTo` misses the reply table.** The
broker takes `dispatch`'s direction from *who sent it*, so every frame a provider sends on its own
topic reads as a reply. A fallback for the miss would make the broker guess between two meanings of
one field, and would turn a duplicate or late reply (today a logged "no awaiting request" drop) into a
send to whatever route the stale id names. A new verb is only a new `type` string, so it costs no
wire-format change.

## Events

Layer-0 notifications about things the broker observes. A client calls `Router.on(name, handler)`,
which completes when the broker acks the subscription. The broker then sends `type = "event"` frames
(`from = null`, `to =` the subscriber's RouteId) whose body is an `Event`: a `name` plus that event's
fields.

- **The name is in the body, not in `type`.** `type` stays the verb (`event` for every event), for the
  same reason topic identity lives in `to`. It also lets an event be *named* after a verb
  (`channel.registered` is both an ack verb and an event name) without the two colliding, since they
  never share a field.
- **Payload shapes** are records implementing the sealed `Event`:
  - `ClientEvent` (`routeId` plus the whole registered `ClientInfo`, because a subscriber that narrowed
    on one client property almost certainly wants the others, and no request returns another
    client's `ClientInfo`);
  - `ChannelEvent` (`channel`, `providerRouteId`);
  - `TopicEvent` (`channel`, `topic`, `topicId`, and `subscriberRouteId` on subscribe/unsubscribe).
    It carries the channel for the same reason `Destination` does.

  `name` is many-to-one onto these types (connected and disconnected are both a `ClientEvent`), so the
  body carries no type discriminator. The subscriber knows the shape from the name it subscribed to.
  Only record components are serialized: `broker/.../contract/ContractTest` pins that the broker's
  `channel()`/`topic()` defaults (used by `EventFilter.matches`) don't leak into the body.
- **`Events.Names` / `EventNames`** pair an arrival with a departure: `client.connected` /
  `client.disconnected` (any connection, channel or not), `channel.registered` / `channel.removed` (a
  provider claiming or releasing a name), and `topic.registered` + `topic.subscribed` /
  `topic.unsubscribed`. `topic.unsubscribed` fires on an **explicit** unsubscribe only. A subscriber
  that drops is reported by `client.disconnected`: the disconnect path unlinks its subscriptions
  without per-topic events.
- **Subscriptions can be narrowed** by `Events.Subscription`'s `channel`/`topic`/`where`, held
  broker-side as an **`EventFilter`** record, with null/empty meaning "any". The halves apply to
  disjoint event families:
  - **`channel` + `topic`** narrow channel and topic events. A pair rather than a predicate or glob
    because that is the axis the payloads have, so matching is one or two string comparisons, with no
    pattern syntax for another language to reimplement.
  - **`where`** narrows `client.*` events by `ClientInfo.properties`, the free-form bag a client
    declares at registration (role, host, region, version…). An **open bag rather than named filter
    fields**, so a new property is usable as a filter immediately with no change to the filter, the
    matcher or the wire body. String values only: exact equality is the one comparison needing no type
    negotiation across languages. All entries must hold (**AND**). A subscriber wanting either can
    subscribe twice, since filters are additive.

  A half that **cannot apply** to the occurrence is ignored rather than treated as unmatched, or a
  channel-narrowed subscription to `client.connected` would silently deliver nothing. Narrowing
  applies to **both** the live stream and the replay. Filters are **additive per name**: two `on`
  calls narrowing differently deliver the union, and a route whose filters overlap still receives each
  occurrence once. Equality is structural (records, and `Map.equals` by content), so re-subscribing the
  same filter is idempotent.
- **Level- vs edge-triggered.** Events describing something that *currently exists*
  (`client.connected`, `channel.registered`, `topic.registered`) are **replayed** to a new subscriber
  for everything live at subscribe time, so a late subscriber still sees a complete picture. The
  `.disconnected`/`.removed`/`.unsubscribed` counterparts describe a transition *away* from a state and
  have nothing to replay. `topic.subscribed` could be replayed but isn't: nothing reads subscriber lists
  by name, and a provider wanting its audience can watch from the moment it registers the topic. Replay
  is sent **before** the ack, so awaiting `on` means "I have the full picture", not merely "I am
  subscribed".
- **Replay is its own verb, `event.replay`, carrying the whole snapshot in one frame** (body:
  `Events.Replay`), where a live occurrence is one `event` frame. Two reasons point the same way.
  *Framing*: a subscriber's send queue is bounded and drops oldest **silently**, so an N-frame replay
  on a broker with thousands of topics could be truncated to its tail unnoticed. One frame makes it one
  all-or-nothing decision. *Meaning*: a replayed occurrence is state, not news. A client reacting to a
  provider *appearing* shouldn't react identically to the forty that already existed. Handlers receive
  an `Events.Origin` (`LIVE`/`REPLAY`) saying which. A name that replays nothing sends **no** replay
  frame. `Replay` holds one concrete-typed list per family (clients, channels, topics, exactly one
  populated), so each element serializes with its own fields.

## CloseCodes

Why the broker closed a connection, carried in the WebSocket close frame rather than in a message. A
constant vocabulary like `Verbs`, in each implementation. Values sit in the 4000–4999 application
range, so none collide with a protocol code an intermediary might send (1006 abnormal, 1001 going
away).

- **`SUPERSEDED` (4001)**: another connection registered under this client's `ClientInfo.identity`
  and took its RouteId (the broker's newer-wins rule, `broker/CLAUDE.md` "Identity"). **Every client
  runtime treats it as terminal: the reconnect policy is disarmed, not merely skipped for that drop.**
  That is the reason the code exists. A reconnect policy answers a drop by re-registering, so an
  evicted client would claim the identity straight back and evict its replacement, which would
  reconnect in turn. Since each registration and teardown emits presence events, the symptom is an
  **event storm** rather than either client failing.
- **A close code rather than a control verb**, because the message is that there will be no more
  frames. A verb would have to be received, dispatched and acted on before the socket died, a race the
  transport settles for free. It also costs no change to `M01`.
- The code is read *only* to make that decision. An ending with no code (a fault, an aborted socket) is
  what the reconnect policy is for, so it stays a plain retry.
