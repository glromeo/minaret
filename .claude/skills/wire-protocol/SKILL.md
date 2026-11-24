---
name: wire-protocol
description: The M01 wire protocol - frame format, field order and broker read-order rationale, base64url Guid encoding, the /channel/topic destination form, gzip compression threshold, validation and versioning, worked example frames, and the codecs that implement it (runtime's text Wire.java, broker's zero-copy Frame/FrameWriter, the JS wire.js). Read this before changing any of them, adding or reordering a header field, touching framing, parsing or compression, or porting the protocol to another language.
---

# M01 wire protocol

`M01|<to>|<type>|<time>|<ttl>|<from>|<replyTo>|<enc>|<bodyLength>[|<extras>...]\0<body>`: a
`\0`-split, `|`-delimited **text** frame (browsers are first-class clients, so binary frames are out).
It exists so the broker can route by reading **only** the header: it scans to the `\0`, parses the
prefix, and never looks past it, so a relayed frame costs no payload walk. The `Header` fields it
carries are defined in the `wire-contract` skill. This document is meant to be enough to implement a
new port.

## Where it is written

Each implementation has a test pinning the same literals, which is what turns "written N times" into
something checkable. A change to framing or field order lands in all of them:

- **`runtime/.../messaging/wire/Wire.java`**, text-only. A client never relays a body it doesn't
  consume, and a WebSocket delivers whole messages, so there is no byte path and no buffer pooling.
  The one pool is `Gzip`'s Deflaters/Inflaters, which a per-message `GZIP*Stream` would build and
  clean through the JDK's globally locked Cleaner.
  `parseHeader(String)` answers a `WireFrame` (or an error); `parseBody`/`parseBodyAsJson`/`parseBodyAs`
  decode the body; `prepareBody` serializes and decides compression; `serializeText` /
  `serializeMessage` originate a frame. Pinned by `runtime/.../wire/WireTest`.
- **`broker/.../wire/`**: `Frame` (parse in place), `FrameWriter` (render), `Guids`, `Wire`
  (constants). The only byte relay path. `Frame` is a flyweight over the message's pooled `ByteBuf`: it
  records where each field lies and decodes only what routing reads (ids into probe `Uuid`s, numbers
  into primitives), builds no `String`, and copies nothing. `FrameWriter.relay` renders a re-stamped
  header around the received body into one pooled buffer, with the WebSocket header back-filled in
  front; `originate` does the same for a broker-authored frame. The grammar and error strings match the
  C# Broker's, checked in the same order, so both brokers drop the same frames for the same reason. A
  message with no `\0` is header-only, not "missing body separator". Headers past `MAX_HEADER_BYTES`
  are fatal to the connection. Pinned by `broker/.../wire/WireTest`.
- **`javascript/src/messaging/utils/wire.js`**, text-only like runtime, with an async body decode
  since gzip goes through `DecompressionStream`. Pinned by `javascript/test/wire.test.js`
  (`node --test`).
- The C# `Runtime/Messaging/Utils/Wire.cs` in the old MessageBus repo is the original, still a live
  peer during migration.

## The rules

- **Field order is broker read-order.** `to` first (it decides control plane vs forward and is the
  routing key), then `type` (the verb, which says what namespace `to` is in), then `time`/`ttl` (the
  whole expiry check). `from` is **overwritten in flight**: the broker stamps it from the registry
  entry for the arriving socket, alongside the `ttl`/`time` defaults, so what a sender writes there never
  reaches a recipient and a recipient may read it as identity. `enc`/`bodyLength` describe the body,
  which the broker never touches. `replyTo` is *mostly* passthrough, but on a topic `dispatch` the broker
  reads it to correlate a reply back to the connection awaiting it. It is never trusted as an identity
  claim, only matched against ids the broker itself recorded.
- **The `to` segment has two forms**: a base64url Guid, or `/channel/topic` for an unresolved topic
  (`wire-contract` "Destination"). Unambiguous because base64url's alphabet excludes `/`. The split is
  on the **first** `/` after the leading marker, so a `/` inside the topic half is kept, while a channel
  name containing one is rejected on serialize. An empty half (`//echo`, `/LAYER_1/`) is a parse error,
  not a name of `""`.
- **`bodyLength` is optional**, unlike every field before it. A sender that knows its body's byte count
  stamps it as a receive-side sizing hint; an older sender or a hand-built frame omits it. Nothing
  routing-related reads it. Every codec here stamps it anyway, so frames stay byte-identical across
  implementations.
- **Empty segment = field absent.** There is deliberately no way to say "`time` genuinely 0", matching
  the `0`-as-sentinel contract in the broker's policy. An explicit `ttl = 0` serializes as `"0"` and
  parses back as `0`, **not** null. Do not collapse them: invisible today, since the policy treats null
  and 0 alike, but a silent bug the moment it distinguishes them.
- **Guids: base64url of the 16 raw bytes, RFC-4122 big-endian, unpadded, exactly 22 chars** (vs 36). In
  Java that is `getMostSignificantBits` then `getLeastSignificantBits`. (C#'s `Guid.ToByteArray()` is
  little-endian for the first three fields, which is why the C# side asks for big-endian explicitly.)
  Pinned by `00112233-4455-6677-8899-aabbccddeeff` → `ABEiM0RVZneImaq7zN3u_w`.
- **Numbers are decimal.** A negative `ttl` must *parse* (the broker treats it as already expired).
  Base-36 was rejected: ~4 chars saved, against a bespoke encoding every port must reimplement, when the
  Guid encoding already recovers 42 chars per frame.
- **base64url's alphabet (`A-Z a-z 0-9 - _`) is disjoint from `|` and `\0`**, so Guids and compressed
  bodies need no escaping. `|` is unconstrained *inside* the body; it only means something before the
  `\0`.
- **Validation on serialize, no escaping.** `type` must be non-empty and free of `|`/`\0`; extras and
  body free of `\0`, or serialization throws. That keeps the parser a plain scan on the broker's hot
  path. No current body can contain `\0`: they are all JSON, which escapes `U+0000`.
- **Compression is app-level, deliberately not `permessage-deflate`**, which must stay off (the
  broker's `WebSocketServerProtocolHandler` is built with `allowExtensions(false)`). Transport
  compression would force the broker to inflate a body it never reads and re-deflate on every hop.
  App-level compression means the body arrives compressed, crosses the broker as opaque bytes, and only
  the recipient inflates. Bodies at or above `COMPRESSION_THRESHOLD_BYTES` (1024) are gzipped and
  base64url'd with `enc=gzip`. The threshold is on the **pre-compression input size**, never the
  compressed output: deflate implementations differ, and an output-size comparison would make two of
  them frame the same message differently.
- **`serializeText` and `serializeMessage` are named apart on purpose.** One overloaded name would bind
  `serializeMessage(header, someString)` to the object overload and JSON-encode a body that was already
  text. Both **overwrite** `header.encoding` with whatever the threshold check decides: they author the
  encoding decision rather than consume one.
- **Parsing does not throw on bad input.** Malformed frames are logged and dropped, not
  connection-fatal (except an oversized header on the broker), and untrusted input shouldn't cost an
  exception on the hot path.
- **`M02` is rejected by an `M01` parser.** Versioning is at the tag, and extras handle *field* growth
  within a version. A frame with fields past `bodyLength` keeps them verbatim in `Header.extras`, and a
  relay carries them forward untouched.

## Worked frames

`bodyLength` shown as the body's UTF-8 byte count; a minimal port may leave it empty.

```
# register handshake / ack — to empty = broker-addressed; from null on the ack = broker-originated
M01||client.register|1769472000000||qO1TL6vjTk-1c4bnp5-lVQ||58\0{"routeId":"a8ed...","clockOffsetMs":0}
M01|qO1TL6vjTk-1c4bnp5-lVQ|client.registered|1769472000123||||60\0{"routeId":"a8ed...","clockOffsetMs":37}

# direct client-to-client — to is a RouteId, since the verb is not a topic verb
M01|B0FpbYy2R0O3hZ2y3sJ2Xw|greeting|1769472000000||qO1TL6vjTk-1c4bnp5-lVQ||7\0"hello"

# topic traffic — to is a TopicId; replyTo correlates a dispatch to its reply
M01|Rk9PdG9waWNpZDEyMzQ1Ng|dispatch|1769472000000||qO1T...|c29tZWNvcnJlbGF0aW9u|7\0"hello"
M01|Rk9PdG9waWNpZDEyMzQ1Ng|publish|1769472000000||B0Fp...||8\0"tick 1"

# first send to a topic whose id the sender hasn't learned yet — /channel/topic, resolved by the
# broker, which then acks topic.resolved so every later send carries the plain id
M01|/LAYER_1/ticks|publish|1769472000000||B0Fp...||8\0"tick 1"

# gzip + an unknown trailing field from a newer sender, both relayed intact
M01|B0Fp...|bulk|1769472000000|60|qO1T...||gzip|1420|somefuturevalue\0H4sIAAAAAAAA...
```

`ClientInfo` is ~70 bytes of JSON, far under the threshold, so **the handshake and ack are always
plain text**: a new client can bootstrap with only the base64url Guid codec, no gzip.
