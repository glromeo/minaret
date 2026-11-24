# @messagebus/client

The JavaScript client runtime — the counterpart of the Java `runtime` module, for Node and the browser.

ESM only, zero dependencies, no build step: the published files are the source files, and the
TypeScript types are hand-written `.d.ts` siblings rather than compiler output. Nothing here is
transpiled, so what you debug is what was written.

## Install

```
npm install @messagebus/client
```

Within this repository it is consumed by path — see `monitoring/package.json`, which depends on
`file:../javascript`.

Requires Node 22+ (for the global `WebSocket`) or any current browser. Both supply the other
globals this uses: `crypto.getRandomValues`, `TextEncoder`, and `CompressionStream`.

## Use

```js
import { MessageBus, newClientInfo } from "@messagebus/client";

const bus = await MessageBus.connect(newClientInfo({ role: "worker" }), { persistent: true });

// Provider: claim a channel, declare a topic, answer requests on it.
const provider = await bus.createChannel("LAYER_1");
await provider.subscribe("echo", (body, header) => provider.dispatch("echo", body, { header }));
await provider.publish("ticks", { at: Date.now() });

// Client: resolve the channel, subscribe to its fanout, ask it something.
const client = await bus.connectChannel("LAYER_1");
await client.subscribe("ticks", tick => console.log(tick));
console.log(await client.call("echo", "hello"));
```

Watching the Broker itself needs no channel at all — that is Layer 0:

```js
import { EventNames } from "@messagebus/client";

await bus.router.on(EventNames.ChannelRegistered, (event, origin) => {
    console.log(origin === "replay" ? "already there:" : "just arrived:", event.channel);
});
```

Awaiting `on` means "I have the full picture", not merely "I am subscribed": the Broker replays
everything already true before it acks.

## Where the Broker is

`MessageBus.connect` resolves that per environment, and the two answers are genuinely different
questions rather than two ways to do one thing:

- **Browser** — the Broker serves the page, so the socket is same-origin. Nothing to discover.
- **Node** — named-pipe discovery, realm-scoped, exactly as the Java runtime does it. Set
  `MESSAGEBUS_REALM` (or pass `realm`) to reach a Broker other than the `Default` one.

Pass `url` to bypass both.

## Layout

Every module mirrors its counterpart in the Java `runtime` (under
`runtime/src/main/java/io/minaret/`), so a change on either side has an obvious home on the other:

| here | there |
| --- | --- |
| `src/messaging/message.js` | `messaging/Message.java` + `Header.java` + `Destination.java` |
| `src/messaging/verbs.js` | `messaging/Verbs.java` |
| `src/messaging/events.js` | `messaging/Events.java` |
| `src/messaging/router.js` | `messaging/Router.java` |
| `src/messaging/channels.js` | `messaging/Channels.java` + `ChannelBase.java`, `ChannelClient.java`, `ChannelProvider.java` |
| `src/messaging/connection.js` | `messaging/Connection.java` + `ConnectionState.java` + `CloseCodes.java` |
| `src/messaging/websocket-connection.js` | `messaging/WebSocketConnection.java` |
| `src/messaging/utils/wire.js` | `messaging/wire/Wire.java` |
| `src/messaging/utils/{guid,base64url}.js` | `messaging/wire/Guids.java` |
| `src/discovery/realm.js` | `discovery/Realm.java` |
| `src/discovery/broker-discovery.js` | `discovery/BrokerDiscovery.java` |
| `src/utils/json.js` | `util/Json.java` |
| `src/utils/emitter.js` | `util/Emitter.java` |

`src/utils/logger.js` and `src/utils/async.js` have no counterpart: Java uses `System.Logger` and
`CompletableFuture`.

`CLAUDE.md` records what this port does differently from `runtime/`, and why.
