# Minaret

A message bus: a broker plus Java and JavaScript clients. Clients claim named **channels**, declare
**topics** on them, and exchange messages by publish/subscribe or request/reply, all through one broker.

## Status

- Working end to end: direct routing, channels, topics, subscribe, publish fanout, request/reply,
  presence events, channel reclaim.
- Java annotated API (`@Channel`/`@Topic` interfaces) works.
- Not built yet: `DispatchMode.Any`/`All`, code generation from `.idl` files. See `TODO.md`.

## Modules

| Path | What it is |
|---|---|
| `broker/` | The broker (Micronaut 4 + Netty). Serves `/msgbus` (WSS), `/idl` (REST) and the monitor on `https://localhost:5443`. |
| `runtime/` | `io.minaret:runtime`, the Java client. Its only dependency is Jackson. |
| `javascript/` | `@messagebus/client`, the JS client for Node 22+ and browsers. See its `README.md`. |
| `monitoring/` | Web app that shows live bus state, served by the broker at `/monitor/`. |
| `examples/layer0`, `examples/layer1` | Runnable echo round trips. |
| `benchmark/` | Load generator run against a live broker. |

## Requirements

- JDK 17
- Maven 3.9 (the POMs are `pom.yml`, read through polyglot-yaml, which does not load on Maven 4)
- Node 22+ for `javascript/` and `monitoring/`
- A dev TLS certificate at `broker/devcert.pfx` (see below)

## Build

```
mvn verify                                   # all Java modules and tests
cd javascript && npm test
cd monitoring && npm ci && npm run build     # outputs to broker/wwwroot/monitor/
```

## Dev certificate

The broker serves `broker/devcert.pfx` (PKCS12). Its password comes from `MESSAGEBUS_CERT_PASSWORD`,
which defaults to `messagebus`. With [mkcert](https://github.com/FiloSottile/mkcert):

```
mkcert -install
mkcert -pkcs12 -p12-file broker/devcert.pfx localhost 127.0.0.1 ::1
```

mkcert sets the password to `changeit`, so either set `MESSAGEBUS_CERT_PASSWORD=changeit` or re-export
the file with `messagebus`.

Who has to trust the certificate:
- Browsers (the monitor): trust comes from the OS store, which `mkcert -install` updates.
- Node clients: set `NODE_EXTRA_CA_CERTS="$(mkcert -CAROOT)/rootCA.pem"`.
- Java clients: nothing to do. They trust localhost.

## Run

Start the broker, then the example's two sides as separate processes. The client may start first: it
waits until the provider claims the channel.

1. Broker: `io.minaret.broker.Application`, with `broker/` as the working directory.
2. Provider: `io.minaret.examples.layer1.Main --provider`
3. Client: `io.minaret.examples.layer1.Main --client`

The client sends three requests and checks that the replies come back in send order. The monitor is at
`https://localhost:5443/monitor/`.

## Using the Java client

```java
try (Channels bus = Channels.connect(ClientInfo.create("my-provider")).join()) {
    ChannelProvider provider = bus.createChannel("LAYER_1").join();
    provider.subscribe("echo", (header, body) ->
            provider.dispatch("echo", "echo: " + body.asText())).join();
    // ...
}
```

```java
try (Channels bus = Channels.connect(ClientInfo.create("my-client")).join()) {
    ChannelClient client = bus.connectChannel("LAYER_1").join();
    client.subscribe("echo", (header, body) -> System.out.println(body.asText())).join();
    client.dispatch("echo", "hello").join();
}
```

`Channels.connect` finds the broker on its own, so clients need no URL. `examples/layer1` is the
complete version of this code.

## Realms

A realm is a named partition (`Default`, `Red`, `Blue`, …) that lets several independent broker and
client setups run on one machine. A client only finds the broker in its own realm, and each realm has
exactly one broker.

- Broker: `MESSAGEBUS_REALM=Red` or `-Dmessagebus.realm=Red`. The default is `Default`.
- Java and Node clients: `MESSAGEBUS_REALM`, or the `realm` option in JS. The default is `Default`.

## IntelliJ import

IntelliJ reads only `pom.xml`. Generate those files from the root (one run covers every module, and the
output is gitignored) and repeat after editing any `pom.yml`:

```
mvn io.takari.polyglot:polyglot-translate-plugin:translate -Dinput=pom.yml -Doutput=pom.xml
```
