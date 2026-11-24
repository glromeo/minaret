# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Guidelines

be extremely concise, use bullet points, and skip pleasantries
don't guess, don't take shortcuts, don't bloat, write clever code

Doc comments are `/** */` Javadoc-style prose. No HTML tags (`<p>`, `<ul>`/`<li>`, `<b>`/`<em>`,
`<code>`): bullets are plain `- ` lines and symbol names are bare. `{@link X}` is fine where it adds a
compiler-checked reference. Prefer explaining in the doc block over inline `//` comments in the body.

**Shape of a doc block: one line saying what the thing is, then one short paragraph per distinctive
call site.** Each paragraph names the caller and says in plain sentences what that use needs from this
member, which find-usages can't tell you from the call site alone. No preamble, no restating the type's
own design, no essay: if a paragraph doesn't correspond to a real usage, cut it.
`ChannelBase.request` is the reference example.

This file is the only one always in context, so it holds the map and the detail lives one hop away, in
a skill or a module `CLAUDE.md`, each loading only when the work touches it. Put new rationale where
the thing it explains lives, not here.

## Project state

Minaret is a message bus, the Java successor to the C# MessageBus solution. It was moved here from
there: `broker` was its Java broker, `runtime` its Java client, and the examples are Java ports of the
C# ones. It stays **wire-identical** to the C# side (same `M01` frames, port, realm pipe name,
`MESSAGEBUS_REALM`), so C# and JS clients still connect to this broker during the migration. Brand
strings that cross the wire still say `messagebus`; Java packages are `io.minaret.*`.

- Layer 0 (router, wire, direct routing) and Layer 1 (channels, topics, subscribe, publish fanout,
  request/reply correlation, presence events, channel reclaim) work end to end. `examples/layer1` is
  the practical smoke test.
- Layer 2's **annotated surface** works in `runtime` (`@Channel`/`@Topic`, a `java.lang.reflect.Proxy`
  for sending, reflective binding for handling). The **JSON-IDL** half has an editor
  (`monitoring/src/idl/`) and a store (`broker`'s `/idl` REST repository). Nothing yet generates from
  or validates against an `.idl`.
- Unbuilt: `DispatchMode.Any`/`All`. `TODO.md` is the open-items list and holds nothing already done.

## Where the detail lives

- **`wire-contract` skill**: the client/broker contract (`Header`, `Message`, `Destination`, `Verbs`,
  `Events`, `CloseCodes`), written once in `runtime` and once in `broker/.../contract`. Read it before
  adding a header field, verb or event name, or touching topic addressing.
- **`wire-protocol` skill**: the `M01` codec carrying all of the above, and the four places it is
  implemented.
- **`broker/CLAUDE.md`**: routing, the registries, `SendRing`, identity, events, the IDL repository,
  policy, shutdown, and the Netty pipeline's traps.
- **`runtime/CLAUDE.md`**: `Router`, the channel handles, the annotated surface, transport, discovery,
  identity.
- **`javascript/CLAUDE.md`** (divergences from `runtime`) and **`monitoring/CLAUDE.md`**.

## Layout

- `runtime/`: `io.minaret:runtime`, the Java client. One dependency (Jackson).
- `broker/`: `io.minaret:broker`, Micronaut 4 + raw Netty, zero allocation per relayed frame. Serves
  `/msgbus` (WSS), `/idl` (REST) and `broker/wwwroot` (static) on `https://localhost:5443`. Its parent is
  `micronaut-parent`, not the root POM, because a POM has one parent; the root lists it as a module only.
- `examples/layer0`, `examples/layer1`: runnable echo round trips. `layer0` addresses the provider by a
  hardcoded RouteId; `layer1` claims and resolves a channel *name*. Each `Main` takes `--provider` or
  `--client`.
- `javascript/`: `@messagebus/client`, the JS client runtime (npm, no build step), for Node and the
  browser. One of the places the protocol is written, so a framing change lands here too, and
  `test/wire.test.js` pins the same literals the Java `WireTest`s pin.
- `monitoring/`: Preact + Vite + TSX app visualizing bus state. It consumes `javascript/` via a `file:`
  dependency and builds into `broker/wwwroot/monitor/` (gitignored).
- Neither npm project is in the Maven reactor.

## Realms

A realm is an isolated named partition for discovery, so several independent broker + client setups
can coexist on one machine (`Default`, `Red`, `Blue`, …). Discovery is a named pipe whose name includes
the uppercased realm, so a client only finds a broker in the realm it targets.

- **broker** reads `messagebus.realm` (`application.yml` sets `Default`). Override it with
  `MESSAGEBUS_REALM` (Micronaut's environment mapping) or `-Dmessagebus.realm=Red`.
- **runtime** reads `MESSAGEBUS_REALM` via `Realm.resolve()`, falling back to `Default`, so clients
  find the `Default` broker with no configuration.
- **javascript** (Node) reads the same variable, or a `realm` option.
- One broker per realm: the pipe allows one owner, and a second broker in the realm fails at startup.

## Build and run

Maven **3.9** with `pom.yml` (polyglot-yaml 0.8.2, registered in `.mvn/extensions.xml`; it does not
load on Maven 4). JDK **17**: no FFM, no virtual threads.

```
mvn verify                                   # all modules, all tests
mvn -pl broker test -Dtest=WireTest          # one class; -Dtest='RoutingTest#method' for one method
cd javascript && npm test
cd monitoring && npm ci && npm run build     # -> broker/wwwroot/monitor/
```

Run long-lived processes from the IDE, not from a shell: `io.minaret.broker.Application` with `broker/`
as working directory, then the examples' `Main` with `--provider` / `--client`.

### IDE import

IntelliJ's Maven importer only reads `pom.xml`, so generate one per POM (gitignored, not the source of
truth; regenerate after editing a `pom.yml`), from the root and from each module directory:

```
mvn io.takari.polyglot:polyglot-translate-plugin:translate -Dinput=pom.yml -Doutput=pom.xml
```

Child POMs name their parent with `relativePath: ../pom.yml`. Maven's default `../pom.xml` does not
exist on a clean checkout, and without the explicit path the parent is looked up remotely and fails.

### Dev certificate

The broker serves `broker/devcert.pfx` (PKCS12, password `MESSAGEBUS_CERT_PASSWORD`, default
`messagebus`; gitignored). Who needs it trusted:
- browsers (the monitor): the OS store;
- Node clients: Node ignores the OS store, so set `NODE_EXTRA_CA_CERTS` to the issuing CA's PEM;
- Java clients: no. `Channels.connect` uses `WebSocketConnection.trustingLocalhostClient()`.

To create one with mkcert:

```
mkcert -install                                  # trusts mkcert's CA in the OS store
mkcert -pkcs12 -p12-file broker/devcert.pfx localhost 127.0.0.1 ::1
# Node: NODE_EXTRA_CA_CERTS="$(mkcert -CAROOT)/rootCA.pem"
```

mkcert's PKCS12 password is `changeit`, so either set `MESSAGEBUS_CERT_PASSWORD=changeit` or re-export
the file with `messagebus`.

## Tests

- Test names are written to be read (`dispatch_providerReply_goesToTheAwaitingClient_notBackToTheProvider`),
  so read the test files rather than a summary here.
- Hand-rolled fakes (`FakeConnection`, `TestBus`), never a mocking library.
- Anything hosting a broker in-process uses a `TEST<uuid>` realm. The `Default` realm's pipe is already
  held by any broker the developer is running, and a second publisher fails at startup.
- Tests in the same package reach package-private members directly; prefer that over widening
  visibility.
