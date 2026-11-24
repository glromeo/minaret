# Monitoring

A Preact + Vite + TSX app that visualizes the state of the bus, served by the Broker.

## Running

```
npm ci
npm run build     # -> broker/wwwroot/monitor/, served at https://localhost:5443/monitor/
npm run dev       # Vite dev server, /msgbus proxied to the Broker
```

Both modes need a broker running (`io.minaret.broker.Application`, from the IDE). The build output is
gitignored — a fresh clone has no `/monitor/` until someone runs `npm run build`.

`npm run dev` proxies `/msgbus` to `wss://localhost:5443`, so the page's WebSocket URL is
same-origin in dev and production alike and needs no build-time switch. If the socket fails with a
TLS error, the broker's dev certificate is not trusted — see the root `CLAUDE.md` "Dev certificate".

## Layout

- `src/graph/` — `store.ts` holds the observed bus state as signals; `simulation.ts` is the force
  layout. Both are DOM-free, which is what let them be driven headlessly against a live Broker.
- `src/components/` — the SVG canvas.

The wire protocol is not here. It lives in `@messagebus/client` (`javascript/`, pulled in as
a `file:` dependency), which this app consumes the way an `examples/` module consumes `runtime`.
`npm install` after a fresh clone links it; nothing needs building first, since the package ships its
source.

## Signals over hooks

Bus state is written from a *WebSocket callback*, not from a component. A hook-based store would
have to route that write back through a setter owned by some component's render cycle; a signal is
written directly from the event handler. It also lets the force simulation run as a plain
`requestAnimationFrame` loop with no React lifecycle involved — positions are mutated in place and
a single `tick` signal is what the canvas re-renders on, rather than 60 state updates a second.
