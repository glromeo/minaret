# CLAUDE.md: benchmark

Load generator, run against a **live** broker: start `io.minaret.broker.Application` from the IDE
first, then `io.minaret.benchmark.Main` with `--name=value` arguments (all listed in `Options`). The
two modes measure different broker paths and are **not comparable**:

- `--mode=dispatch` (default): `--clients` clients pipeline `--perClientInFlight` requests each to
  random providers among `--providers`, through `Session.forwardToRoute`. It never touches a topic. Its
  "messages" are round trips.
- `--mode=fanout`: one provider publishes `--publishes` messages on one topic to `--subscribers`,
  through `Session.handlePublish` and the topic's subscriber list. Its "messages" are deliveries. Never
  quote a fanout number without its `--subscribers`: at low counts the path is socket-bound and
  broker-side changes are invisible. In the C# measurements they surfaced only at 2000 subscribers.

Results print to stdout and append a row to `BENCHMARK_RESULTS` (default `benchmark-results.csv` in the
working directory, gitignored). The columns match the C# benchmark's CSV plus `mode`.

## What a run costs the machine

Every worker is a full client in this process, so the defaults (1000 + 1000) cost:
- 2000 `Router`s, each with its own dispatch thread. This is the runtime's design (in-order inbound per
  client), so the thread count is part of what's measured, not an artifact to engineer around.
- `--httpClients` `HttpClient`s (default: one per core), shared round-robin, each with one selector
  thread. `Connector` explains why neither one per worker nor one for all.
- A connect at `--connectConcurrency` (default 50), so thousands of TLS handshakes don't front-load a
  CPU spike unrelated to routing.

The benchmark shares the machine with the broker it measures, so its numbers are a floor for A/B
comparison on the same machine, not a capacity figure. Discard the first run (JIT warm-up on both
sides). Compare `min` latency as well as throughput: the least contended sample tracks the broker's
own per-message work more directly than p50, which send queueing dominates at scale.

## Identities

Each worker connects as `benchmark-<role>-<i>`. Distinct identities are required: the default is the
main class, shared by every worker, and the broker's newer-wins rule would leave one connected. The
broker never forgets a route (copy-on-write table, see `broker/CLAUDE.md`), but a rerun reuses the same
identities and so the same RouteIds, so repeated runs don't grow it.
