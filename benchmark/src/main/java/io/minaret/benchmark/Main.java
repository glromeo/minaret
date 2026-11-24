package io.minaret.benchmark;

import io.minaret.discovery.Realm;
import io.minaret.messaging.ChannelProvider;
import io.minaret.messaging.Channels;
import io.minaret.messaging.Router;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

/**
 * Load generator for a live broker, in one of two modes that measure different broker paths and do
 * not compare (see Options.Mode).
 *
 * - dispatch: `--clients` clients each send `--messagesPerClient` requests to random providers among
 *   `--providers`, `--perClientInFlight` at a time, and time each round trip.
 * - fanout: one provider publishes `--publishes` messages on one topic to `--subscribers`
 *   subscribers, and every delivery is timed from its publish.
 *
 * Every worker is a full client (its own Router and socket) in this one process, so the benchmark
 * competes with the broker for the machine: its numbers are for A/B comparison, not capacity.
 */
public final class Main {

    private static final Logger LOG = System.getLogger(Main.class.getName());

    private static final String FANOUT_CHANNEL = "BENCHMARK_FANOUT";
    private static final String FANOUT_TOPIC = "ticks";

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        String realm = Realm.resolve();

        LOG.log(Level.INFO, "Discovering broker in realm {0}...", realm);
        Connector connector = Connector.discover(realm, options.httpClients());
        LOG.log(Level.INFO, "Broker found at {0}", connector.broker());

        List<Router> routers = new ArrayList<>();
        try {
            Report report = options.mode() == Options.Mode.FANOUT
                    ? fanout(options, connector, routers)
                    : dispatch(options, connector, routers);
            if (report != null) publish(report);
        } finally {
            routers.forEach(Router::close);
        }
    }

    private static Report dispatch(Options options, Connector connector, List<Router> routers) {
        LOG.log(Level.INFO, "Connecting {0} providers...", options.providers());
        List<Router> providers = connectAll(options.providers(), options.connectConcurrency(), connector, i -> "benchmark-provider-" + i);
        routers.addAll(providers);
        providers.forEach(DispatchProvider::serve);
        List<UUID> providerIds = providers.stream().map(Router::routeId).toList();

        LOG.log(Level.INFO, "Connecting {0} clients...", options.clients());
        List<Router> clientRouters = connectAll(options.clients(), options.connectConcurrency(), connector, i -> "benchmark-client-" + i);
        routers.addAll(clientRouters);

        Latencies latencies = new Latencies(options.clients() * options.messagesPerClient());
        AtomicInteger timeouts = new AtomicInteger();
        String filler = Payload.filler(options.payloadBytes());
        List<DispatchClient> clients = clientRouters.stream()
                .map(router -> new DispatchClient(router, providerIds, filler, options, latencies, timeouts))
                .toList();

        LOG.log(Level.INFO, "Sending {0} messages ({1} clients x {2} each)...",
                (long) options.clients() * options.messagesPerClient(), options.clients(), options.messagesPerClient());

        long start = System.nanoTime();
        CompletableFuture.allOf(clients.stream().map(DispatchClient::run).toArray(CompletableFuture[]::new)).join();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        if (timeouts.get() > 0) LOG.log(Level.WARNING, "{0} requests got no reply within {1}", timeouts.get(), options.replyTimeout());
        return report(Options.Mode.DISPATCH, latencies, elapsed);
    }

    private static Report fanout(Options options, Connector connector, List<Router> routers) throws InterruptedException {
        int expected = options.publishes() * options.subscribers();
        FanoutRecorder recorder = new FanoutRecorder(expected);

        LOG.log(Level.INFO, "Connecting fanout publisher on channel {0}...", FANOUT_CHANNEL);
        Router publisherRouter = connector.connect("benchmark-fanout-publisher").join();
        routers.add(publisherRouter);
        ChannelProvider publisher = Channels.over(publisherRouter).createChannel(FANOUT_CHANNEL).join();
        // Registers the topic so subscribers find it by name, and caches its id so every publish
        // addresses it by id instead of paying a path resolution on the first.
        publisher.subscribe(FANOUT_TOPIC, (header, body) -> { }).join();

        LOG.log(Level.INFO, "Connecting {0} subscribers...", options.subscribers());
        AtomicInteger connected = new AtomicInteger();
        Pipeline.run(options.subscribers(), options.connectConcurrency(), i -> connector.connect("benchmark-fanout-subscriber-" + i)
                .thenCompose(router -> {
                    synchronized (routers) {
                        routers.add(router);
                    }
                    return Channels.over(router).connectChannel(FANOUT_CHANNEL);
                })
                .thenCompose(client -> client.subscribe(FANOUT_TOPIC, (header, body) -> recorder.delivered(Payload.idOf(body), System.nanoTime())))
                .whenComplete((ignored, failure) -> {
                    if (failure == null) connected.incrementAndGet();
                    else LOG.log(Level.ERROR, "Subscriber " + i + " failed to connect", failure);
                })).join();
        requireAll("subscribers", connected.get(), options.subscribers());

        String filler = Payload.filler(options.payloadBytes());
        LOG.log(Level.INFO, "Publishing {0} messages to {1} subscribers ({2} deliveries)...",
                options.publishes(), options.subscribers(), expected);

        long start = System.nanoTime();
        for (int i = 0; i < options.publishes(); i++) {
            UUID id = UUID.randomUUID();
            recorder.published(id, System.nanoTime());
            publisher.publish(FANOUT_TOPIC, new Payload(id, filler)).join();
        }

        // The ack says the broker accepted the publish, not that every subscriber received it: stopping
        // the clock at the last ack would report a throughput the subscribers never saw.
        drain(recorder.latencies(), expected, options.replyTimeout());
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        return report(Options.Mode.FANOUT, recorder.latencies(), elapsed);
    }

    /**
     * Waits until every expected delivery has landed or progress stalls for `timeout`. Polling instead
     * of a signal per delivery keeps each subscriber's handler down to one map read and one append.
     */
    private static void drain(Latencies latencies, int expected, Duration timeout) throws InterruptedException {
        int last = -1;
        long lastProgress = System.nanoTime();

        while (latencies.count() < expected) {
            int count = latencies.count();
            if (count != last) {
                last = count;
                lastProgress = System.nanoTime();
            } else if (System.nanoTime() - lastProgress > timeout.toNanos()) {
                LOG.log(Level.WARNING, "Deliveries stalled at {0} of {1}; giving up after {2}", count, expected, timeout);
                return;
            }
            Thread.sleep(25);
        }
    }

    /** Connects `count` routers at `concurrency`, failing the run if any of them fails. */
    private static List<Router> connectAll(int count, int concurrency, Connector connector, IntFunction<String> identity) {
        Router[] routers = new Router[count];
        Pipeline.run(count, concurrency, i -> connector.connect(identity.apply(i))
                .whenComplete((router, failure) -> {
                    if (failure == null) routers[i] = router;
                    else LOG.log(Level.ERROR, "Worker " + identity.apply(i) + " failed to connect", failure);
                })).join();

        List<Router> connected = new ArrayList<>(count);
        for (Router router : routers) if (router != null) connected.add(router);
        requireAll("workers", connected.size(), count);
        return connected;
    }

    private static void requireAll(String what, int connected, int expected) {
        if (connected < expected) throw new IllegalStateException(connected + " of " + expected + " " + what + " connected");
    }

    private static Report report(Options.Mode mode, Latencies latencies, Duration elapsed) {
        double[] samples = latencies.snapshot();
        if (samples.length == 0) {
            LOG.log(Level.ERROR, "Nothing was received; no report.");
            return null;
        }
        return Report.of(mode, samples, elapsed);
    }

    /**
     * Prints the report, then appends it to `BENCHMARK_RESULTS` (default: benchmark-results.csv in the
     * working directory). The report is already on stdout by then, so a failed append only warns.
     */
    private static void publish(Report report) {
        report.print(System.out);

        String configured = System.getenv("BENCHMARK_RESULTS");
        Path path = Path.of(configured == null || configured.isBlank() ? "benchmark-results.csv" : configured).toAbsolutePath();
        try {
            report.appendTo(path);
            LOG.log(Level.INFO, "Appended results to {0}", path);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Could not append results to " + path + "; the report above is unaffected", e);
        }
    }

}
