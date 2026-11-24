package io.minaret.benchmark;

import io.minaret.messaging.Destination;
import io.minaret.messaging.Header;
import io.minaret.messaging.Router;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One simulated client in the dispatch benchmark: sends requests to random providers and times each
 * round trip.
 *
 * Main builds one per `--clients`, and each keeps `--perClientInFlight` requests pipelined, so with a
 * thousand clients the broker holds a large number of simultaneous requests. A reply is matched to its
 * request by the payload id through one map lookup.
 */
final class DispatchClient {

    private final Router router;
    private final List<UUID> providers;
    private final String filler;
    private final Options options;
    private final Latencies latencies;
    private final AtomicInteger timeouts;
    private final ConcurrentHashMap<UUID, CompletableFuture<Long>> pending = new ConcurrentHashMap<>();

    DispatchClient(Router router, List<UUID> providers, String filler, Options options, Latencies latencies, AtomicInteger timeouts) {
        this.router = router;
        this.providers = providers;
        this.filler = filler;
        this.options = options;
        this.latencies = latencies;
        this.timeouts = timeouts;

        router.handleMessage().add(message -> {
            if (!DispatchProvider.REPLY.equals(message.header().type())) return;

            long receivedAt = System.nanoTime();
            CompletableFuture<Long> reply = pending.remove(Payload.idOf(message.body()));
            if (reply != null) reply.complete(receivedAt);
        });
    }

    CompletableFuture<Void> run() {
        return Pipeline.run(options.messagesPerClient(), options.perClientInFlight(), ignored -> sendOne());
    }

    private CompletableFuture<?> sendOne() {
        UUID provider = providers.get(ThreadLocalRandom.current().nextInt(providers.size()));
        UUID id = UUID.randomUUID();
        CompletableFuture<Long> reply = new CompletableFuture<>();
        pending.put(id, reply);

        long sentAt = System.nanoTime();
        return router.sendMessage(Header.of(DispatchProvider.REQUEST, Destination.ofId(provider)), new Payload(id, filler))
                .thenCompose(ignored -> reply.orTimeout(options.replyTimeout().toMillis(), TimeUnit.MILLISECONDS))
                .handle((receivedAt, failure) -> {
                    if (failure == null) {
                        latencies.add((receivedAt - sentAt) / 1e6);
                    } else {
                        pending.remove(id);
                        timeouts.incrementAndGet();
                    }
                    return null;
                });
    }

}
