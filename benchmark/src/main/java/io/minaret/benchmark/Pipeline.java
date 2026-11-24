package io.minaret.benchmark;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

/**
 * Runs `total` asynchronous steps with at most `width` in flight, without a thread per step.
 *
 * `width` chains each take the next index when their previous step completes, failed or not. A step
 * records its own failure; the pipeline only bounds concurrency.
 *
 * Main uses it to connect workers at `--connectConcurrency`, so a burst of thousands of TLS handshakes
 * doesn't front-load a CPU spike unrelated to steady-state routing. DispatchClient uses it to pipeline
 * `--perClientInFlight` requests per client, so the broker sees many simultaneous requests without
 * each reply gating the next send.
 */
final class Pipeline {

    private Pipeline() {
    }

    static CompletableFuture<Void> run(int total, int width, IntFunction<CompletableFuture<?>> step) {
        AtomicInteger next = new AtomicInteger();
        CompletableFuture<?>[] chains = new CompletableFuture<?>[Math.max(1, Math.min(width, total))];
        for (int i = 0; i < chains.length; i++) chains[i] = chain(next, total, step);
        return CompletableFuture.allOf(chains);
    }

    /**
     * Loops while steps are already done (a send failing synchronously on a closed socket, say), and
     * only continues asynchronously from one still pending, so a long run of synchronous steps doesn't
     * recurse one stack frame per step.
     */
    private static CompletableFuture<Void> chain(AtomicInteger next, int total, IntFunction<CompletableFuture<?>> step) {
        while (true) {
            int index = next.getAndIncrement();
            if (index >= total) return CompletableFuture.completedFuture(null);

            CompletableFuture<?> current;
            try {
                current = step.apply(index);
            } catch (RuntimeException e) {
                continue;
            }

            if (!current.isDone()) return current.handle((ignored, failure) -> null).thenCompose(ignored -> chain(next, total, step));
        }
    }

}
