package io.minaret.benchmark;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineTest {

    @Test
    void run_neverHasMoreThanWidthInFlight_andRunsEveryIndexOnce() {
        List<CompletableFuture<Void>> pending = new ArrayList<>();
        ConcurrentHashMap.KeySetView<Integer, Boolean> seen = ConcurrentHashMap.newKeySet();

        CompletableFuture<Void> done = Pipeline.run(10, 3, i -> {
            assertTrue(seen.add(i), "index " + i + " ran twice");
            CompletableFuture<Void> step = new CompletableFuture<>();
            synchronized (pending) {
                pending.add(step);
            }
            return step;
        });

        while (!done.isDone()) {
            CompletableFuture<Void> next;
            synchronized (pending) {
                assertTrue(pending.size() <= 3, pending.size() + " in flight");
                next = pending.remove(0);
            }
            next.complete(null);
        }

        assertEquals(10, seen.size());
    }

    @Test
    void run_continuesPastFailedAndThrowingSteps() {
        AtomicInteger ran = new AtomicInteger();

        Pipeline.run(6, 2, i -> {
            ran.incrementAndGet();
            if (i % 3 == 0) throw new IllegalStateException("thrown");
            return CompletableFuture.failedFuture(new IllegalStateException("failed"));
        }).orTimeout(5, TimeUnit.SECONDS).join();

        assertEquals(6, ran.get());
    }

    @Test
    void run_survivesALongRunOfSynchronouslyCompletedSteps() {
        CompletableFuture<Void> done = Pipeline.run(1_000_000, 1, i -> CompletableFuture.completedFuture(null));

        assertTrue(done.isDone());
        assertFalse(done.isCompletedExceptionally());
    }

}
