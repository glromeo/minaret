package io.minaret.benchmark;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pairs every fanout delivery with the publish that produced it, across all subscribers.
 *
 * One publish produces `--subscribers` deliveries, so there is no one-to-one reply to hang a future
 * off. Main records each publish's send time by payload id; every subscriber records its receive time
 * against that id.
 */
final class FanoutRecorder {

    private final ConcurrentHashMap<UUID, Long> publishedAt = new ConcurrentHashMap<>();
    private final Latencies latencies;

    FanoutRecorder(int expectedDeliveries) {
        latencies = new Latencies(expectedDeliveries);
    }

    /** Called before the publish is sent, so a delivery racing back ahead of the ack still finds it. */
    void published(UUID id, long nanos) {
        publishedAt.put(id, nanos);
    }

    void delivered(UUID id, long nanos) {
        // A delivery with no publish time can only be a late frame from an earlier run on a connection
        // still subscribed: ignored rather than skewing the numbers.
        Long sentAt = publishedAt.get(id);
        if (sentAt != null) latencies.add((nanos - sentAt) / 1e6);
    }

    Latencies latencies() {
        return latencies;
    }

}
