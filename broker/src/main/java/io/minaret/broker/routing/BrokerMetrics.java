package io.minaret.broker.routing;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Singleton;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Broker's instruments, over Micrometer. No exporter is configured here: adding a
 * micronaut-micrometer-registry-* module is what publishes them.
 *
 * Every drop reason has its own pre-built Counter (Drop), so counting a drop on the hot path is one
 * increment with no tag lookup and no Meter.Id allocation.
 */
@Singleton
public final class BrokerMetrics {

    /** The broker.messages.dropped reasons. */
    public enum Drop {
        MALFORMED("malformed"),
        UNKNOWN_TYPE("unknown-type"),
        EXPIRED("expired"),
        UNKNOWN_ROUTE("unknown-route"),
        UNKNOWN_TOPIC("unknown-topic"),
        CORRELATION_IN_USE("correlation-in-use"),
        NOT_PROVIDER("not-provider"),
        NO_RECIPIENT("no-recipient"),
        UNREGISTERED_SENDER("unregistered-sender"),
        QUEUE_FULL("queue-full"),
        QUEUE_CLOSED("queue-closed"),
        SEND_FAILED("send-failed");

        final String tag;

        Drop(String tag) {
            this.tag = tag;
        }
    }

    private final Counter routed;
    private final Counter[] dropped = new Counter[Drop.values().length];
    private final Timer dispatch;
    private final AtomicInteger active = new AtomicInteger();

    public BrokerMetrics(MeterRegistry registry) {
        routed = Counter.builder("broker.messages.routed").baseUnit("{message}").register(registry);
        for (Drop drop : Drop.values())
            dropped[drop.ordinal()] = Counter.builder("broker.messages.dropped").baseUnit("{message}").tag("reason", drop.tag).register(registry);
        dispatch = Timer.builder("broker.dispatch.duration").register(registry);
        registry.gauge("broker.connections.active", active);
    }

    public void routed() {
        routed.increment();
    }

    public void dropped(Drop reason) {
        dropped[reason.ordinal()].increment();
    }

    public void dispatchDuration(long nanos) {
        dispatch.record(nanos, TimeUnit.NANOSECONDS);
    }

    public void connectionRegistered() {
        active.incrementAndGet();
    }

    public void connectionUnregistered() {
        active.decrementAndGet();
    }

}
