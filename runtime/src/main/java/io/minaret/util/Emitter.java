package io.minaret.util;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * A multicast callback list.
 *
 * Backed by a copy-on-write list so emit iterates a snapshot. That is not a general concurrency
 * nicety: every request/ack helper in this runtime removes its own correlator from inside the emit
 * that resolved it, so an emit that iterated the live list would hit a concurrent modification on the
 * ordinary path rather than the exotic one.
 */
public final class Emitter<T> {

    private final List<Consumer<T>> handlers = new CopyOnWriteArrayList<>();

    public void add(Consumer<T> handler) {
        handlers.add(handler);
    }

    public void remove(Consumer<T> handler) {
        handlers.remove(handler);
    }

    public boolean isEmpty() {
        return handlers.isEmpty();
    }

    /**
     * Invokes every handler. One handler throwing must not stop the rest — a subscriber's bug is not a
     * reason to strand every other subscriber of the same frame — so failures are collected and the
     * first is rethrown once the round is complete.
     */
    public void emit(T value) {
        RuntimeException failure = null;

        for (Consumer<T> handler : handlers) {
            try {
                handler.accept(value);
            } catch (RuntimeException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            }
        }

        if (failure != null) throw failure;
    }

}
