package io.minaret.benchmark;

import java.util.Arrays;

/**
 * An append-only array of latencies in milliseconds, safe to append from any thread.
 *
 * Primitive rather than a List of Double, so recording a million samples doesn't box a million
 * objects into the heap being measured. DispatchClient appends from its router's dispatch thread and
 * from timeout threads; FanoutRecorder from every subscriber's dispatch thread.
 */
final class Latencies {

    private double[] values;
    private int count;

    Latencies(int expected) {
        values = new double[Math.max(16, expected)];
    }

    synchronized void add(double ms) {
        if (count == values.length) values = Arrays.copyOf(values, values.length * 2);
        values[count++] = ms;
    }

    synchronized int count() {
        return count;
    }

    synchronized double[] snapshot() {
        return Arrays.copyOf(values, count);
    }

}
