package io.minaret.broker.wire;

import java.util.UUID;

/**
 * A 128-bit id held as two longs: every RouteId, TopicId and correlation id the Broker touches.
 *
 * Mutable on purpose, and that is the whole trick. The hot path looks ids up in ConcurrentHashMap
 * tables keyed by Uuid, and a lookup needs a key object: Session parses each frame's ids into probe
 * instances it owns and reuses (see Frame), so routing a frame allocates no key. Instances that are
 * *stored* — as a map key, a Topic id, a ClientInfo's RouteId — are made with copy() or the
 * factories and never mutated afterwards; only probes are ever set().
 *
 * equals/hashCode are by value and the class is final, so a probe and a stored key compare equal
 * across that divide.
 */
public final class Uuid {

    long msb;
    long lsb;

    public Uuid() {
    }

    public Uuid(long msb, long lsb) {
        this.msb = msb;
        this.lsb = lsb;
    }

    public static Uuid of(UUID value) {
        return value == null ? null : new Uuid(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    public long msb() {
        return msb;
    }

    public long lsb() {
        return lsb;
    }

    public Uuid set(long msb, long lsb) {
        this.msb = msb;
        this.lsb = lsb;
        return this;
    }

    public Uuid set(Uuid other) {
        return set(other.msb, other.lsb);
    }

    /** An immutable-by-convention copy, for storing a probe's value. */
    public Uuid copy() {
        return new Uuid(msb, lsb);
    }

    public UUID toUUID() {
        return new UUID(msb, lsb);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Uuid other && msb == other.msb && lsb == other.lsb;
    }

    @Override
    public int hashCode() {
        long h = msb ^ lsb;
        return (int) (h ^ (h >>> 32));
    }

    @Override
    public String toString() {
        return toUUID().toString();
    }

}
