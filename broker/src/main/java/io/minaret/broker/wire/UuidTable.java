package io.minaret.broker.wire;

/**
 * An open-addressing hash table keyed by a 128-bit id held as two longs: parallel msb/lsb/value
 * arrays, linear probing, backward-shift deletion (so no tombstones). A lookup reads three arrays
 * and makes no call; adding or removing an entry allocates nothing until the table grows.
 *
 * Not thread-safe. The owners make it so in one of two ways:
 * - copy-on-write behind a volatile reference (ConnectionRegistry's routes, ChannelRegistry's
 *   topics), written rarely under a lock, read lock-free on every frame. A reader sees a whole
 *   snapshot because the copy is complete before it is published.
 * - lock-striped (ReplyTable), for correlations claimed and consumed on different event loops.
 */
public final class UuidTable<V> {

    private long[] msbs;
    private long[] lsbs;
    private Object[] values;
    private int mask;
    private int size;

    public UuidTable() {
        this(16);
    }

    private UuidTable(int capacity) {
        msbs = new long[capacity];
        lsbs = new long[capacity];
        values = new Object[capacity];
        mask = capacity - 1;
    }

    /** The mixed hash. Low bits pick the slot; ReplyTable picks its stripe from the top bits. */
    public static long hash(long msb, long lsb) {
        long h = msb * 0x9E3779B97F4A7C15L + lsb;
        h ^= h >>> 32;
        h *= 0xD6E8FEB86659FD93L;
        return h ^ (h >>> 32);
    }

    public V get(Uuid key) {
        return key == null ? null : get(key.msb, key.lsb);
    }

    @SuppressWarnings("unchecked")
    public V get(long msb, long lsb) {
        for (int i = (int) hash(msb, lsb) & mask; ; i = (i + 1) & mask) {
            Object value = values[i];
            if (value == null) return null;
            if (msbs[i] == msb && lsbs[i] == lsb) return (V) value;
        }
    }

    /** Stores value under key, answering the previous value or null. key is read, never kept. */
    public V put(Uuid key, V value) {
        return insert(key.msb, key.lsb, value, true);
    }

    /** Stores value only when key is absent, answering the value already there or null. */
    public V putIfAbsent(Uuid key, V value) {
        return insert(key.msb, key.lsb, value, false);
    }

    @SuppressWarnings("unchecked")
    public V remove(Uuid key) {
        for (int i = (int) hash(key.msb, key.lsb) & mask; ; i = (i + 1) & mask) {
            Object value = values[i];
            if (value == null) return null;
            if (msbs[i] == key.msb && lsbs[i] == key.lsb) {
                delete(i);
                return (V) value;
            }
        }
    }

    /** Removes every entry whose value equals value. */
    public void removeValue(Object value) {
        for (int i = 0; i < values.length; )
            if (value.equals(values[i])) delete(i); // a later entry may shift into i: look again
            else i++;
    }

    public int size() {
        return size;
    }

    /** Slot count, for iterating with valueAt without an iterator. */
    public int capacity() {
        return values.length;
    }

    /** The value in a slot, or null for an empty one. */
    @SuppressWarnings("unchecked")
    public V valueAt(int slot) {
        return (V) values[slot];
    }

    /** An independent copy, for copy-on-write owners. */
    public UuidTable<V> copy() {
        UuidTable<V> copy = new UuidTable<>(values.length);
        System.arraycopy(msbs, 0, copy.msbs, 0, msbs.length);
        System.arraycopy(lsbs, 0, copy.lsbs, 0, lsbs.length);
        System.arraycopy(values, 0, copy.values, 0, values.length);
        copy.size = size;
        return copy;
    }

    @SuppressWarnings("unchecked")
    private V insert(long msb, long lsb, V value, boolean replace) {
        if (value == null) throw new IllegalArgumentException("null values mark empty slots");
        if ((size + 1) * 2 > values.length) grow();

        int i = (int) hash(msb, lsb) & mask;
        for (; values[i] != null; i = (i + 1) & mask)
            if (msbs[i] == msb && lsbs[i] == lsb) {
                Object previous = values[i];
                if (replace) values[i] = value;
                return (V) previous;
            }

        msbs[i] = msb;
        lsbs[i] = lsb;
        values[i] = value;
        size++;
        return null;
    }

    /** Empties slot i, then pulls back every later entry of the run that its home slot allows. */
    private void delete(int i) {
        size--;
        for (int j = i; ; ) {
            j = (j + 1) & mask;
            if (values[j] == null) break;
            int home = (int) hash(msbs[j], lsbs[j]) & mask;
            // j's entry may move to i only if its home is not cyclically within (i, j].
            if (i <= j ? i < home && home <= j : i < home || home <= j) continue;
            msbs[i] = msbs[j];
            lsbs[i] = lsbs[j];
            values[i] = values[j];
            i = j;
        }
        values[i] = null;
    }

    private void grow() {
        long[] oldMsbs = msbs;
        long[] oldLsbs = lsbs;
        Object[] oldValues = values;
        int capacity = oldValues.length * 2;
        msbs = new long[capacity];
        lsbs = new long[capacity];
        values = new Object[capacity];
        mask = capacity - 1;
        size = 0;
        for (int i = 0; i < oldValues.length; i++)
            if (oldValues[i] != null) insertFresh(oldMsbs[i], oldLsbs[i], oldValues[i]);
    }

    private void insertFresh(long msb, long lsb, Object value) {
        int i = (int) hash(msb, lsb) & mask;
        while (values[i] != null) i = (i + 1) & mask;
        msbs[i] = msb;
        lsbs[i] = lsb;
        values[i] = value;
        size++;
    }

}
