package io.minaret.broker.wire;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * UuidTable against HashMap as the oracle. Backward-shift deletion is where an open-addressing
 * table goes subtly wrong (an entry stranded past a hole is never found again), so the keys are
 * drawn from a small pool to force long collision runs, and every key is checked after every step.
 */
class UuidTableTest {

    @Test
    void randomOperations_agreeWithHashMap() {
        Random random = new Random(42);
        List<Uuid> pool = new ArrayList<>();
        for (int i = 0; i < 200; i++) pool.add(new Uuid(random.nextLong(), random.nextLong()));

        UuidTable<Integer> table = new UuidTable<>();
        Map<Uuid, Integer> oracle = new HashMap<>();

        for (int step = 0; step < 20_000; step++) {
            Uuid key = pool.get(random.nextInt(pool.size()));
            int op = random.nextInt(10);
            if (op < 4) assertEquals(oracle.put(key, step), table.put(key, step));
            else if (op < 6) assertEquals(oracle.putIfAbsent(key, step), table.putIfAbsent(key, step));
            else if (op < 9) assertEquals(oracle.remove(key), table.remove(key));
            else {
                Integer value = oracle.get(key);
                if (value != null) {
                    oracle.values().removeIf(value::equals);
                    table.removeValue(value);
                }
            }

            assertEquals(oracle.size(), table.size());
            for (Uuid probe : pool) assertEquals(oracle.get(probe), table.get(probe));
        }
    }

    @Test
    void removeValue_dropsEveryEntryHoldingIt() {
        UuidTable<String> table = new UuidTable<>();
        for (int i = 0; i < 100; i++) table.put(new Uuid(i, i), i % 3 == 0 ? "gone" : "kept" + i);

        table.removeValue("gone");

        for (int i = 0; i < 100; i++) assertEquals(i % 3 == 0 ? null : "kept" + i, table.get(new Uuid(i, i)));
        assertEquals(66, table.size());
    }

    @Test
    void copy_isIndependentOfTheOriginal() {
        UuidTable<String> original = new UuidTable<>();
        Uuid key = new Uuid(1, 2);
        original.put(key, "a");

        UuidTable<String> copy = original.copy();
        copy.put(key, "b");
        copy.put(new Uuid(3, 4), "c");

        assertEquals("a", original.get(key));
        assertNull(original.get(new Uuid(3, 4)));
        assertEquals("b", copy.get(key));
    }

    @Test
    void get_withAProbe_findsTheStoredEntry() {
        UuidTable<String> table = new UuidTable<>();
        String value = "v";
        table.put(new Uuid(7, 8), value);

        Uuid probe = new Uuid();
        assertSame(value, table.get(probe.set(7, 8)));
        assertNull(table.get(probe.set(8, 7)));
    }

}
