package com.example.demo.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class InMemoryKvStorageTest {

    @Test
    void supportsNullAndOverwrite() {
        InMemoryKvStorage storage = new InMemoryKvStorage();

        storage.put("alpha", null);
        KvGetResult resultNull = storage.get("alpha");
        assertTrue(resultNull.found());
        assertTrue(resultNull.value().isEmpty());

        byte[] payload = "data".getBytes(StandardCharsets.UTF_8);
        storage.put("alpha", payload);
        KvGetResult resultData = storage.get("alpha");
        assertArrayEquals(payload, resultData.value().get());
    }

    @Test
    void supportsRangeCountAndDelete() {
        InMemoryKvStorage storage = new InMemoryKvStorage();
        storage.put("a", "1".getBytes(StandardCharsets.UTF_8));
        storage.put("b", null);
        storage.put("c", "3".getBytes(StandardCharsets.UTF_8));

        List<KvPair> range = new ArrayList<>();
        storage.range("a", "c", range::add);

        assertEquals(3, range.size());
        assertEquals("a", range.get(0).key());
        assertEquals("b", range.get(1).key());
        assertEquals("c", range.get(2).key());
        assertTrue(range.get(1).value() == null);

        assertEquals(3, storage.count());
        assertTrue(storage.delete("b"));
        assertFalse(storage.delete("z"));
        assertEquals(2, storage.count());
    }

    @Test
    void rangeIsInclusiveAndOrdered() {
        InMemoryKvStorage storage = new InMemoryKvStorage();
        storage.put("a", "A".getBytes(StandardCharsets.UTF_8));
        storage.put("b", "B".getBytes(StandardCharsets.UTF_8));
        storage.put("c", "C".getBytes(StandardCharsets.UTF_8));
        storage.put("d", "D".getBytes(StandardCharsets.UTF_8));

        List<KvPair> range = new ArrayList<>();
        storage.range("b", "c", range::add);

        assertEquals(2, range.size());
        assertEquals("b", range.get(0).key());
        assertEquals("c", range.get(1).key());
    }

    @Test
    void getMissingKeyReturnsNotFound() {
        InMemoryKvStorage storage = new InMemoryKvStorage();
        KvGetResult result = storage.get("missing");

        assertFalse(result.found());
        assertTrue(result.value().isEmpty());
    }
}