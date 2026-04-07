package com.example.demo.kv;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class TarantoolKvStorageIntegrationTest {

    @Container
    private static final GenericContainer<?> TARANTOOL = new GenericContainer<>("tarantool/tarantool:3.2")
            .withExposedPorts(3301)
            .withCopyFileToContainer(
                    MountableFile.forHostPath(testInitLuaPath()),
                    "/opt/tarantool/init.lua"
            )
            .withCommand("tarantool", "/opt/tarantool/init.lua");

    private static TarantoolBoxClient client;
    private static TarantoolKvStorage storage;

    @BeforeAll
    static void setUp() throws Exception {
        client = TarantoolFactory.box()
                .withHost(TARANTOOL.getHost())
                .withPort(TARANTOOL.getMappedPort(3301))
                .withUser("guest")
                .build();
        storage = new TarantoolKvStorage(client, "KV", 2);
    }

    private static Path testInitLuaPath() {
        Path path = Paths.get("tarantool", "init.lua").toAbsolutePath();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Expected init.lua at " + path);
        }
        return path;
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void shouldSupportPutGetNullDeleteRangeAndCount() {
        String prefix = "it_basic_" + UUID.randomUUID() + "_";
        String keyA = prefix + "a";
        String keyB = prefix + "b";
        String keyC = prefix + "c";

        storage.put(keyA, "A".getBytes(StandardCharsets.UTF_8));
        storage.put(keyB, null);
        storage.put(keyC, "C".getBytes(StandardCharsets.UTF_8));

        KvGetResult valueA = storage.get(keyA);
        assertTrue(valueA.found());
        assertArrayEquals("A".getBytes(StandardCharsets.UTF_8), valueA.value().get());

        KvGetResult valueB = storage.get(keyB);
        assertTrue(valueB.found());
        assertTrue(valueB.value().isEmpty());

        assertFalse(storage.get(prefix + "missing").found());

        List<KvPair> rangeResults = new ArrayList<>();
        storage.range(keyA, keyC, rangeResults::add);

        assertEquals(3, rangeResults.size());
        assertEquals(keyA, rangeResults.get(0).key());
        assertEquals(keyB, rangeResults.get(1).key());
        assertEquals(keyC, rangeResults.get(2).key());
        assertNull(rangeResults.get(1).value());

        assertTrue(storage.count() >= 3);
        assertTrue(storage.delete(keyB));
        assertFalse(storage.delete(keyB));
    }

    @Test
    void shouldPageRangeAcrossMultipleBatchesAndRespectUpperBound() {
        String prefix = "it_page_" + UUID.randomUUID() + "_";
        for (int i = 0; i < 10; i++) {
            String key = prefix + String.format("%02d", i);
            storage.put(key, ("v" + i).getBytes(StandardCharsets.UTF_8));
        }

        String from = prefix + "02";
        String to = prefix + "08";

        List<KvPair> rangeResults = new ArrayList<>();
        storage.range(from, to, rangeResults::add);

        assertEquals(7, rangeResults.size());
        for (int i = 0; i < 7; i++) {
            String expectedKey = prefix + String.format("%02d", i + 2);
            assertEquals(expectedKey, rangeResults.get(i).key());
            assertEquals("v" + (i + 2), new String(rangeResults.get(i).value(), StandardCharsets.UTF_8));
        }
    }
}