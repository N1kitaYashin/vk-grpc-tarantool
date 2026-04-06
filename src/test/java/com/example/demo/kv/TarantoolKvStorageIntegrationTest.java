package com.example.demo.kv;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
        storage.put("it_a", "A".getBytes(StandardCharsets.UTF_8));
        storage.put("it_b", null);
        storage.put("it_c", "C".getBytes(StandardCharsets.UTF_8));

        KvGetResult valueA = storage.get("it_a");
        assertTrue(valueA.found());
        assertArrayEquals("A".getBytes(StandardCharsets.UTF_8), valueA.value().get());

        KvGetResult valueB = storage.get("it_b");
        assertTrue(valueB.found());
        assertTrue(valueB.value().isEmpty());

        assertFalse(storage.get("it_missing").found());

        List<KvPair> rangeResults = new ArrayList<>();
        storage.range("it_a", "it_c", rangeResults::add);

        assertEquals(3, rangeResults.size());
        assertEquals("it_a", rangeResults.get(0).key());
        assertEquals("it_b", rangeResults.get(1).key());
        assertEquals("it_c", rangeResults.get(2).key());
        assertNull(rangeResults.get(1).value());

        assertTrue(storage.count() >= 3);
        assertTrue(storage.delete("it_b"));
        assertFalse(storage.delete("it_b"));
    }
}