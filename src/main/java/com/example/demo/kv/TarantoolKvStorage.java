package com.example.demo.kv;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.mapping.TarantoolResponse;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class TarantoolKvStorage implements KvStorage {

    private final TarantoolBoxClient client;
    private final String spaceName;
    private final int rangeBatchSize;

    public TarantoolKvStorage(TarantoolBoxClient client, String spaceName, int rangeBatchSize) {
        this.client = Objects.requireNonNull(client);
        this.spaceName = Objects.requireNonNull(spaceName);
        if (rangeBatchSize <= 0) {
            throw new IllegalArgumentException("rangeBatchSize must be greater than 0");
        }
        this.rangeBatchSize = rangeBatchSize;
    }

    @Override
    public void put(String key, byte[] value) {
        Objects.requireNonNull(key, "key must not be null");
        eval(TarantoolLuaScripts.PUT, Arrays.asList(spaceName, key, value));
    }

    @Override
    public KvGetResult get(String key) {
        Objects.requireNonNull(key, "key must not be null");
        List<?> result = eval(TarantoolLuaScripts.GET, List.of(spaceName, key));
        Object tuple = first(result);
        if (tuple == null) {
            return new KvGetResult(false, Optional.empty());
        }
        if (!(tuple instanceof List<?> list) || list.size() < 2) {
            throw new IllegalStateException("Unexpected GET response shape");
        }
        return new KvGetResult(true, Optional.ofNullable(toBytes(list.get(1))));
    }

    @Override
    public boolean delete(String key) {
        Objects.requireNonNull(key, "key must not be null");
        Object deleted = first(eval(TarantoolLuaScripts.DELETE, List.of(spaceName, key)));
        return deleted instanceof Boolean b && b;
    }

    @Override
    public void range(String keySince, String keyTo, Consumer<KvPair> consumer) {
        Objects.requireNonNull(keySince, "keySince must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        String cursor = keySince;
        boolean exclusive = false;

        while (true) {
            List<?> results = eval(TarantoolLuaScripts.RANGE_PAGE, Arrays.asList(spaceName, cursor, keyTo, exclusive, rangeBatchSize));
            Object firstResult = first(results);

            if (!(firstResult instanceof List<?> page) || page.isEmpty()) {
                break;
            }

            for (Object raw : page) {
                if (!(raw instanceof List<?> tuple) || tuple.size() < 2 || !(tuple.get(0) instanceof String key)) {
                    throw new IllegalStateException("Unexpected RANGE response shape");
                }
                consumer.accept(new KvPair(key, toBytes(tuple.get(1))));
                cursor = key;
            }

            if (page.size() < rangeBatchSize) {
                break;
            }
            exclusive = true;
        }
    }

    @Override
    public long count() {
        Object count = first(eval(TarantoolLuaScripts.COUNT, List.of(spaceName)));
        if (count instanceof Number n) {
            return n.longValue();
        }
        throw new IllegalStateException("Unexpected COUNT response shape");
    }

    private List<?> eval(String script, List<?> args) {
        CompletableFuture<?> future = client.eval(script, args);
        Object response = future.join();
        if (!(response instanceof TarantoolResponse<?> tarantoolResponse)) {
            throw new IllegalStateException("Unexpected Tarantool response type");
        }
        Object body = tarantoolResponse.get();
        if (!(body instanceof List<?> list)) {
            return List.of(body);
        }
        return list;
    }

    private static Object first(List<?> list) {
        return list.isEmpty() ? null : list.get(0);
    }

    private static byte[] toBytes(Object rawValue) {
        return switch (rawValue) {
            case null -> null;
            case byte[] bytes -> bytes;
            case String str -> str.getBytes(StandardCharsets.UTF_8);
            default -> throw new IllegalStateException("Unsupported value type: " + rawValue.getClass().getName());
        };
    }
}
