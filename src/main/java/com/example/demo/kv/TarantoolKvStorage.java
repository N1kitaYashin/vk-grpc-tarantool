package com.example.demo.kv;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.mapping.TarantoolResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class TarantoolKvStorage implements KvStorage {

    private static final String PUT_LUA = """
        local space = box.space[...]
        local key = select(2, ...)
        local value = select(3, ...)
        space:replace({key, value})
        return true
        """;

    private static final String GET_LUA = """
        local space = box.space[...]
        local key = select(2, ...)
        local tuple = space:get({key})
        if tuple == nil then
          return nil
        end
        return {tuple[1], tuple[2]}
        """;

    private static final String DELETE_LUA = """
        local space = box.space[...]
        local key = select(2, ...)
        local tuple = space:delete({key})
        return tuple ~= nil
        """;

    private static final String COUNT_LUA = """
        local space = box.space[...]
        return space:count()
        """;

    private static final String RANGE_PAGE_LUA = """
        local space = box.space[...]
        local from_key = select(2, ...)
        local to_key = select(3, ...)
        local exclusive_from = select(4, ...)
        local page_size = select(5, ...)
        local out = {}
        
        -- Вызываем pairs() прямо в конструкции for
        for _, tuple in space.index[0]:pairs({from_key}, {iterator = 'GE'}) do
          local k = tuple[1]
          if exclusive_from and k == from_key then
            goto continue
          end
          if to_key ~= nil and to_key ~= '' and k > to_key then
            break
          end
          table.insert(out, {k, tuple[2]})
          if #out >= page_size then
            break
          end
          ::continue::
        end
        return out
        """;

    private final TarantoolBoxClient client;
    private final String spaceName;
    private final int rangeBatchSize;

    public TarantoolKvStorage(TarantoolBoxClient client, String spaceName, int rangeBatchSize) {
        this.client = Objects.requireNonNull(client);
        this.spaceName = Objects.requireNonNull(spaceName);
        this.rangeBatchSize = rangeBatchSize;
    }

    @Override
    public void put(String key, byte[] value) {
        eval(PUT_LUA, Arrays.asList(spaceName, key, value));
    }

    @Override
    public KvGetResult get(String key) {
        List<?> result = callLua(GET_LUA, List.of(spaceName, key));
        Object tuple = first(result);
        if (tuple == null) {
            return new KvGetResult(false, Optional.empty());
        }
        List<?> list = (List<?>) tuple;
        return new KvGetResult(true, Optional.ofNullable(toBytes(list.get(1))));
    }

    @Override
    public boolean delete(String key) {
        Object deleted = first(eval(DELETE_LUA, List.of(spaceName, key)));
        return deleted instanceof Boolean b && b;
    }

    @Override
    public void range(String keySince, String keyTo, Consumer<KvPair> consumer) {
        String cursor = keySince;
        boolean exclusive = false;

        while (true) {
            List<?> results = callLua(RANGE_PAGE_LUA, Arrays.asList(spaceName, cursor, keyTo, exclusive, rangeBatchSize));
            Object firstResult = first(results);

            if (!(firstResult instanceof List<?> page) || page.isEmpty()) {
                break;
            }

            for (Object raw : page) {
                List<?> tuple = (List<?>) raw;
                String key = (String) tuple.get(0);
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
        Object count = first(eval(COUNT_LUA, List.of(spaceName)));
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

    private List<?> callLua(String script, List<?> args) {
        TarantoolResponse<?> response = client.eval(script, args).join();
        Object body = response.get();
        if (!(body instanceof List<?> list)) {
            return List.of(body);
        }
        return list;
    }
}
