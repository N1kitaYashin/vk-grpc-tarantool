package com.example.demo.kv;

import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

public class InMemoryKvStorage implements KvStorage {

    private record ValueHolder(boolean isNull, byte[] bytes) {
    }

    private final ConcurrentNavigableMap<String, ValueHolder> data = new ConcurrentSkipListMap<>();

    @Override
    public void put(String key, byte[] value) {
        ValueHolder holder = value == null
                ? new ValueHolder(true, null)
                : new ValueHolder(false, value.clone());
        data.put(key, holder);
    }

    @Override
    public KvGetResult get(String key) {
        ValueHolder holder = data.get(key);

        if (holder == null) {
            return new KvGetResult(false, Optional.empty());
        }

        if (holder.isNull()) {
            return new KvGetResult(true, Optional.empty());
        }

        return new KvGetResult(true, Optional.of(holder.bytes().clone()));
    }

    @Override
    public boolean delete(String key) {
        return data.remove(key) != null;
    }

    @Override
    public void range(String keySince, String keyTo, Consumer<KvPair> consumer) {
        data.subMap(keySince, true, keyTo, true).forEach((String key, ValueHolder holder) -> {
            byte[] actualValue = holder.isNull() ? null : holder.bytes().clone();
            consumer.accept(new KvPair(key, actualValue));
        });
    }

    @Override
    public long count() {
        return data.size();
    }
}