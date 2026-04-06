package com.example.demo.kv;

import java.util.function.Consumer;

public interface KvStorage extends AutoCloseable {

    void put(String key, byte[] value);

    KvGetResult get(String key);

    boolean delete(String key);

    void range(String keySince, String keyTo, Consumer<KvPair> consumer);

    long count();

    @Override
    default void close() throws Exception {
    }
}
