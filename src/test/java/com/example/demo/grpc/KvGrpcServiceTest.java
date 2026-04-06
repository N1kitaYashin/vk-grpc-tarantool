package com.example.demo.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.demo.kv.InMemoryKvStorage;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class KvGrpcServiceTest {

    @Test
    void putGetDeleteCountAndRangeWork() {
        InMemoryKvStorage storage = new InMemoryKvStorage();
        KvGrpcService service = new KvGrpcService(storage);

        TestObserver<PutResponse> putObserver = new TestObserver<>();
        service.put(PutRequest.newBuilder()
                .setKey("k1")
                .setValue(BytesValue.of(ByteString.copyFromUtf8("v1")))
                .build(), putObserver);
        assertTrue(putObserver.completed);

        TestObserver<PutResponse> putNullObserver = new TestObserver<>();
        service.put(PutRequest.newBuilder().setKey("k2").build(), putNullObserver);
        assertTrue(putNullObserver.completed);

        TestObserver<GetResponse> getObserver = new TestObserver<>();
        service.get(GetRequest.newBuilder().setKey("k1").build(), getObserver);
        assertTrue(getObserver.completed);
        assertEquals(1, getObserver.values.size());
        assertTrue(getObserver.values.getFirst().getFound());
        assertEquals("v1", getObserver.values.getFirst().getValue().getValue().toStringUtf8());

        TestObserver<GetResponse> getNullObserver = new TestObserver<>();
        service.get(GetRequest.newBuilder().setKey("k2").build(), getNullObserver);
        assertTrue(getNullObserver.values.getFirst().getFound());
        assertFalse(getNullObserver.values.getFirst().hasValue());

        TestObserver<CountResponse> countObserver = new TestObserver<>();
        service.count(CountRequest.newBuilder().build(), countObserver);
        assertEquals(2, countObserver.values.getFirst().getCount());

        TestObserver<KeyValue> rangeObserver = new TestObserver<>();
        service.range(RangeRequest.newBuilder().setKeySince("k1").setKeyTo("k9").build(), rangeObserver);
        assertEquals(2, rangeObserver.values.size());
        assertEquals("k1", rangeObserver.values.get(0).getKey());
        assertEquals("v1", rangeObserver.values.get(0).getValue().getValue().toStringUtf8());
        assertEquals("k2", rangeObserver.values.get(1).getKey());
        assertFalse(rangeObserver.values.get(1).hasValue());

        TestObserver<DeleteResponse> deleteObserver = new TestObserver<>();
        service.delete(DeleteRequest.newBuilder().setKey("k1").build(), deleteObserver);
        assertTrue(deleteObserver.values.getFirst().getDeleted());

        TestObserver<GetResponse> getDeletedObserver = new TestObserver<>();
        service.get(GetRequest.newBuilder().setKey("k1").build(), getDeletedObserver);
        assertFalse(getDeletedObserver.values.getFirst().getFound());
        assertFalse(getDeletedObserver.values.getFirst().hasValue());
    }

    private static final class TestObserver<T> implements StreamObserver<T> {
        private final List<T> values = new ArrayList<>();
        private boolean completed;

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable t) {
            throw new AssertionError(t);
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }
    }
}