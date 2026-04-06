package com.example.demo.grpc;

import com.example.demo.kv.KvGetResult;
import com.example.demo.kv.KvPair;
import com.example.demo.kv.KvStorage;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;

@GrpcService
public class KvGrpcService extends KvStoreServiceGrpc.KvStoreServiceImplBase {

    private final KvStorage storage;

    public KvGrpcService(KvStorage storage) {
        this.storage = storage;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        byte[] value = request.hasValue() ? request.getValue().getValue().toByteArray() : null;
        storage.put(request.getKey(), value);
        responseObserver.onNext(PutResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            KvGetResult result = storage.get(request.getKey());
            GetResponse.Builder builder = GetResponse.newBuilder().setFound(result.found());

            result.value().ifPresent(bytes ->
                    builder.setValue(BytesValue.of(ByteString.copyFrom(bytes)))
            );

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        boolean deleted = storage.delete(request.getKey());
        responseObserver.onNext(DeleteResponse.newBuilder().setDeleted(deleted).build());
        responseObserver.onCompleted();
    }

    @Override
    public void range(RangeRequest request, StreamObserver<KeyValue> responseObserver) {
        try {
            storage.range(request.getKeySince(), request.getKeyTo(), (KvPair pair) -> {
                KeyValue.Builder pairBuilder = KeyValue.newBuilder().setKey(pair.key());
                if (pair.value() != null) {
                    pairBuilder.setValue(BytesValue.of(ByteString.copyFrom(pair.value())));
                }
                responseObserver.onNext(pairBuilder.build());
            });
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        responseObserver.onNext(CountResponse.newBuilder().setCount(storage.count()).build());
        responseObserver.onCompleted();
    }
}
