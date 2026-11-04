package io.github.rawwise.metastore;

import com.google.protobuf.ByteString;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.engine.spi.FaultInjector;
import io.github.rawwise.meta.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcMetastoreClient implements Metastore {

    private static final long RPC_DEADLINE_SECONDS = 10;
    private static final long SCAN_DEADLINE_SECONDS = 60;

    private static final ExecutorService CHANNEL_EXECUTOR =
            Executors.newFixedThreadPool(
                    Runtime.getRuntime().availableProcessors(),
                    r -> {
                        Thread t = new Thread(r, "metastore-grpc-worker");
                        t.setDaemon(true);
                        return t;
                    });

    private final ManagedChannel channel;
    private final MetastoreServiceGrpc.MetastoreServiceBlockingStub blockingStub;
    private final FaultInjector faultInjector;

    public GrpcMetastoreClient(String host, int port) {
        this(
                ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .maxInboundMessageSize(16 * 1024 * 1024)
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .keepAliveWithoutCalls(false)
                        .idleTimeout(300, TimeUnit.SECONDS)
                        .executor(CHANNEL_EXECUTOR)
                        .build());
    }

    public GrpcMetastoreClient(ManagedChannel channel) {
        this(channel, null);
    }

    public GrpcMetastoreClient(ManagedChannel channel, FaultInjector faultInjector) {
        this.channel = channel;
        this.blockingStub = MetastoreServiceGrpc.newBlockingStub(channel);
        this.faultInjector = faultInjector != null ? faultInjector : FaultInjector.NOOP;
    }

    private MetastoreServiceGrpc.MetastoreServiceBlockingStub withDeadline() {
        return blockingStub.withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);
    }

    private MetastoreServiceGrpc.MetastoreServiceBlockingStub withScanDeadline() {
        return blockingStub.withDeadlineAfter(SCAN_DEADLINE_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public Optional<byte[]> get(byte[] key) throws IOException {
        IOException lastError = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                faultInjector.inject(FaultInjector.InjectionPoint.DURING_METASTORE_GRPC_CALL);
                KvGetResponse response =
                        withDeadline()
                                .kvGet(
                                        KvGetRequest.newBuilder()
                                                .setKey(ByteString.copyFrom(key))
                                                .build());
                return response.getFound()
                        ? Optional.of(response.getValue().toByteArray())
                        : Optional.empty();
            } catch (Exception e) {
                lastError = new IOException("kvGet failed (attempt " + (attempt + 1) + "/3)", e);
                if (attempt < 2) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        throw lastError;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        try {
            withDeadline()
                    .kvPut(
                            KvPutRequest.newBuilder()
                                    .setKey(ByteString.copyFrom(key))
                                    .setValue(ByteString.copyFrom(value))
                                    .build());
        } catch (Exception e) {
            throw new IOException("kvPut failed", e);
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        try {
            withDeadline()
                    .kvDelete(
                            KvDeleteRequest.newBuilder().setKey(ByteString.copyFrom(key)).build());
        } catch (Exception e) {
            throw new IOException("kvDelete failed", e);
        }
    }

    @Override
    public boolean exists(byte[] key) throws IOException {
        try {
            KvExistsResponse response =
                    withDeadline()
                            .kvExists(
                                    KvExistsRequest.newBuilder()
                                            .setKey(ByteString.copyFrom(key))
                                            .build());
            return response.getExists();
        } catch (Exception e) {
            throw new IOException("kvExists failed", e);
        }
    }

    @Override
    public CloseableIterator<KeyValue> scan(byte[] prefix) throws IOException {
        return doScan(KvScanRequest.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build());
    }

    @Override
    public CloseableIterator<KeyValue> scan(byte[] prefix, int limit) throws IOException {
        return doScan(
                KvScanRequest.newBuilder()
                        .setPrefix(ByteString.copyFrom(prefix))
                        .setLimit(limit)
                        .build());
    }

    @Override
    public CloseableIterator<KeyValue> scanRange(byte[] startInclusive, byte[] endExclusive)
            throws IOException {
        return doScanRange(
                KvScanRangeRequest.newBuilder()
                        .setStartInclusive(ByteString.copyFrom(startInclusive))
                        .setEndExclusive(ByteString.copyFrom(endExclusive))
                        .build());
    }

    @Override
    public CloseableIterator<KeyValue> scanRange(
            byte[] startInclusive, byte[] endExclusive, int limit) throws IOException {
        return doScanRange(
                KvScanRangeRequest.newBuilder()
                        .setStartInclusive(ByteString.copyFrom(startInclusive))
                        .setEndExclusive(ByteString.copyFrom(endExclusive))
                        .setLimit(limit)
                        .build());
    }

    private CloseableIterator<KeyValue> doScan(KvScanRequest request) throws IOException {
        try {
            java.util.Iterator<KvEntry> it = withScanDeadline().kvScan(request);
            return new GrpcEntryIterator(it);
        } catch (Exception e) {
            throw new IOException("kvScan failed", e);
        }
    }

    private CloseableIterator<KeyValue> doScanRange(KvScanRangeRequest request) throws IOException {
        try {
            java.util.Iterator<KvEntry> it = withScanDeadline().kvScanRange(request);
            return new GrpcEntryIterator(it);
        } catch (Exception e) {
            throw new IOException("kvScanRange failed", e);
        }
    }

    @Override
    public boolean compareAndSwap(List<Precondition> preconditions, List<Mutation> mutations)
            throws IOException {
        try {
            KvCasRequest.Builder reqBuilder = KvCasRequest.newBuilder();
            for (Precondition p : preconditions) {
                reqBuilder.addPreconditions(convertPrecondition(p));
            }
            for (Mutation m : mutations) {
                reqBuilder.addMutations(convertMutation(m));
            }
            KvCasResponse response = withDeadline().kvCompareAndSwap(reqBuilder.build());
            return response.getSuccess();
        } catch (Exception e) {
            throw new IOException("kvCompareAndSwap failed", e);
        }
    }

    @Override
    public void batchWrite(List<Mutation> mutations) throws IOException {
        try {
            KvBatchWriteRequest.Builder reqBuilder = KvBatchWriteRequest.newBuilder();
            for (Mutation m : mutations) {
                reqBuilder.addMutations(convertMutation(m));
            }
            withDeadline().kvBatchWrite(reqBuilder.build());
        } catch (Exception e) {
            throw new IOException("kvBatchWrite failed", e);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            withDeadline().kvFlush(com.google.protobuf.Empty.getDefaultInstance());
        } catch (Exception e) {
            throw new IOException("kvFlush failed", e);
        }
    }

    @Override
    public void close() throws IOException {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static PbPrecondition convertPrecondition(Precondition p) {
        if (p instanceof Precondition.Exists e) {
            return PbPrecondition.newBuilder()
                    .setKind(PbPreconditionKind.EXISTS)
                    .setKey(ByteString.copyFrom(e.key()))
                    .build();
        } else if (p instanceof Precondition.NotExists ne) {
            return PbPrecondition.newBuilder()
                    .setKind(PbPreconditionKind.NOT_EXISTS)
                    .setKey(ByteString.copyFrom(ne.key()))
                    .build();
        } else if (p instanceof Precondition.ValueEquals ve) {
            return PbPrecondition.newBuilder()
                    .setKind(PbPreconditionKind.VALUE_EQUALS)
                    .setKey(ByteString.copyFrom(ve.key()))
                    .setExpectedValue(ByteString.copyFrom(ve.expected()))
                    .build();
        }
        throw new IllegalArgumentException("Unknown precondition type: " + p.getClass());
    }

    private static PbMutation convertMutation(Mutation m) {
        if (m instanceof Mutation.Put put) {
            return PbMutation.newBuilder()
                    .setKind(PbMutationKind.PUT)
                    .setKey(ByteString.copyFrom(put.key()))
                    .setValue(ByteString.copyFrom(put.value()))
                    .build();
        } else if (m instanceof Mutation.Delete del) {
            return PbMutation.newBuilder()
                    .setKind(PbMutationKind.DELETE)
                    .setKey(ByteString.copyFrom(del.key()))
                    .build();
        }
        throw new IllegalArgumentException("Unknown mutation type: " + m.getClass());
    }

    private static class GrpcEntryIterator implements CloseableIterator<KeyValue> {

        private final java.util.Iterator<KvEntry> delegate;

        GrpcEntryIterator(java.util.Iterator<KvEntry> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public KeyValue next() {
            KvEntry entry = delegate.next();
            return new KeyValue(entry.getKey().toByteArray(), entry.getValue().toByteArray());
        }

        @Override
        public void close() {}
    }
}
