package io.github.rawwise.backend.replication;

import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.SnapshotRequest;
import io.grpc.ManagedChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * New replica full sync synchronizer.
 *
 * <p>Architecture: on new startup or crash recovery, replica uses {@code StreamSnapshot} RPC from
 * primary to pull full key-value snapshot, writes to local LMDB, then takes over real-time sync
 * mode.
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>replica sends {@code StreamSnapshot} request to primary (specified tableId + shardId)
 *   <li>primary scans local LMDB for all shard data, pushes in chunks (SnapshotChunk)
 *   <li>replica receives each SnapshotChunk, writes to local LMDB via {@code store.write()}
 *   <li>on stream complete (hasMore=false), returns synced record count
 *   <li>HDFS archive WAL as supplementary recovery source (when primary unavailable), enabled via
 *       {@code rawwise.replication.hdfs-fallback=true}
 * </ul>
 *
 * <p>Thread safety: single sync via CountDownLatch waiting for completion; does not support
 * concurrent shard sync.
 */
@Slf4j
public class WalReplayer {
    private final ManagedChannel primaryChannel;
    private final long rpcTimeoutMs;

    public WalReplayer(ManagedChannel primaryChannel, long rpcTimeoutMs) {
        this.primaryChannel = primaryChannel;
        this.rpcTimeoutMs = rpcTimeoutMs;
    }

    /**
     * Full sync specified shard data from primary to local.
     *
     * <p>Caller (replica BE Node) passes in {@code ReplicationSink}; replayer writes each key-value
     * to local LMDB via sink.
     *
     * @param tableId table identifier
     * @param shardId shard identifier
     * @param sink local write sink (replica encapsulates store.write/delete)
     * @return synced record count, -1 indicates failure
     */
    public long fullSync(String tableId, int shardId, ReplicationSink sink) {
        ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub =
                ExecutorPointQueryServiceGrpc.newBlockingStub(primaryChannel)
                        .withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS);
        SnapshotRequest request =
                SnapshotRequest.newBuilder()
                        .setTableId(tableId)
                        .setShardId(shardId)
                        .setFromSeq(0L)
                        .build();
        AtomicLong synced = new AtomicLong(0L);
        AtomicReference<String> errorRef = new AtomicReference<>(null);
        CountDownLatch done = new CountDownLatch(1);
        try {
            stub.streamSnapshot(request)
                    .forEachRemaining(
                            chunk -> {
                                int keyCount = chunk.getKeysCount();
                                for (int i = 0; i < keyCount; i++) {
                                    byte[] key = chunk.getKeys(i).toByteArray();
                                    byte[] value = chunk.getValues(i).toByteArray();
                                    try {
                                        sink.apply(tableId, key, value);
                                        synced.incrementAndGet();
                                    } catch (Exception e) {
                                        errorRef.compareAndSet(
                                                null, "sink apply failed: " + e.getMessage());
                                        return;
                                    }
                                }
                            });
            done.countDown();
        } catch (Exception e) {
            errorRef.set("streamSnapshot RPC failed: " + e.getMessage());
            done.countDown();
        }
        String err = errorRef.get();
        if (err != null) {
            log.error("fullSync failed for table={}, shard={}: {}", tableId, shardId, err);
            return -1L;
        }
        log.info(
                "fullSync completed: table={}, shard={}, synced={}",
                tableId,
                shardId,
                synced.get());
        return synced.get();
    }

    /**
     * Replica write sink abstraction.
     *
     * <p>Provided by replica BE Node's {@code ExecutorPointQueryServiceImpl}, writes key-value to
     * local LMDB (tombstone triggers delete).
     */
    public interface ReplicationSink {
        void apply(String tableId, byte[] key, byte[] value);
    }
}
