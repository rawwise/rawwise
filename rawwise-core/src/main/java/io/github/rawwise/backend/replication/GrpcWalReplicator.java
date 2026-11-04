package io.github.rawwise.backend.replication;

import com.google.protobuf.ByteString;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.backend.wal.WALRecord;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.ReplicateAck;
import io.github.rawwise.server.WalEntry;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * gRPC streaming WAL replicator (primary).
 *
 * <p>Architecture:
 *
 * <pre>
 *   Primary BE Node --gRPC streaming WalEntry--> Replica BE Node
 *                      (ReplicateWal client-streaming RPC)
 * </pre>
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>each replica executor maintains a long-lived connection {@link StreamObserver} (lazily
 *       created)
 *   <li>{@link #replicate(int, WALRecord)} async push of WalEntry, does not block primary write
 *       path (eventual consistency mode)
 *   <li>replica's {@code ExecutorPointQueryServiceImpl.replicateWal} receives and replays to local
 *       LMDB
 *   <li>stream auto-rebuilds on open (triggered by next replicate)
 *   <li>no replica ({@link RoutingCoordinator#lookupReplica} returns null) -> no-op
 * </ul>
 *
 * <p>Thread safety: stream observer creation is protected by synchronized; onNext calls are
 * thread-safe (gRPC StreamObserver guarantee).
 */
@Slf4j
public class GrpcWalReplicator {
    private final RoutingCoordinator routingCoordinator;
    private final Map<String, ManagedChannel> replicaChannels;
    private final Map<String, StreamObserver<WalEntry>> replicaStreams = new ConcurrentHashMap<>();
    private final Map<String, Object> streamLocks = new ConcurrentHashMap<>();
    private volatile boolean syncMode;
    private final long syncTimeoutMs;

    public GrpcWalReplicator(
            RoutingCoordinator routingCoordinator,
            Map<String, ManagedChannel> replicaChannels,
            boolean syncMode,
            long syncTimeoutMs) {
        this.routingCoordinator = routingCoordinator;
        this.replicaChannels = replicaChannels != null ? replicaChannels : Map.of();
        this.syncMode = syncMode;
        this.syncTimeoutMs = syncTimeoutMs;
    }

    /**
     * Async copy WAL record to shard replica (eventual consistency mode).
     *
     * <p>Does not block primary write path. If replica is unreachable, logs a warning and skips
     * (eventual consistency degradation).
     *
     * @param shardId shard identifier
     * @param walRecord WAL record (includes tableId, payload, seq)
     */
    public void replicate(int shardId, WALRecord walRecord) {
        if (routingCoordinator == null) {
            return;
        }
        ShardLookupResult replica;
        try {
            replica = routingCoordinator.lookupReplica(new ShardId(shardId));
        } catch (Exception e) {
            log.debug("lookupReplica failed for shard {}: {}", shardId, e.getMessage());
            return;
        }
        if (replica == null || !replica.hasAssignment()) {
            return;
        }
        String replicaExecutorId = replica.assignment().executorId();
        pushToReplica(replicaExecutorId, walRecord);
    }

    /**
     * Traverse all configured replica channels and push WAL record.
     *
     * <p>Used by BE Node's {@code triggerReplication} (triggered after write success). BE Node does
     * not hold RoutingCoordinator, cannot precisely find replica by shard, so traverses all
     * replicaChannels to push. In a single Backend Group + shard replica this is correct for
     * typical deployment (each replica has shard data, as the primary shard key).
     *
     * <p>In multi-shard scenarios, if BE Node is primary for multiple shards and replicas are
     * distributed across different nodes, each replica receives all shard WAL. The replica's {@code
     * replayWalEntry} writes by tableId, does not filter by shard, so non-this-shard data may be
     * incorrectly written. Production should use {@link #replicate(int, WALRecord)} for
     * shard-precise routing (requires BE Node to hold RoutingCoordinator reference).
     *
     * @param walRecord WAL record
     */
    public void replicateToAllReplicas(WALRecord walRecord) {
        if (replicaChannels.isEmpty()) {
            return;
        }
        for (String replicaExecutorId : replicaChannels.keySet()) {
            pushToReplica(replicaExecutorId, walRecord);
        }
    }

    private void pushToReplica(String replicaExecutorId, WALRecord walRecord) {
        ManagedChannel channel = replicaChannels.get(replicaExecutorId);
        if (channel == null || channel.isShutdown()) {
            log.debug("No channel to replica executor {}, skipping replication", replicaExecutorId);
            return;
        }
        WalEntry entry =
                WalEntry.newBuilder()
                        .setSeq(walRecord.sequence())
                        .setPayload(ByteString.copyFrom(walRecord.value()))
                        .setTimestamp(walRecord.timestamp())
                        .setTableId(
                                walRecord.uniqTableIdentifier() != null
                                        ? walRecord.uniqTableIdentifier()
                                        : "")
                        .build();
        try {
            StreamObserver<WalEntry> stream = getOrCreateStream(replicaExecutorId, channel);
            stream.onNext(entry);
        } catch (Exception e) {
            log.warn(
                    "Replicate to {} failed for seq {}: {}",
                    replicaExecutorId,
                    walRecord.sequence(),
                    e.getMessage());
            invalidateStream(replicaExecutorId);
        }
    }

    /**
     * Lazily create or reuse per-replica long-lived connection streaming observer.
     *
     * <p>Stream creation is in a synchronized block, preventing concurrent creation of multiple
     * streams.
     *
     * <p>Response observer only logs acks at debug level (eventual consistency mode, does not
     * block); on error, invalidates stream to trigger rebuild on next call.
     */
    private StreamObserver<WalEntry> getOrCreateStream(
            String replicaExecutorId, ManagedChannel channel) {
        StreamObserver<WalEntry> existing = replicaStreams.get(replicaExecutorId);
        if (existing != null) {
            return existing;
        }
        Object lock = streamLocks.computeIfAbsent(replicaExecutorId, k -> new Object());
        synchronized (lock) {
            existing = replicaStreams.get(replicaExecutorId);
            if (existing != null) {
                return existing;
            }
            ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceStub stub =
                    ExecutorPointQueryServiceGrpc.newStub(channel);
            StreamObserver<WalEntry> stream =
                    stub.replicateWal(
                            new StreamObserver<>() {
                                @Override
                                public void onNext(ReplicateAck ack) {
                                    log.debug(
                                            "Replica {} acked seq={}, success={}",
                                            replicaExecutorId,
                                            ack.getAckedSeq(),
                                            ack.getSuccess());
                                    if (!ack.getSuccess()) {
                                        log.warn(
                                                "Replica {} reported replication error: {}",
                                                replicaExecutorId,
                                                ack.getError());
                                    }
                                }

                                @Override
                                public void onError(Throwable t) {
                                    log.warn(
                                            "Replication stream to {} errored: {}",
                                            replicaExecutorId,
                                            t.getMessage());
                                    invalidateStream(replicaExecutorId);
                                }

                                @Override
                                public void onCompleted() {
                                    log.debug(
                                            "Replication stream to {} completed (will rebuild on next replicate)",
                                            replicaExecutorId);
                                    invalidateStream(replicaExecutorId);
                                }
                            });
            replicaStreams.put(replicaExecutorId, stream);
            return stream;
        }
    }

    private void invalidateStream(String replicaExecutorId) {
        replicaStreams.remove(replicaExecutorId);
    }

    /** Close all replication streams (called during graceful shutdown). */
    public void close() {
        for (Map.Entry<String, StreamObserver<WalEntry>> entry : replicaStreams.entrySet()) {
            try {
                entry.getValue().onCompleted();
            } catch (Exception e) {
                log.debug(
                        "Error closing replication stream to {}: {}",
                        entry.getKey(),
                        e.getMessage());
            }
        }
        replicaStreams.clear();
        streamLocks.clear();
    }

    public boolean isSyncMode() {
        return syncMode;
    }

    public void setSyncMode(boolean syncMode) {
        this.syncMode = syncMode;
    }

    public long getSyncTimeoutMs() {
        return syncTimeoutMs;
    }
}
