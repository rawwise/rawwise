package io.github.rawwise.backend.snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Distributed snapshot coordinator (BE Server).
 *
 * <p>Architecture:
 *
 * <pre>
 *   BE Server:
 *     DistributedSnapshotManager.beginGlobalSnapshot()
 * -> generate global snapshotId
 * -> notify all BE Nodes: NodeSnapshotCheckpoint.checkpoint(snapshotId)
 * -> collect all node snapshot results
 * -> persist global snapshot metadata
 *
 * crash recovery:
 *     DistributedSnapshotManager.recoverNode(executorId)
 * -> find the node's most recent successful snapshot
 *       -> NodeSnapshotCheckpoint.recover(snapshotId)
 * -> replay WAL to latest
 * </pre>
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>global snapshot ID is unique (AtomicLong)
 *   <li>all node checkpoints execute in parallel; failure marks global snapshot as PARTIAL
 *   <li>snapshot metadata persisted in-memory (MVP); production uses Metastore
 *   <li>recovery uses the most recent successful global snapshot
 *   <li>supports periodic automatic snapshots (ScheduledExecutorService, default 5 minutes)
 * </ul>
 */
@Slf4j
public class DistributedSnapshotManager {
    public static final long DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 300L;
    public static final int MAX_SNAPSHOT_RETENTION = 100;

    private final Map<String, NodeSnapshotCheckpoint> nodeCheckpoints = new ConcurrentHashMap<>();
    private final Map<Long, GlobalSnapshotMetadata> snapshots = new ConcurrentHashMap<>();
    private final AtomicLong snapshotIdGenerator = new AtomicLong(System.currentTimeMillis());

    public DistributedSnapshotManager() {}

    /**
     * Register a BE Node snapshot checkpoint.
     *
     * @param executorId executor identifier
     * @param checkpoint snapshot checkpoint implementation
     */
    public void registerNode(String executorId, NodeSnapshotCheckpoint checkpoint) {
        Objects.requireNonNull(executorId, "executorId");
        Objects.requireNonNull(checkpoint, "checkpoint");
        nodeCheckpoints.put(executorId, checkpoint);
        log.info("Snapshot checkpoint registered: executorId={}", executorId);
    }

    /** Deregister a BE Node snapshot checkpoint (called on node down). */
    public void unregisterNode(String executorId) {
        nodeCheckpoints.remove(executorId);
        log.info("Snapshot checkpoint unregistered: executorId={}", executorId);
    }

    /**
     * Begin a global snapshot.
     *
     * <p>Flow:
     *
     * <ol>
     *   <li>generate global snapshotId
     *   <li>call all registered nodes' checkpoint(snapshotId) in parallel
     *   <li>collect results, mark global snapshot state (COMPLETE/PARTIAL/FAILED)
     *   <li>persist snapshot metadata
     * </ol>
     *
     * @return global snapshot metadata
     */
    public synchronized GlobalSnapshotMetadata beginGlobalSnapshot() {
        long snapshotId = snapshotIdGenerator.incrementAndGet();
        long timestamp = System.currentTimeMillis();
        log.info(
                "Beginning global snapshot: snapshotId={}, nodes={}",
                snapshotId,
                nodeCheckpoints.size());

        Map<String, NodeSnapshotCheckpoint.CheckpointResult> results = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Map.Entry<String, NodeSnapshotCheckpoint> entry : nodeCheckpoints.entrySet()) {
            String executorId = entry.getKey();
            NodeSnapshotCheckpoint checkpoint = entry.getValue();
            CompletableFuture<Void> future =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    NodeSnapshotCheckpoint.CheckpointResult result =
                                            checkpoint.checkpoint(snapshotId);
                                    results.put(executorId, result);
                                    if (result.success()) {
                                        log.debug(
                                                "Snapshot checkpoint success: snapshotId={}, executorId={}, walSeq={}, sizeBytes={}",
                                                snapshotId,
                                                executorId,
                                                result.walSeq(),
                                                result.dataSizeBytes());
                                    } else {
                                        log.warn(
                                                "Snapshot checkpoint failed: snapshotId={}, executorId={}, error={}",
                                                snapshotId,
                                                executorId,
                                                result.error());
                                    }
                                } catch (Exception e) {
                                    results.put(
                                            executorId,
                                            NodeSnapshotCheckpoint.CheckpointResult.failed(
                                                    e.getMessage()));
                                    log.warn(
                                            "Snapshot checkpoint exception: snapshotId={}, executorId={}, error={}",
                                            snapshotId,
                                            executorId,
                                            e.getMessage());
                                }
                            });
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        GlobalSnapshotMetadata.SnapshotState state;
        long successCount =
                results.values().stream()
                        .filter(NodeSnapshotCheckpoint.CheckpointResult::success)
                        .count();
        if (successCount == results.size()) {
            state = GlobalSnapshotMetadata.SnapshotState.COMPLETE;
        } else if (successCount == 0) {
            state = GlobalSnapshotMetadata.SnapshotState.FAILED;
        } else {
            state = GlobalSnapshotMetadata.SnapshotState.PARTIAL;
        }

        GlobalSnapshotMetadata metadata =
                new GlobalSnapshotMetadata(snapshotId, timestamp, Map.copyOf(results), state);
        snapshots.put(snapshotId, metadata);
        pruneOldSnapshots();
        log.info(
                "Global snapshot completed: snapshotId={}, state={}, successNodes={}/{}",
                snapshotId,
                state,
                successCount,
                results.size());
        return metadata;
    }

    /**
     * Recover a specified node from the most recent successful global snapshot.
     *
     * <p>Recovery flow:
     *
     * <ol>
     *   <li>find the node's most recent successful snapshot (scan from newest to oldest)
     *   <li>call NodeSnapshotCheckpoint.recover(snapshotId)
     *   <li>return recovery result
     * </ol>
     *
     * @param executorId executorId to recover
     * @return recovery result, or empty (no available snapshot or node not registered)
     */
    public Optional<NodeSnapshotCheckpoint.RecoveryResult> recoverNode(String executorId) {
        NodeSnapshotCheckpoint checkpoint = nodeCheckpoints.get(executorId);
        if (checkpoint == null) {
            log.warn("recoverNode: executorId={} not registered", executorId);
            return Optional.empty();
        }
        Optional<Long> latestSnapshotId = findLatestSuccessfulSnapshot(executorId);
        if (latestSnapshotId.isEmpty()) {
            log.warn("recoverNode: no successful snapshot found for executorId={}", executorId);
            return Optional.empty();
        }
        long snapshotId = latestSnapshotId.get();
        log.info(
                "recoverNode: recovering executorId={} from snapshotId={}", executorId, snapshotId);
        try {
            NodeSnapshotCheckpoint.RecoveryResult result = checkpoint.recover(snapshotId);
            if (result.success()) {
                log.info(
                        "recoverNode: success executorId={}, snapshotId={}, replayedCount={}",
                        executorId,
                        snapshotId,
                        result.replayedCount());
            } else {
                log.warn(
                        "recoverNode: failed executorId={}, snapshotId={}, error={}",
                        executorId,
                        snapshotId,
                        result.error());
            }
            return Optional.of(result);
        } catch (Exception e) {
            log.error(
                    "recoverNode: exception executorId={}, snapshotId={}, error={}",
                    executorId,
                    snapshotId,
                    e.getMessage(),
                    e);
            return Optional.of(NodeSnapshotCheckpoint.RecoveryResult.failed(e.getMessage()));
        }
    }

    /** Query all snapshot metadata (sorted by snapshotId descending). */
    public List<GlobalSnapshotMetadata> listSnapshots() {
        List<GlobalSnapshotMetadata> list = new ArrayList<>(snapshots.values());
        list.sort((a, b) -> Long.compare(b.snapshotId(), a.snapshotId()));
        return Collections.unmodifiableList(list);
    }

    /** Query specified snapshot metadata. */
    public Optional<GlobalSnapshotMetadata> getSnapshot(long snapshotId) {
        return Optional.ofNullable(snapshots.get(snapshotId));
    }

    /** Returns the registered node count. */
    public int getNodeCount() {
        return nodeCheckpoints.size();
    }

    private void pruneOldSnapshots() {
        while (snapshots.size() > MAX_SNAPSHOT_RETENTION) {
            long oldest = snapshots.keySet().stream().min(Long::compare).orElse(-1L);
            if (oldest < 0) break;
            snapshots.remove(oldest);
            log.debug("Pruned old snapshot: snapshotId={}", oldest);
        }
    }

    private Optional<Long> findLatestSuccessfulSnapshot(String executorId) {
        List<GlobalSnapshotMetadata> sorted = listSnapshots();
        for (GlobalSnapshotMetadata meta : sorted) {
            NodeSnapshotCheckpoint.CheckpointResult result = meta.nodeResults().get(executorId);
            if (result != null && result.success()) {
                return Optional.of(meta.snapshotId());
            }
        }
        return Optional.empty();
    }

    /**
     * Global snapshot metadata.
     *
     * @param snapshotId global snapshot ID
     * @param timestamp snapshot timestamp
     * @param nodeResults each node's snapshot result (executorId -> CheckpointResult)
     * @param state global snapshot state (COMPLETE/PARTIAL/FAILED)
     */
    public record GlobalSnapshotMetadata(
            long snapshotId,
            long timestamp,
            Map<String, NodeSnapshotCheckpoint.CheckpointResult> nodeResults,
            SnapshotState state) {
        public enum SnapshotState {
            /** all node snapshots succeeded */
            COMPLETE,
            /** partial node snapshots succeeded */
            PARTIAL,
            /** all node snapshots failed */
            FAILED
        }

        public boolean isComplete() {
            return state == SnapshotState.COMPLETE;
        }

        public int successNodeCount() {
            return (int)
                    nodeResults.values().stream()
                            .filter(NodeSnapshotCheckpoint.CheckpointResult::success)
                            .count();
        }
    }
}
