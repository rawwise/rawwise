package io.github.rawwise.backend.snapshot;

import java.util.Optional;

/**
 * BE Node snapshot checkpoint interface.
 *
 * <p>Architecture:
 *
 * <pre>
 *   DistributedSnapshotManager (BE Server)
 * -> NodeSnapshotCheckpoint.checkpoint(snapshotId) (each BE Node)
 * -> LMDB data copy to snapshot directory + WAL record
 * -> NodeSnapshotCheckpoint.recover(snapshotId) (crash recovery)
 * -> load LMDB from snapshot + replay WAL after snapshot
 * </pre>
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>{@link #checkpoint} creates a local consistency snapshot (LMDB current state + WAL)
 *   <li>{@link #recover} recovers from a specified snapshot (loads snapshot data + replays WAL to
 *       latest)
 *   <li>{@link #getLatestSnapshotId} returns the latest local snapshot ID (for recovery)
 *   <li>checkpoint is idempotent: duplicate calls with same snapshotId are safe
 *   <li>recover is idempotent: duplicate calls on already-recovered snapshot are safe
 * </ul>
 */
public interface NodeSnapshotCheckpoint {

    /**
     * Create a local snapshot.
     *
     * @param snapshotId global snapshot ID (assigned by DistributedSnapshotManager)
     * @return snapshot result (success/failure + WAL seq)
     */
    CheckpointResult checkpoint(long snapshotId);

    /**
     * Recover from a specified snapshot.
     *
     * <p>Recovery stream: load snapshot LMDB data -> replay WAL after snapshot -> recover to latest
     * state.
     *
     * @param snapshotId snapshot ID
     * @return recovery result (success/failure + replayed WAL entry count)
     */
    RecoveryResult recover(long snapshotId);

    /**
     * Query the latest local snapshot ID.
     *
     * @return latest snapshot ID, or empty (no snapshot)
     */
    Optional<Long> getLatestSnapshotId();

    /**
     * Snapshot result.
     *
     * @param success whether successful
     * @param walSeq WAL seq (largest WAL seq at snapshot time)
     * @param dataSizeBytes snapshot data size in bytes
     * @param error failure reason (when success=false)
     */
    record CheckpointResult(boolean success, long walSeq, long dataSizeBytes, String error) {
        public static CheckpointResult success(long walSeq, long dataSizeBytes) {
            return new CheckpointResult(true, walSeq, dataSizeBytes, null);
        }

        public static CheckpointResult failed(String error) {
            return new CheckpointResult(false, -1L, 0L, error);
        }
    }

    /**
     * Recovery result.
     *
     * @param success whether successful
     * @param replayedCount replayed WAL entry count
     * @param error failure reason
     */
    record RecoveryResult(boolean success, int replayedCount, String error) {
        public static RecoveryResult success(int replayedCount) {
            return new RecoveryResult(true, replayedCount, null);
        }

        public static RecoveryResult failed(String error) {
            return new RecoveryResult(false, 0, error);
        }
    }
}
