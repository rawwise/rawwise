package io.github.rawwise.engine.txn.isolate;

import io.github.rawwise.engine.txn.IsolationLevel;

/**
 * immutablereadsnapshot, definetransactionvisibleityboundary.
 *
 * <p>transaction {@code begin()} whencapture, holdthesnapshottransactioncan seetoall commitTs &le;
 * snapshotTs dataversion (INV-004).
 *
 * <p>toin READ_COMMITTED isolationlevel, readSnapshot as null(eachtimereadlatestcommitteddata).
 * toin REPEATABLE_READ and SERIALIZABLE, readSnapshot in begin() whenvia {@code clock.now()}
 * capture.
 */
public record ReadSnapshot(long snapshotTs, long baseSnapshotId) {

    public ReadSnapshot {
        if (snapshotTs < 0) {
            throw new IllegalArgumentException(
                    "snapshotTs must be non-negative, got: " + snapshotTs);
        }
        if (baseSnapshotId < 0) {
            throw new IllegalArgumentException(
                    "baseSnapshotId must be non-negative, got: " + baseSnapshotId);
        }
    }

    /**
     * createcurrentlysnapshot: use clock.now() asas snapshotTs.
     *
     * @param currentCommitTs currentlylogical clockvalue
     * @param baseSnapshotId foundationsnapshot ID
     * @return new ReadSnapshot
     */
    public static ReadSnapshot now(long currentCommitTs, long baseSnapshotId) {
        return new ReadSnapshot(currentCommitTs, baseSnapshotId);
    }

    /**
     * according toisolationlevel decides whethercreatesnapshot.
     *
     * @param level isolationlevel
     * @param currentCommitTs currentlylogical clockvalue
     * @param baseSnapshotId foundationsnapshot ID
     * @return REPEATABLE_READ/SERIALIZABLE whenreturnsnapshot, READ_COMMITTED whenreturn null
     */
    public static ReadSnapshot forLevel(
            IsolationLevel level, long currentCommitTs, long baseSnapshotId) {
        return switch (level) {
            case READ_COMMITTED -> null;
            case REPEATABLE_READ, SERIALIZABLE -> now(currentCommitTs, baseSnapshotId);
        };
    }

    /**
     * determinegiven commitTs inthissnapshotunderwhethervisible.
     *
     * @param commitTs pendingcheck commit timestamp
     * @return true if commitTs <= snapshotTs
     */
    public boolean isVisible(long commitTs) {
        return commitTs <= snapshotTs;
    }

    @Override
    public String toString() {
        return "ReadSnapshot{snapshotTs=" + snapshotTs + ", baseSnapshotId=" + baseSnapshotId + "}";
    }
}
