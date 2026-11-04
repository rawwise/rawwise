package io.github.rawwise.engine.txn;

/**
 * immutablerecord, capturetransactionstartwhenvisibleityboundary.
 *
 * <p>ReadSnapshot is MVCC visibleitydeterminefoundation: holdthesnapshottransactioncanbyseeto all
 * commitTs <= snapshotTs dataversion.baseSnapshotId recordtransaction startwhen Metastore
 * snapshotversion.
 *
 * @param snapshotTs timestampboundary: commitTs <= snapshotTs dataversionvisible
 * @param baseSnapshotId transactionstartwhen Metastore snapshot ID
 */
public record ReadSnapshot(long snapshotTs, long baseSnapshotId) {
    public ReadSnapshot {
        if (snapshotTs < 0) {
            throw new IllegalArgumentException("snapshotTs must be non-negative: " + snapshotTs);
        }
        if (baseSnapshotId < 0) {
            throw new IllegalArgumentException(
                    "baseSnapshotId must be non-negative: " + baseSnapshotId);
        }
    }

    /**
     * useexplicitspecified snapshotTs create ReadSnapshot.
     *
     * <p>snapshotTs shouldfrom {@link ShardVersionTracker#currentCommitTs()}, by LogicalClock
     * unifiedallocation, implementation commitTs sourceunified.
     *
     * @param baseSnapshotId transactionstartwhen Metastore snapshot ID
     * @param snapshotTs explicitspecifiedtimestampboundary
     * @return new ReadSnapshot
     */
    public static ReadSnapshot now(long baseSnapshotId, long snapshotTs) {
        return new ReadSnapshot(snapshotTs, baseSnapshotId);
    }

    /**
     * determinegiven commitTs dataversioninsnapshotwithin boundary whethervisible.
     *
     * @param commitTs dataversioncommit timestamp
     * @return true if commitTs <= snapshotTs
     */
    public boolean isVisible(long commitTs) {
        return commitTs <= snapshotTs;
    }
}
