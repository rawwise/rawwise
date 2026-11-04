package io.github.rawwise.engine.clean.strategy;

import io.github.rawwise.engine.clean.CleanAction;
import io.github.rawwise.engine.clean.CleanContext;
import io.github.rawwise.engine.clean.ExpireSnapshotAction;
import io.github.rawwise.model.record.Snapshot;
import java.util.ArrayList;
import java.util.List;

public class SnapshotExpiryStrategy implements CleanStrategy {

    private static final long serialVersionUID = 1L;

    private final long retentionMs;
    private final int minSnapshotsToKeep;

    public SnapshotExpiryStrategy(long retentionMs, int minSnapshotsToKeep) {
        if (retentionMs < 0) {
            throw new IllegalArgumentException("retentionMs must not be negative");
        }
        if (minSnapshotsToKeep < 1) {
            throw new IllegalArgumentException("minSnapshotsToKeep must be at least 1");
        }
        this.retentionMs = retentionMs;
        this.minSnapshotsToKeep = minSnapshotsToKeep;
    }

    @Override
    public StrategyType strategyType() {
        return StrategyType.SNAPSHOT_EXPIRY;
    }

    @Override
    public List<CleanAction> plan(CleanContext context) {
        List<Snapshot> snapshots = context.snapshots();
        if (snapshots == null || snapshots.size() <= minSnapshotsToKeep) {
            return List.of();
        }

        List<Snapshot> sorted = new ArrayList<>(snapshots);
        sorted.sort((a, b) -> Long.compare(b.timestampMillis(), a.timestampMillis()));

        long cutoffTime = context.currentTimestampMs() - retentionMs;
        List<CleanAction> actions = new ArrayList<>();

        for (int i = minSnapshotsToKeep; i < sorted.size(); i++) {
            Snapshot snapshot = sorted.get(i);
            if (retentionMs <= 0 || snapshot.timestampMillis() < cutoffTime) {
                actions.add(new ExpireSnapshotAction(snapshot.snapshotId()));
            }
        }
        return actions;
    }

    public long retentionMs() {
        return retentionMs;
    }

    public int minSnapshotsToKeep() {
        return minSnapshotsToKeep;
    }
}
