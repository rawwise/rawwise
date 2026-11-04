package io.github.rawwise.engine.compaction.gc;

import io.github.rawwise.model.record.Snapshot;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class DefaultSnapshotRetentionPolicy implements SnapshotRetentionPolicy {

    private final int minSnapshotsToKeep;
    private final long maxSnapshotAgeMinutes;

    public DefaultSnapshotRetentionPolicy(int minSnapshotsToKeep, long maxSnapshotAgeMinutes) {
        if (minSnapshotsToKeep < 1) {
            throw new IllegalArgumentException(
                    "minSnapshotsToKeep must be at least 1, got " + minSnapshotsToKeep);
        }
        this.minSnapshotsToKeep = minSnapshotsToKeep;
        this.maxSnapshotAgeMinutes = maxSnapshotAgeMinutes;
    }

    public DefaultSnapshotRetentionPolicy() {
        this(5, 10080L);
    }

    @Override
    public List<Snapshot> retain(List<Snapshot> snapshots) {
        if (snapshots.isEmpty()) {
            return List.of();
        }

        List<Snapshot> sorted = new ArrayList<>(snapshots);
        sorted.sort(Comparator.comparingLong(Snapshot::snapshotId).reversed());

        long cutoffMs = System.currentTimeMillis() - maxSnapshotAgeMinutes * 60_000L;

        List<Snapshot> retained = new ArrayList<>();
        for (Snapshot s : sorted) {
            if (retained.size() < minSnapshotsToKeep) {
                retained.add(s);
                continue;
            }
            if (s.timestampMillis() >= cutoffMs) {
                retained.add(s);
                continue;
            }
            break;
        }

        return retained;
    }
}
