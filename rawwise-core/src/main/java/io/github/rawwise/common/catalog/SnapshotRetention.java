package io.github.rawwise.common.catalog;

import java.time.Duration;

public record SnapshotRetention(int minSnapshotsToKeep, Duration maxAge) {
    public static SnapshotRetention forever() {
        return new SnapshotRetention(1, Duration.ofDays(36500));
    }
}
