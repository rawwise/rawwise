package io.github.rawwise.engine.clean;

public record CleanResult(
        int deletedFileCount,
        long deletedBytes,
        int rewrittenFileCount,
        long rewrittenBytes,
        int expiredSnapshotCount,
        long reclaimedBytes,
        long scheduledBytes,
        long physicallyDeletedBytes,
        int failedActionCount,
        long durationMs) {

    public CleanResult {
        if (deletedFileCount < 0)
            throw new IllegalArgumentException("deletedFileCount must not be negative");
        if (deletedBytes < 0)
            throw new IllegalArgumentException("deletedBytes must not be negative");
        if (rewrittenFileCount < 0)
            throw new IllegalArgumentException("rewrittenFileCount must not be negative");
        if (rewrittenBytes < 0)
            throw new IllegalArgumentException("rewrittenBytes must not be negative");
        if (expiredSnapshotCount < 0)
            throw new IllegalArgumentException("expiredSnapshotCount must not be negative");
        if (reclaimedBytes < 0)
            throw new IllegalArgumentException("reclaimedBytes must not be negative");
        if (scheduledBytes < 0)
            throw new IllegalArgumentException("scheduledBytes must not be negative");
        if (physicallyDeletedBytes < 0)
            throw new IllegalArgumentException("physicallyDeletedBytes must not be negative");
        if (failedActionCount < 0)
            throw new IllegalArgumentException("failedActionCount must not be negative");
        if (durationMs < 0) throw new IllegalArgumentException("durationMs must not be negative");
    }

    public static CleanResult empty() {
        return new CleanResult(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    public CleanResult merge(CleanResult other) {
        if (other == null) {
            throw new IllegalArgumentException("other must not be null");
        }
        return new CleanResult(
                deletedFileCount + other.deletedFileCount,
                deletedBytes + other.deletedBytes,
                rewrittenFileCount + other.rewrittenFileCount,
                rewrittenBytes + other.rewrittenBytes,
                expiredSnapshotCount + other.expiredSnapshotCount,
                reclaimedBytes + other.reclaimedBytes,
                scheduledBytes + other.scheduledBytes,
                physicallyDeletedBytes + other.physicallyDeletedBytes,
                failedActionCount + other.failedActionCount,
                durationMs + other.durationMs);
    }
}
