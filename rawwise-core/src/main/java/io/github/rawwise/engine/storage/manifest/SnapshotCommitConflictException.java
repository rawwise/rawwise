package io.github.rawwise.engine.storage.manifest;

import lombok.Getter;

@Getter
public class SnapshotCommitConflictException extends RuntimeException {
    private final long baseSnapshotId;
    private final long currentSnapshotId;

    public SnapshotCommitConflictException(
            String message, long baseSnapshotId, long currentSnapshotId) {
        super(message);
        this.baseSnapshotId = baseSnapshotId;
        this.currentSnapshotId = currentSnapshotId;
    }
}
