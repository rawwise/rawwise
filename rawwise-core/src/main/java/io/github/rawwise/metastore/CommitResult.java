package io.github.rawwise.metastore;

public record CommitResult(
        CommitStatus status, long commitId, long newSnapshotId, String conflictReason) {

    public enum CommitStatus {
        OK,
        CONFLICT,
        ERROR
    }

    public boolean success() {
        return status == CommitStatus.OK;
    }

    public boolean isConflict() {
        return status == CommitStatus.CONFLICT;
    }

    public static CommitResult ok(long commitId, long newSnapshotId) {
        return new CommitResult(CommitStatus.OK, commitId, newSnapshotId, null);
    }

    public static CommitResult conflict(long commitId, String reason) {
        return new CommitResult(CommitStatus.CONFLICT, commitId, -1L, reason);
    }

    public static CommitResult error(long commitId, String reason) {
        return new CommitResult(CommitStatus.ERROR, commitId, -1L, reason);
    }
}
