package io.github.rawwise.engine.compaction;

import io.github.rawwise.engine.storage.manifest.SnapshotCommitConflictException;

public final class RetryableException extends RuntimeException {

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public static boolean isRetryable(Exception e) {
        if (e instanceof CompactionRetryableException) return true;
        if (e instanceof CompactionException ce) return ce.isRetryable();
        if (e instanceof SnapshotCommitConflictException) return true;
        if (e instanceof RetryableException) return true;
        return false;
    }

    public static boolean isFatal(Exception e) {
        if (e instanceof CompactionFatalException) return true;
        if (e instanceof CompactionException ce) return ce.isFatal();
        return false;
    }
}
