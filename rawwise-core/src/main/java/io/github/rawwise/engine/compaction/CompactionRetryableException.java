package io.github.rawwise.engine.compaction;

public class CompactionRetryableException extends CompactionException {

    public CompactionRetryableException(String message) {
        super(message);
    }

    public CompactionRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
