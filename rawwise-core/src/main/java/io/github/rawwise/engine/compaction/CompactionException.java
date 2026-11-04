package io.github.rawwise.engine.compaction;

import java.io.IOException;

public class CompactionException extends IOException {

    public CompactionException(String message) {
        super(message);
    }

    public CompactionException(String message, Throwable cause) {
        super(message, cause);
    }

    public boolean isRetryable() {
        return this instanceof CompactionRetryableException;
    }

    public boolean isFatal() {
        return this instanceof CompactionFatalException;
    }
}
