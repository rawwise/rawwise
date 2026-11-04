package io.github.rawwise.engine.compaction;

public class CompactionFatalException extends CompactionException {

    public CompactionFatalException(String message) {
        super(message);
    }

    public CompactionFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
