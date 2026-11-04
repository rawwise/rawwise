package io.github.rawwise.engine.storage.ingestion;

public final class SinkFatalException extends SinkException {
    public SinkFatalException(String message) {
        super(message);
    }

    public SinkFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
