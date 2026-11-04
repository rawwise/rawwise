package io.github.rawwise.engine.storage.ingestion;

public final class SinkRetryableException extends SinkException {
    public SinkRetryableException(String message) {
        super(message);
    }

    public SinkRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
