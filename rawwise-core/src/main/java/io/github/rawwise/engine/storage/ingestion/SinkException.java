package io.github.rawwise.engine.storage.ingestion;

import java.io.IOException;

public abstract sealed class SinkException extends IOException
        permits SinkRetryableException, SinkFatalException {

    public SinkException(String message) {
        super(message);
    }

    public SinkException(String message, Throwable cause) {
        super(message, cause);
    }

    public boolean isRetryable() {
        return this instanceof SinkRetryableException;
    }

    public boolean isFatal() {
        return this instanceof SinkFatalException;
    }
}
