package io.github.rawwise.engine.vector;

public class VectorSearchUnavailableException extends RuntimeException {
    public VectorSearchUnavailableException(String message) {
        super(message);
    }

    public VectorSearchUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
