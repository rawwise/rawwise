package io.github.rawwise.engine.vector;

public class EmbeddingUnavailableException extends RuntimeException {
    public EmbeddingUnavailableException(String message) {
        super(message);
    }

    public EmbeddingUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
