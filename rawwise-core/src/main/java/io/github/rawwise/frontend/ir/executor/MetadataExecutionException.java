package io.github.rawwise.frontend.ir.executor;

public class MetadataExecutionException extends RuntimeException {
    public MetadataExecutionException(String message) {
        super(message);
    }

    public MetadataExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
