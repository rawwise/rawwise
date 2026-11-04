package io.github.rawwise.engine.clean.executor;

import io.github.rawwise.engine.clean.CleanFatalException;
import io.github.rawwise.engine.clean.CleanRetryableException;

public class CleanExecutionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CleanExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public boolean isRetryable() {
        return getCause() instanceof CleanRetryableException;
    }

    public boolean isFatal() {
        return getCause() instanceof CleanFatalException;
    }
}
