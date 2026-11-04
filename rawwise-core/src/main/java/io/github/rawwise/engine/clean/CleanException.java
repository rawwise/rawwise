package io.github.rawwise.engine.clean;

import java.io.IOException;

public class CleanException extends IOException {

    public CleanException(String message) {
        super(message);
    }

    public CleanException(String message, Throwable cause) {
        super(message, cause);
    }

    public boolean isRetryable() {
        return this instanceof CleanRetryableException;
    }

    public boolean isFatal() {
        return this instanceof CleanFatalException;
    }
}
