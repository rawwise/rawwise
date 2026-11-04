package io.github.rawwise.engine.clean;

public class CleanRetryableException extends CleanException {

    public CleanRetryableException(String message) {
        super(message);
    }

    public CleanRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
