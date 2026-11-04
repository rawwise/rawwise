package io.github.rawwise.frontend.backend;

public class BackendUnavailableException extends RuntimeException {
    public BackendUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
