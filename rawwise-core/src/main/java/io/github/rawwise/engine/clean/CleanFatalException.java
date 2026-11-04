package io.github.rawwise.engine.clean;

public class CleanFatalException extends CleanException {

    public CleanFatalException(String message) {
        super(message);
    }

    public CleanFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
