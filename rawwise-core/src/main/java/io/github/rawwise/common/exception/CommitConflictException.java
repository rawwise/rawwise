package io.github.rawwise.common.exception;

public final class CommitConflictException extends CatalogException {

    private static final long serialVersionUID = 1L;

    public CommitConflictException(String message) {
        super(message);
    }

    public CommitConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
