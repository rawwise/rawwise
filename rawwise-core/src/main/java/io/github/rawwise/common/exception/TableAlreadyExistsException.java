package io.github.rawwise.common.exception;

public final class TableAlreadyExistsException extends CatalogException {

    private static final long serialVersionUID = 1L;

    public TableAlreadyExistsException(String message) {
        super(message);
    }

    public TableAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
