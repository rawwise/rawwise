package io.github.rawwise.common.exception;

public final class SchemaIncompatibleException extends CatalogException {

    private static final long serialVersionUID = 1L;

    public SchemaIncompatibleException(String message) {
        super(message);
    }

    public SchemaIncompatibleException(String message, Throwable cause) {
        super(message, cause);
    }
}
