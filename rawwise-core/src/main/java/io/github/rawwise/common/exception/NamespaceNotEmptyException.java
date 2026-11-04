package io.github.rawwise.common.exception;

public final class NamespaceNotEmptyException extends CatalogException {

    private static final long serialVersionUID = 1L;

    public NamespaceNotEmptyException(String message) {
        super(message);
    }

    public NamespaceNotEmptyException(String message, Throwable cause) {
        super(message, cause);
    }
}
