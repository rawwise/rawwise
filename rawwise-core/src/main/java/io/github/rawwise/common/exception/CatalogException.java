package io.github.rawwise.common.exception;

public sealed class CatalogException extends RuntimeException
        permits TableNotFoundException,
                TableAlreadyExistsException,
                CatalogIOException,
                CommitConflictException,
                NamespaceNotEmptyException,
                SchemaIncompatibleException,
                InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    protected CatalogException(String message) {
        super(message);
    }

    protected CatalogException(String message, Throwable cause) {
        super(message, cause);
    }

    protected CatalogException(Throwable cause) {
        super(cause);
    }
}
