package io.github.rawwise.common.exception;

public final class CatalogIOException extends CatalogException {

    private static final long serialVersionUID = 1L;

    private final String storageSystem;

    public CatalogIOException(String message) {
        super(message);
        this.storageSystem = null;
    }

    public CatalogIOException(String message, String storageSystem) {
        super(message);
        this.storageSystem = storageSystem;
    }

    public CatalogIOException(String message, Throwable cause) {
        super(message, cause);
        this.storageSystem = null;
    }

    public CatalogIOException(String message, String storageSystem, Throwable cause) {
        super(message, cause);
        this.storageSystem = storageSystem;
    }

    public String getStorageSystem() {
        return storageSystem;
    }
}
