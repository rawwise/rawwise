package io.github.rawwise.common.exception;

public final class InvalidMetadataException extends CatalogException {

    private static final long serialVersionUID = 1L;

    private final String metadataLocation;

    public InvalidMetadataException(String message) {
        super(message);
        this.metadataLocation = null;
    }

    public InvalidMetadataException(String message, String metadataLocation) {
        super(message);
        this.metadataLocation = metadataLocation;
    }

    public InvalidMetadataException(String message, Throwable cause) {
        super(message, cause);
        this.metadataLocation = null;
    }

    public InvalidMetadataException(String message, String metadataLocation, Throwable cause) {
        super(message, cause);
        this.metadataLocation = metadataLocation;
    }

    public String getMetadataLocation() {
        return metadataLocation;
    }
}
