package io.github.rawwise.engine.storage;

/**
 * Thrown when a storage-level operation fails due to corruption, checksum mismatch, IO error, or
 * irrecoverable data integrity violation.
 */
public class StorageException extends RuntimeException {

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
