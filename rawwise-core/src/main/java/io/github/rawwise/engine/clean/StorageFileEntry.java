package io.github.rawwise.engine.clean;

import java.io.Serializable;

public record StorageFileEntry(String path, long sizeBytes, long lastModifiedMillis)
        implements Serializable {

    public StorageFileEntry {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path must not be null or empty");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must not be negative");
        }
        if (lastModifiedMillis < 0) {
            throw new IllegalArgumentException("lastModifiedMillis must not be negative");
        }
    }
}
