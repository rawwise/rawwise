package io.github.rawwise.engine.compaction.cleanup;

import io.github.rawwise.engine.storage.spi.Storage;
import java.nio.file.Path;
import java.util.Set;

public final class StoragePathValidator {

    private static final Set<String> ALLOWED_SCHEMES = Set.of("s3", "s3a", "gs", "hdfs", "file");
    private static final Set<String> METADATA_DIRS = Set.of("metadata", "_metadata", ".metadata");

    private StoragePathValidator() {}

    public static void validate(Storage.StoragePath sp, String tableId) throws SecurityException {
        if (sp == null) {
            throw new SecurityException("StoragePath must not be null");
        }
        if (tableId == null || tableId.isEmpty()) {
            throw new SecurityException("tableId must not be null or empty");
        }
        if (!ALLOWED_SCHEMES.contains(sp.scheme())) {
            throw new SecurityException(
                    "Forbidden scheme: " + sp.scheme() + " for table " + tableId);
        }
        String loc = sp.location();
        String normalized = Path.of(loc).normalize().toString();
        if (!normalized.equals(loc)) {
            throw new SecurityException(
                    "Path traversal via normalization: " + loc + " -> " + normalized);
        }
        if (!normalized.startsWith(tableId + "/")) {
            throw new SecurityException(
                    "Path outside table directory: " + loc + " for table " + tableId);
        }
        for (String segment : normalized.split("/")) {
            if (METADATA_DIRS.contains(segment)) {
                throw new SecurityException("Cannot delete metadata directory: " + loc);
            }
        }
    }
}
