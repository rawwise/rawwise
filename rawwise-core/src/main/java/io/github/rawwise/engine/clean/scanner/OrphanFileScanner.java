package io.github.rawwise.engine.clean.scanner;

import io.github.rawwise.engine.clean.StorageFileEntry;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrphanFileScanner {

    private final SnapshotManager snapshotManager;
    private final Storage storage;
    private final String scheme;
    private final List<String> scanDirectories;

    public OrphanFileScanner(
            SnapshotManager snapshotManager,
            Storage storage,
            String scheme,
            List<String> scanDirectories) {
        this.snapshotManager =
                Objects.requireNonNull(snapshotManager, "snapshotManager must not be null");
        this.storage = Objects.requireNonNull(storage, "storage must not be null");
        if (scheme == null || scheme.isEmpty()) {
            throw new IllegalArgumentException("scheme must not be null or empty");
        }
        this.scheme = scheme;
        if (scanDirectories != null && !scanDirectories.isEmpty()) {
            for (String dir : scanDirectories) {
                if (dir == null || dir.isBlank()) {
                    throw new IllegalArgumentException(
                            "scanDirectory entry must not be null or blank");
                }
                if (dir.contains("..")) {
                    throw new IllegalArgumentException(
                            "scanDirectory must not contain path traversal: " + dir);
                }
            }
        }
        this.scanDirectories =
                (scanDirectories == null || scanDirectories.isEmpty())
                        ? List.of("data/", "log/", "delete_vector/")
                        : List.copyOf(scanDirectories);
    }

    public List<StorageFileEntry> scanOrphanFiles(
            String tableId, long minOrphanAgeMs, long currentTimestampMs) throws IOException {
        validateArgs(tableId, minOrphanAgeMs, currentTimestampMs);
        Set<String> referencedPaths = getReferencedPaths(tableId);
        List<StorageFileEntry> storageFiles = listStorageFiles(tableId);
        return filterOrphans(referencedPaths, storageFiles, currentTimestampMs - minOrphanAgeMs);
    }

    public List<StorageFileEntry> scanOrphanFiles(
            String tableId,
            List<StorageFileEntry> storageFiles,
            long minOrphanAgeMs,
            long currentTimestampMs)
            throws IOException {
        validateArgs(tableId, minOrphanAgeMs, currentTimestampMs);
        Objects.requireNonNull(storageFiles, "storageFiles must not be null");
        Set<String> referencedPaths = getReferencedPaths(tableId);
        return filterOrphans(referencedPaths, storageFiles, currentTimestampMs - minOrphanAgeMs);
    }

    private void validateArgs(String tableId, long minOrphanAgeMs, long currentTimestampMs) {
        if (tableId == null || tableId.isEmpty()) {
            throw new IllegalArgumentException("tableId must not be null or empty");
        }
        if (minOrphanAgeMs < 0) {
            throw new IllegalArgumentException("minOrphanAgeMs must not be negative");
        }
        if (currentTimestampMs <= 0) {
            throw new IllegalArgumentException("currentTimestampMs must be positive");
        }
    }

    private List<StorageFileEntry> filterOrphans(
            Set<String> referencedPaths, List<StorageFileEntry> candidates, long cutoffTime) {
        List<StorageFileEntry> orphans = new ArrayList<>();
        for (StorageFileEntry entry : candidates) {
            if (!referencedPaths.contains(entry.path())
                    && entry.lastModifiedMillis() < cutoffTime) {
                orphans.add(entry);
            }
        }
        return orphans;
    }

    private Set<String> getReferencedPaths(String tableId) throws IOException {
        Set<String> paths = new HashSet<>();
        List<Snapshot> snapshots = snapshotManager.getSnapshots(tableId);
        if (snapshots == null) return paths;

        for (Snapshot snapshot : snapshots) {
            List<DataFile> dataFiles =
                    snapshotManager.listDataFiles(tableId, snapshot.snapshotId());
            if (dataFiles != null) dataFiles.forEach(f -> paths.add(f.filePath()));
            List<LogFile> logFiles = snapshotManager.listLogFiles(tableId, snapshot.snapshotId());
            if (logFiles != null) logFiles.forEach(f -> paths.add(f.filePath()));
        }
        return paths;
    }

    private List<StorageFileEntry> listStorageFiles(String tableId) throws IOException {
        List<StorageFileEntry> entries = new ArrayList<>();
        for (String dir : scanDirectories) {
            Storage.StoragePath prefix = new Storage.StoragePath(scheme, tableId + "/" + dir);
            List<Storage.StoragePath> paths = storage.list(prefix);
            if (paths == null) continue;
            for (Storage.StoragePath path : paths) {
                try {
                    Storage.StorageStatus status = storage.status(path);
                    entries.add(
                            new StorageFileEntry(
                                    path.location(), status.length(), status.lastModifiedMillis()));
                } catch (IOException e) {
                    log.warn("Failed to get status for storage path {}", path, e);
                }
            }
        }
        return entries;
    }
}
