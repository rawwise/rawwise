package io.github.rawwise.engine.clean;

import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.model.record.Snapshot;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record CleanContext(
        String tableId,
        List<Snapshot> snapshots,
        List<DataFile> dataFiles,
        List<LogFile> logFiles,
        List<StorageFileEntry> storageFileEntries,
        long currentTimestampMs)
        implements Serializable {

    public CleanContext {
        if (tableId == null || tableId.isEmpty()) {
            throw new IllegalArgumentException("tableId must not be null or empty");
        }
        if (currentTimestampMs <= 0) {
            throw new IllegalArgumentException("currentTimestampMs must be positive");
        }
        snapshots = snapshots == null ? List.of() : List.copyOf(snapshots);
        dataFiles = dataFiles == null ? List.of() : List.copyOf(dataFiles);
        logFiles = logFiles == null ? List.of() : List.copyOf(logFiles);
        storageFileEntries =
                storageFileEntries == null ? List.of() : List.copyOf(storageFileEntries);
    }

    public static CleanContext forOrphan(
            String tableId,
            List<StorageFileEntry> storageFiles,
            List<DataFile> manifestDataFiles,
            List<LogFile> manifestLogFiles,
            long currentTimestampMs) {
        return new CleanContext(
                tableId,
                List.of(),
                manifestDataFiles,
                manifestLogFiles,
                storageFiles,
                currentTimestampMs);
    }

    public static CleanContext forTTL(String tableId, List<DataFile> dataFiles, long nowMs) {
        return new CleanContext(tableId, List.of(), dataFiles, List.of(), List.of(), nowMs);
    }

    public static CleanContext forSnapshot(
            String tableId, List<Snapshot> snapshots, long currentTimestampMs) {
        return new CleanContext(
                tableId, snapshots, List.of(), List.of(), List.of(), currentTimestampMs);
    }

    public Set<String> allReferencedPaths() {
        Set<String> paths = new HashSet<>();
        dataFiles.forEach(df -> paths.add(df.filePath()));
        logFiles.forEach(lf -> paths.add(lf.filePath()));
        return paths;
    }
}
