package io.github.rawwise.engine.storage;

import java.util.Map;

public record ParquetDataFile(
        String filePath,
        long fileSizeBytes,
        long createdAtMillis,
        String tableId,
        long snapshotId,
        KeyRange keyRange,
        long rowCount,
        long schemaVersion,
        long minSequenceNumber,
        long maxSequenceNumber,
        Map<String, String> partitionValues,
        ColumnStatistics columnStatistics,
        int compactionLevel)
        implements DataFile {

    public ParquetDataFile {
        partitionValues = partitionValues == null ? Map.of() : Map.copyOf(partitionValues);
        if (compactionLevel < -1) {
            throw new IllegalArgumentException(
                    "compactionLevel must be >= -1, got: " + compactionLevel);
        }
    }

    @Override
    public FileFormat format() {
        return FileFormat.PARQUET;
    }
}
