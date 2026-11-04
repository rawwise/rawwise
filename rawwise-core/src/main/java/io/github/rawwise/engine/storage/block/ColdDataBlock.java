package io.github.rawwise.engine.storage.block;

import io.github.rawwise.engine.storage.KeyRange;
import java.util.Map;

public record ColdDataBlock(
        String blockId,
        String tableId,
        DataBlock.Kind kind,
        DataBlock.Format format,
        DataBlock.Format fileFormat,
        DataBlock.Location location,
        KeyRange keyRange,
        long rowCount,
        long sizeBytes,
        long schemaVersion,
        long snapshotId,
        Map<String, String> partitionValues,
        Statistics statistics,
        DataBlock.Visibility visibility,
        long minSequenceNumber,
        long maxSequenceNumber,
        int compactionLevel,
        byte[] bloomFilter,
        byte[] rowGroupIndex)
        implements DataBlock {
    public ColdDataBlock {
        if (compactionLevel < -1) {
            throw new IllegalArgumentException(
                    "compactionLevel must be >= -1, got: " + compactionLevel);
        }
    }

    public record Statistics(
            long rowCount, long sizeBytes, long minSequenceNumber, long maxSequenceNumber) {}
}
