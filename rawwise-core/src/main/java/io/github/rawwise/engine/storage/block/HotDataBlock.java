package io.github.rawwise.engine.storage.block;

import io.github.rawwise.engine.storage.KeyRange;

public record HotDataBlock(
        String blockId,
        String tableId,
        DataBlock.Kind kind,
        DataBlock.Format format,
        DataBlock.Location location,
        KeyRange keyRange,
        long rowCount,
        long sizeBytes,
        long schemaVersion,
        DataBlock.Visibility visibility,
        long minSequenceNumber,
        long maxSequenceNumber,
        long mappedOffset,
        long mappedLength,
        String regionKey)
        implements DataBlock {}
