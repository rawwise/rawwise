package io.github.rawwise.engine.storage.flush;

import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.block.DataBlock;
import java.util.List;

public record FlushPlan(
        String planId,
        String tableId,
        List<String> sourceHotBlockIds,
        DataBlock.Location targetLocation,
        DataBlock.Format targetFormat,
        KeyRange targetKeyRange,
        long estimatedRows,
        long schemaVersion,
        long createdAt) {}
