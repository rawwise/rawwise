package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.op.KvScan;
import io.github.rawwise.model.op.Scan;
import io.github.rawwise.model.record.MapRecord;
import java.util.List;
import java.util.stream.Collectors;

final class ColdBlockFilters {
    private ColdBlockFilters() {}

    static List<ColdDataBlock> visibleBlocks(
            List<DataBlock> blocks, long requestedSnapshotId, Expression predicate) {
        return blocks.stream()
                .filter(ColdDataBlock.class::isInstance)
                .map(ColdDataBlock.class::cast)
                .filter(block -> isSnapshotVisible(block, requestedSnapshotId))
                .filter(block -> matchesPartitionValues(block, predicate))
                .collect(Collectors.toList());
    }

    static long requestedSnapshotId(Scan scan) {
        if (scan instanceof KvScan kvScan) {
            return kvScan.snapshotId();
        }
        return -1L;
    }

    private static boolean isSnapshotVisible(ColdDataBlock block, long requestedSnapshotId) {
        if (block.visibility() == DataBlock.Visibility.DELETED
                || block.visibility() == DataBlock.Visibility.FLUSHING
                || block.visibility() == DataBlock.Visibility.PENDING) {
            return false;
        }
        if (requestedSnapshotId < 0 || block.snapshotId() < 0) {
            return true;
        }
        return block.snapshotId() <= requestedSnapshotId;
    }

    private static boolean matchesPartitionValues(ColdDataBlock block, Expression predicate) {
        if (predicate == null || block.partitionValues().isEmpty()) {
            return true;
        }
        try {
            return predicate.evaluate(MapRecord.fromMap(block.partitionValues()));
        } catch (RuntimeException ignored) {
            return true;
        }
    }
}
