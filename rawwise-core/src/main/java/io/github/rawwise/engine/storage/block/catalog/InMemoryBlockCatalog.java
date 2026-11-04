package io.github.rawwise.engine.storage.block.catalog;

import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.HotDataBlock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemoryBlockCatalog implements BlockCatalog {
    private final String tableId;
    private final ConcurrentNavigableMap<String, DataBlock> blocks = new ConcurrentSkipListMap<>();

    public InMemoryBlockCatalog(String tableId) {
        this.tableId = tableId;
        log.warn(
                "InMemoryBlockCatalog is purely in-memory for table '{}'. Block catalog state will be lost on process restart.",
                tableId);
    }

    @Override
    public String tableId() {
        return tableId;
    }

    @Override
    public void upsert(DataBlock block) {
        blocks.put(block.blockId(), block);
    }

    @Override
    public Optional<DataBlock> find(String blockId) {
        return Optional.ofNullable(blocks.get(blockId));
    }

    @Override
    public void updateVisibility(String blockId, DataBlock.Visibility visibility) {
        blocks.computeIfPresent(
                blockId,
                (ignored, block) -> {
                    if (block.visibility() == visibility) {
                        return block;
                    }
                    block.visibility().validateTransitionTo(visibility);
                    return switch (block) {
                        case HotDataBlock hot ->
                                new HotDataBlock(
                                        hot.blockId(),
                                        hot.tableId(),
                                        hot.kind(),
                                        hot.format(),
                                        hot.location(),
                                        hot.keyRange(),
                                        hot.rowCount(),
                                        hot.sizeBytes(),
                                        hot.schemaVersion(),
                                        visibility,
                                        hot.minSequenceNumber(),
                                        hot.maxSequenceNumber(),
                                        hot.mappedOffset(),
                                        hot.mappedLength(),
                                        hot.regionKey());
                        case ColdDataBlock cold ->
                                new ColdDataBlock(
                                        cold.blockId(),
                                        cold.tableId(),
                                        cold.kind(),
                                        cold.format(),
                                        cold.fileFormat(),
                                        cold.location(),
                                        cold.keyRange(),
                                        cold.rowCount(),
                                        cold.sizeBytes(),
                                        cold.schemaVersion(),
                                        cold.snapshotId(),
                                        cold.partitionValues(),
                                        cold.statistics(),
                                        visibility,
                                        cold.minSequenceNumber(),
                                        cold.maxSequenceNumber(),
                                        cold.compactionLevel(),
                                        cold.bloomFilter(),
                                        cold.rowGroupIndex());
                    };
                });
    }

    @Override
    public void remove(String blockId) {
        blocks.remove(blockId);
    }

    @Override
    public List<DataBlock> currentBlocks() {
        return blocks.descendingMap().values().stream()
                .filter(
                        block ->
                                block.visibility() != DataBlock.Visibility.DELETED
                                        && block.visibility() != DataBlock.Visibility.PENDING)
                .toList();
    }

    @Override
    public List<DataBlock> pendingBlocks() {
        return blocks.descendingMap().values().stream()
                .filter(block -> block.visibility() == DataBlock.Visibility.PENDING)
                .toList();
    }

    @Override
    public synchronized void batchUpdateVisibility(
            List<String> blockIds, DataBlock.Visibility from, DataBlock.Visibility to) {
        for (String id : blockIds) {
            DataBlock block = blocks.get(id);
            if (block != null && block.visibility() == from) {
                updateVisibility(id, to);
            }
        }
    }
}
