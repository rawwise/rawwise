package io.github.rawwise.engine.storage.block.catalog;

import io.github.rawwise.engine.storage.block.DataBlock;
import java.util.List;
import java.util.Optional;

public interface BlockCatalog {
    String tableId();

    void upsert(DataBlock block);

    Optional<DataBlock> find(String blockId);

    void updateVisibility(String blockId, DataBlock.Visibility visibility);

    void batchUpdateVisibility(
            List<String> blockIds, DataBlock.Visibility from, DataBlock.Visibility to);

    void remove(String blockId);

    List<DataBlock> currentBlocks();

    List<DataBlock> pendingBlocks();
}
