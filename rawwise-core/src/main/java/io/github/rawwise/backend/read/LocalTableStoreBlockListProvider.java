package io.github.rawwise.backend.read;

import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.table.TableStore;
import java.util.List;

public final class LocalTableStoreBlockListProvider implements DataBlockListProvider {
    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(LocalTableStoreBlockListProvider.class);

    @Override
    public List<DataBlock> listBlocks(String basePath, String storeId, String tableIdentifier) {
        log.info(
                "listBlocks: basePath={}, storeId={}, tableIdentifier={}",
                basePath,
                storeId,
                tableIdentifier);
        DynamicMmapStore store = DynamicMmapStore.getInstance(basePath, storeId);
        log.info(
                "listBlocks: DynamicMmapStore key={}, tableStores={}",
                basePath + ":" + storeId,
                store.getAllTableIdentifiers());
        TableStore tableStore = store.tableStore(tableIdentifier);
        List<DataBlock> blocks = tableStore.currentDataBlocks();
        log.info("listBlocks: tableStore={} blocks={}", tableIdentifier, blocks.size());
        return blocks;
    }
}
