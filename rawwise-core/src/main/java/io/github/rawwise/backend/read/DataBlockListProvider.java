package io.github.rawwise.backend.read;

import io.github.rawwise.engine.storage.block.DataBlock;
import java.io.Serializable;
import java.util.List;

public interface DataBlockListProvider extends Serializable {
    List<DataBlock> listBlocks(String basePath, String storeId, String tableIdentifier);
}
