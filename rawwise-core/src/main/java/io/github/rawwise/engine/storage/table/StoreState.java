package io.github.rawwise.engine.storage.table;

import io.github.rawwise.common.collection.cache.SortedCache;
import io.github.rawwise.engine.storage.block.DataBlock;
import java.util.ArrayList;
import java.util.List;

public class StoreState {

    private final SortedCache<Long, List<DataBlock>> state = new SortedCache<>(2000);

    public List<DataBlock> commitState(long commitId) {
        List<DataBlock> value = state.get(commitId);
        if (value == null) {
            return new ArrayList<>();
        }
        return List.copyOf(value);
    }
}
