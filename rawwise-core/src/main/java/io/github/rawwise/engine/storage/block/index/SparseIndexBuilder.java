package io.github.rawwise.engine.storage.block.index;

import java.util.ArrayList;
import java.util.List;

public final class SparseIndexBuilder {

    private static final int SPARSE_INDEX_INTERVAL = 1000;

    private final List<SparseIndex.SparseIndexEntry> entries = new ArrayList<>();
    private int rowIndex;

    public void accept(byte[] key) {
        if (rowIndex % SPARSE_INDEX_INTERVAL == 0) {
            entries.add(new SparseIndex.SparseIndexEntry(key.clone(), rowIndex));
        }
        rowIndex++;
    }

    public byte[] build() {
        return SparseIndex.serialize(new SparseIndex(entries));
    }
}
