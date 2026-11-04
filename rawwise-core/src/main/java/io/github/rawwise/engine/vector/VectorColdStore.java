package io.github.rawwise.engine.vector;

import java.util.Collection;
import java.util.List;

public interface VectorColdStore extends AutoCloseable {

    void write(String namespace, String blockId, List<ColdVectorEntry> entries);

    List<VectorSearchResult> search(String namespace, float[] queryEmbedding, int topK);

    void delete(String namespace, Collection<String> entryKeys);

    boolean hasColdData(String namespace);

    default float[] readVector(String namespace, String entryKey) {
        return null;
    }

    default String readL0Abstract(String namespace, String entryKey) {
        return null;
    }

    @Override
    default void close() {}
}
