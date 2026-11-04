package io.github.rawwise.engine.vector;

import java.util.Collection;
import java.util.List;

public interface VectorIndex extends AutoCloseable {

    void index(String namespace, String entryKey, String l0Abstract);

    void indexWithEmbedding(String namespace, String entryKey, float[] embedding);

    default void indexWithEmbedding(
            String namespace, String entryKey, float[] embedding, String l0Abstract) {
        indexWithEmbedding(namespace, entryKey, embedding);
    }

    List<VectorSearchResult> search(String namespace, float[] queryEmbedding, int topK);

    default List<VectorSearchResult> search(
            String namespace, float[] queryEmbedding, int topK, long snapshotId) {
        return search(namespace, queryEmbedding, topK);
    }

    void delete(String namespace, String entryKey);

    void flushToCold(String namespace, Collection<String> entryKeys);

    void setColdStore(VectorColdStore coldStore);

    default void persist() {}

    @Override
    default void close() {}
}
