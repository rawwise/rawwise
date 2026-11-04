package io.github.rawwise.engine.vector;

import java.util.List;

@FunctionalInterface
public interface ShardClient {

    List<SearchCandidate> search(String tableId, String shardId, float[] queryEmbedding, int topK);

    default boolean isAvailable(String shardId) {
        return true;
    }

    static ShardClient local(ShardInternalIndex localIndex) {
        return (tableId, shardId, query, topK) -> localIndex.search(query, topK);
    }
}
