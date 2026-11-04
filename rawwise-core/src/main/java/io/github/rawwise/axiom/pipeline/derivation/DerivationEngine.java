package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.model.record.Chunk;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.View;
import java.util.List;
import java.util.Map;

public interface DerivationEngine {

    List<Chunk> deriveChunks(Record.Key rawKey, Chunk.ChunkType type, Map<String, Object> params);

    List<Chunk> deriveChunks(
            Record.Key rawKey,
            Chunk.ChunkType type,
            String strategyName,
            Map<String, Object> params);

    View deriveView(
            List<String> chunkIds,
            View.ViewType type,
            String modelVersion,
            Map<String, Object> params);

    List<DerivationChain> getChain(String entityId);

    List<DerivationChain> getChain(String sourceId, Layer sourceLayer);

    List<DerivationChain> getDerivedFrom(String derivedId, Layer derivedLayer);

    default Chunk.ChunkType resolveChunkType(String strategyName, Chunk.ChunkType fallback) {
        return fallback;
    }

    default ChunkingStrategy selectStrategy(Record record, String tableId, Chunk.ChunkType type) {
        return null;
    }

    default void bindTableStrategy(String tableId, String strategyName) {}

    default void clearDeriveIntent(Record.Key rawKey) {}

    default void clearAllDeriveIntents(Record.Key rawKey) {
        clearDeriveIntent(rawKey);
    }

    default boolean isChunkDerived(Record.Key rawKey) {
        return false;
    }

    default int deleteChunks(Record.Key rawKey) {
        return 0;
    }
}
