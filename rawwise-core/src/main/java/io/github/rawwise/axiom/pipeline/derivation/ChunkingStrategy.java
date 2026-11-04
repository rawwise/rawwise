package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.model.record.Chunk;
import io.github.rawwise.model.record.Record;
import java.util.List;
import java.util.Map;

public interface ChunkingStrategy {

    String name();

    Chunk.ChunkType supportedType();

    default boolean appliesTo(Record record, String tableId) {
        return true;
    }

    List<byte[]> split(byte[] content, Map<String, Object> params);

    default List<ChunkMeta> splitWithMeta(byte[] content, Map<String, Object> params) {
        return split(content, params).stream()
                .map(bytes -> new ChunkMeta(bytes, Map.of()))
                .toList();
    }
}
