package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.model.record.Chunk;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableRowChunkingStrategy implements ChunkingStrategy {

    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    @Override
    public String name() {
        return "table-row";
    }

    @Override
    public Chunk.ChunkType supportedType() {
        return Chunk.ChunkType.TABLE_ROW;
    }

    @Override
    public List<byte[]> split(byte[] content, Map<String, Object> params) {
        if (content == null || content.length == 0) {
            return List.of();
        }
        String text = new String(content, StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) {
            return List.of();
        }
        if (text.startsWith("[")) {
            try {
                List<?> rows =
                        MAPPER.readValue(
                                text,
                                MAPPER.getTypeFactory()
                                        .constructCollectionType(List.class, Object.class));
                List<byte[]> chunks = new ArrayList<>();
                for (Object row : rows) {
                    byte[] rowBytes = MAPPER.writeValueAsBytes(row);
                    chunks.add(rowBytes);
                }
                return chunks.isEmpty() ? List.of(content) : chunks;
            } catch (Exception e) {
                return List.of(content);
            }
        }
        return List.of(content);
    }
}
