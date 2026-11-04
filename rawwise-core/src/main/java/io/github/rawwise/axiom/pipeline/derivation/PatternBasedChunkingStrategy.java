package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.model.record.Chunk;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class PatternBasedChunkingStrategy implements ChunkingStrategy {

    private static final int MAX_PATTERN_CHUNK_BYTES = 1024;
    private final SemanticPatternRenderer renderer = new DefaultSemanticPatternRenderer();

    @Override
    public String name() {
        return "pattern";
    }

    @Override
    public Chunk.ChunkType supportedType() {
        return Chunk.ChunkType.TEXT_SEGMENT;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<byte[]> split(byte[] content, Map<String, Object> params) {
        byte[] renderedBytes = null;
        if (params != null) {
            Object rendered = params.get("renderedContent");
            if (rendered instanceof byte[] bytes && bytes.length > 0) {
                renderedBytes = bytes;
            }
        }
        if (renderedBytes == null) {
            String pattern = params != null ? (String) params.get("semantic.pattern") : null;
            Map<String, Object> fields = null;
            if (params != null) {
                Object f = params.get("record.fields");
                if (f instanceof Map<?, ?> m) {
                    fields = (Map<String, Object>) m;
                }
            }
            if (pattern != null && !pattern.isBlank() && fields != null && !fields.isEmpty()) {
                String renderedStr = renderer.render(pattern, fields);
                if (!renderedStr.isBlank()) {
                    renderedBytes = renderedStr.getBytes(StandardCharsets.UTF_8);
                }
            }
        }
        if (renderedBytes == null) {
            renderedBytes = content;
        }
        if (renderedBytes.length <= MAX_PATTERN_CHUNK_BYTES) {
            return List.of(renderedBytes);
        }
        return splitByBytes(renderedBytes, MAX_PATTERN_CHUNK_BYTES);
    }

    private static List<byte[]> splitByBytes(byte[] data, int maxBytes) {
        List<byte[]> chunks = new java.util.ArrayList<>();
        int start = 0;
        while (start < data.length) {
            int end = Math.min(start + maxBytes, data.length);
            byte[] chunk = new byte[end - start];
            System.arraycopy(data, start, chunk, 0, chunk.length);
            chunks.add(chunk);
            start = end;
        }
        return chunks;
    }
}
