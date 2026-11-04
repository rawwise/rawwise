package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.model.record.Chunk;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SlidingWindowChunkingStrategy implements ChunkingStrategy {

    private static final int DEFAULT_MAX_CHUNK_SIZE = 1024;
    private static final int DEFAULT_OVERLAP = 128;

    @Override
    public String name() {
        return "sliding-window";
    }

    @Override
    public Chunk.ChunkType supportedType() {
        return Chunk.ChunkType.TEXT_SEGMENT;
    }

    @Override
    public List<byte[]> split(byte[] content, Map<String, Object> params) {
        String text = new String(content, StandardCharsets.UTF_8);
        int maxChunkSize = getIntParam(params, "maxChunkSize", DEFAULT_MAX_CHUNK_SIZE);
        int overlap = getIntParam(params, "chunkOverlap", DEFAULT_OVERLAP);
        if (maxChunkSize <= 0) {
            maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;
        }
        if (overlap < 0) {
            overlap = DEFAULT_OVERLAP;
        }
        if (text.length() <= maxChunkSize) {
            return List.of(content);
        }
        int step = Math.max(1, maxChunkSize - overlap);
        List<byte[]> chunks = new ArrayList<>();
        int start = 0;
        int len = text.length();
        while (start < len) {
            int end = Math.min(start + maxChunkSize, len);
            chunks.add(text.substring(start, end).getBytes(StandardCharsets.UTF_8));
            if (end >= len) {
                break;
            }
            start += step;
        }
        return chunks;
    }

    private static int getIntParam(Map<String, Object> params, String key, int defaultValue) {
        Object val = params != null ? params.get(key) : null;
        if (val instanceof Number n) {
            return n.intValue();
        }
        return defaultValue;
    }
}
