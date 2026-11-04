package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.model.record.Chunk;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParagraphChunkingStrategy implements ChunkingStrategy {

    private static final int DEFAULT_MAX_CHUNK_SIZE = 1024;

    @Override
    public String name() {
        return "paragraph";
    }

    @Override
    public Chunk.ChunkType supportedType() {
        return Chunk.ChunkType.TEXT_SEGMENT;
    }

    @Override
    public List<byte[]> split(byte[] content, Map<String, Object> params) {
        String text = new String(content, StandardCharsets.UTF_8);
        int maxChunkSize = getIntParam(params, "maxChunkSize", DEFAULT_MAX_CHUNK_SIZE);

        List<byte[]> chunks = new ArrayList<>();
        List<String> paragraphs = splitByDoubleNewline(text);
        StringBuilder current = new StringBuilder();

        for (String para : paragraphs) {
            if (current.length() + para.length() > maxChunkSize && current.length() > 0) {
                chunks.add(current.toString().getBytes(StandardCharsets.UTF_8));
                current = new StringBuilder();
            }
            if (current.length() > 0) {
                current.append("\n\n");
            }
            current.append(para);
        }

        if (current.length() > 0) {
            chunks.add(current.toString().getBytes(StandardCharsets.UTF_8));
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

    static List<String> splitByDoubleNewline(String text) {
        List<String> result = new ArrayList<>();
        int len = text.length();
        int start = 0;
        for (int i = 0; i < len; i++) {
            if (text.charAt(i) == '\n') {
                int j = i + 1;
                while (j < len && text.charAt(j) == '\n') {
                    j++;
                }
                if (j - i >= 2) {
                    result.add(text.substring(start, i));
                    start = j;
                    i = j - 1;
                }
            }
        }
        if (start < len) {
            result.add(text.substring(start));
        }
        return result;
    }
}
