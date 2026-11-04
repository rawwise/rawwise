package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.model.record.Chunk;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SentenceChunkingStrategy implements ChunkingStrategy {

    private static final Pattern SENTENCE_BOUNDARY =
            Pattern.compile("(?<=[.!?。！？])\\s*(?=[A-Z\\p{Lu}\\p{Lo}])");
    private static final int DEFAULT_MAX_SENTENCES = 5;

    @Override
    public String name() {
        return "sentence";
    }

    @Override
    public Chunk.ChunkType supportedType() {
        return Chunk.ChunkType.TEXT_SEGMENT;
    }

    @Override
    public List<byte[]> split(byte[] content, Map<String, Object> params) {
        String text = new String(content, StandardCharsets.UTF_8);
        int maxSentences = getIntParam(params, "maxSentences", DEFAULT_MAX_SENTENCES);

        String[] sentences = SENTENCE_BOUNDARY.split(text);
        if (sentences.length == 0) {
            sentences = new String[] {text};
        }

        List<byte[]> chunks = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int count = 0;

        for (String sentence : sentences) {
            if (count >= maxSentences && count > 0) {
                chunks.add(current.toString().getBytes(StandardCharsets.UTF_8));
                current = new StringBuilder();
                count = 0;
            }
            if (current.length() > 0) {
                current.append(" ");
            }
            current.append(sentence);
            count++;
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
}
