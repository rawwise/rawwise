package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.model.record.Chunk;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CodeBlockChunkingStrategy implements ChunkingStrategy {

    private static final Pattern FUNCTION_HEADER =
            Pattern.compile(
                    "^\\s*(public|private|protected|static|def|func|function|class|struct|interface|enum)\\s+",
                    Pattern.MULTILINE);

    @Override
    public String name() {
        return "code-block";
    }

    @Override
    public Chunk.ChunkType supportedType() {
        return Chunk.ChunkType.CODE_BLOCK;
    }

    @Override
    public List<byte[]> split(byte[] content, Map<String, Object> params) {
        String code = new String(content, StandardCharsets.UTF_8);
        List<int[]> ranges = findCodeBlockRanges(code);
        if (ranges.isEmpty()) {
            return List.of(content);
        }

        List<byte[]> chunks = new ArrayList<>();
        for (int[] range : ranges) {
            String block = code.substring(range[0], range[1]).trim();
            if (!block.isEmpty()) {
                chunks.add(block.getBytes(StandardCharsets.UTF_8));
            }
        }

        return chunks.isEmpty() ? List.of(content) : chunks;
    }

    List<int[]> findCodeBlockRanges(String code) {
        List<int[]> ranges = new ArrayList<>();
        Matcher matcher = FUNCTION_HEADER.matcher(code);
        List<Integer> startPositions = new ArrayList<>();

        while (matcher.find()) {
            startPositions.add(matcher.start());
        }

        if (startPositions.isEmpty()) {
            return ranges;
        }

        for (int i = 0; i < startPositions.size(); i++) {
            int start = startPositions.get(i);
            int end = (i + 1 < startPositions.size()) ? startPositions.get(i + 1) : code.length();
            ranges.add(new int[] {start, end});
        }

        return ranges;
    }
}
