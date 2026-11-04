package io.github.rawwise.model.record;

public record Chunk(String chunkId, Record.Key rawKey, ChunkType chunkType, byte[] content) {
    public enum ChunkType {
        TEXT_SEGMENT,
        IMAGE_REGION,
        TABLE_ROW,
        CODE_BLOCK
    }
}
