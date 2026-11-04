package io.github.rawwise.engine.compaction.merger;

public record MergeResult(long writtenRows) {

    public MergeResult {
        if (writtenRows < 0) {
            throw new IllegalArgumentException("writtenRows must be non-negative: " + writtenRows);
        }
    }
}
