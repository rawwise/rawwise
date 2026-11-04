package io.github.rawwise.axiom.pipeline.graph.distributed;

public enum BloomFilterSyncStrategy {
    PUSH_30S,
    PUSH_60S,
    PULL_BY_TYPE;

    public static BloomFilterSyncStrategy forEntityCount(long entityCount) {
        if (entityCount < 1_000_000L) return PUSH_30S;
        if (entityCount <= 10_000_000L) return PUSH_60S;
        return PULL_BY_TYPE;
    }
}
