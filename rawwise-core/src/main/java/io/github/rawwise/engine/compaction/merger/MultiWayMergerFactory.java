package io.github.rawwise.engine.compaction.merger;

public final class MultiWayMergerFactory {

    private static final int LOSER_TREE_THRESHOLD = 20;

    private MultiWayMergerFactory() {}

    public static MultiWayMerger create(int sourceFileCount) {
        return sourceFileCount >= LOSER_TREE_THRESHOLD
                ? new LoserTreeMerger()
                : new MinHeapMerger();
    }
}
