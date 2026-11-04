package io.github.rawwise.engine.compaction.strategy;

import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.KeyRange;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LeveledStrategy implements CompactionStrategy, Serializable {

    private final int fileCountLimit;
    private final double sizeMultiplier;
    private final int maxLevel;
    private final long baseFileSizeBytes;

    public LeveledStrategy(
            int fileCountLimit, double sizeMultiplier, int maxLevel, long baseFileSizeBytes) {
        this.fileCountLimit = fileCountLimit;
        this.sizeMultiplier = sizeMultiplier;
        this.maxLevel = maxLevel;
        this.baseFileSizeBytes = baseFileSizeBytes;
    }

    public LeveledStrategy() {
        this(10, 10.0, 3, 256L * 1024 * 1024);
    }

    @Override
    public CompactionStrategy.StrategyType strategyType() {
        return StrategyType.LEVELED;
    }

    @Override
    public List<SourceGroup> plan(
            List<DataFile> l0Files, List<DataFile> l1Files, List<DataFile> l2Files) {
        List<SourceGroup> groups = new ArrayList<>();
        List<List<DataFile>> allLevels = List.of(l0Files, l1Files, l2Files);

        for (int level = 0; level < maxLevel; level++) {
            List<DataFile> current = allLevels.get(level);
            if (current.size() < fileCountLimit) {
                continue;
            }
            int nextLevel = level + 1;
            if (nextLevel >= allLevels.size()) {
                break;
            }
            List<DataFile> nextFiles = allLevels.get(nextLevel);

            List<DataFile> candidates = new ArrayList<>(current);

            List<DataFile> overlappingNext = new ArrayList<>();
            for (DataFile nf : nextFiles) {
                for (DataFile candidate : candidates) {
                    if (keyRangesOverlap(candidate.keyRange(), nf.keyRange())) {
                        if (!overlappingNext.contains(nf)) {
                            overlappingNext.add(nf);
                        }
                    }
                }
            }

            List<String> paths = new ArrayList<>();
            long totalBytes = 0;
            long totalRows = 0;
            for (DataFile candidate : candidates) {
                paths.add(candidate.filePath());
                totalBytes += candidate.fileSizeBytes();
                totalRows += candidate.rowCount();
            }
            for (DataFile nf : overlappingNext) {
                paths.add(nf.filePath());
                totalBytes += nf.fileSizeBytes();
                totalRows += nf.rowCount();
            }

            groups.add(new SourceGroup(nextLevel, paths, totalBytes, totalRows));
        }

        return groups;
    }

    private boolean keyRangesOverlap(KeyRange a, KeyRange b) {
        if (a == null || b == null) {
            return true;
        }
        return a.overlaps(b);
    }

    @Override
    public int maxLevel() {
        return maxLevel;
    }

    @Override
    public long maxLevelBytes(int level) {
        return (long) (baseFileSizeBytes * Math.pow(sizeMultiplier, level));
    }

    @Override
    public int levelFileCountLimit(int level) {
        return fileCountLimit;
    }
}
