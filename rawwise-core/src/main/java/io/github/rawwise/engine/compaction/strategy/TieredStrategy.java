package io.github.rawwise.engine.compaction.strategy;

import io.github.rawwise.engine.storage.DataFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TieredStrategy implements CompactionStrategy, Serializable {

    private final int l0FileCountThreshold;
    private final long maxFileSizeBytes;
    private final int maxLevel;
    private final double levelSizeMultiplier;

    public TieredStrategy(
            int l0FileCountThreshold,
            long maxFileSizeBytes,
            int maxLevel,
            double levelSizeMultiplier) {
        this.l0FileCountThreshold = l0FileCountThreshold;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.maxLevel = maxLevel;
        this.levelSizeMultiplier = levelSizeMultiplier;
    }

    public TieredStrategy() {
        this(20, 256L * 1024 * 1024, 3, 10.0);
    }

    @Override
    public CompactionStrategy.StrategyType strategyType() {
        return StrategyType.TIERED;
    }

    @Override
    public List<SourceGroup> plan(
            List<DataFile> l0Files, List<DataFile> l1Files, List<DataFile> l2Files) {
        List<SourceGroup> groups = new ArrayList<>();
        if (l0Files.size() < l0FileCountThreshold) {
            return groups;
        }

        List<String> l0Paths = l0Files.stream().map(DataFile::filePath).toList();
        long l0Bytes = l0Files.stream().mapToLong(DataFile::fileSizeBytes).sum();
        long l0Rows = l0Files.stream().mapToLong(DataFile::rowCount).sum();
        groups.add(new SourceGroup(1, l0Paths, l0Bytes, l0Rows));

        int targetLevel = 1;
        List<List<DataFile>> levelFiles = new ArrayList<>();
        levelFiles.add(List.of());
        levelFiles.add(l1Files);
        levelFiles.add(l2Files);

        while (targetLevel < maxLevel) {
            List<DataFile> current = levelFiles.get(targetLevel);
            if (current == null || current.isEmpty()) {
                break;
            }
            long levelBytes = current.stream().mapToLong(DataFile::fileSizeBytes).sum();
            long maxBytes =
                    (long) (maxFileSizeBytes * Math.pow(levelSizeMultiplier, targetLevel - 1));
            if (levelBytes <= maxBytes) {
                break;
            }

            List<String> paths = current.stream().map(DataFile::filePath).toList();
            long bytes = current.stream().mapToLong(DataFile::fileSizeBytes).sum();
            long rows = current.stream().mapToLong(DataFile::rowCount).sum();
            groups.add(new SourceGroup(targetLevel + 1, paths, bytes, rows));
            targetLevel++;
        }

        return groups;
    }

    @Override
    public int maxLevel() {
        return maxLevel;
    }

    @Override
    public long maxLevelBytes(int level) {
        return (long) (maxFileSizeBytes * Math.pow(levelSizeMultiplier, level));
    }

    @Override
    public int levelFileCountLimit(int level) {
        return Integer.MAX_VALUE;
    }
}
