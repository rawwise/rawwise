package io.github.rawwise.engine.compaction.strategy;

import io.github.rawwise.engine.storage.DataFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class UniversalStrategy implements CompactionStrategy, Serializable {

    private final double sizeRatio;
    private final int minMergeWidth;
    private final int maxLevel;
    private final long baseFileSizeBytes;

    public UniversalStrategy(
            double sizeRatio, int minMergeWidth, int maxLevel, long baseFileSizeBytes) {
        this.sizeRatio = sizeRatio;
        this.minMergeWidth = minMergeWidth;
        this.maxLevel = maxLevel;
        this.baseFileSizeBytes = baseFileSizeBytes;
    }

    public UniversalStrategy() {
        this(5.0, 2, 3, 256L * 1024 * 1024);
    }

    @Override
    public CompactionStrategy.StrategyType strategyType() {
        return StrategyType.UNIVERSAL;
    }

    @Override
    public List<SourceGroup> plan(
            List<DataFile> l0Files, List<DataFile> l1Files, List<DataFile> l2Files) {
        List<SourceGroup> groups = new ArrayList<>();
        List<DataFile> allFiles = new ArrayList<>();
        allFiles.addAll(l0Files);
        allFiles.addAll(l1Files);
        if (l2Files != null) {
            allFiles.addAll(l2Files);
        }

        allFiles.sort(Comparator.comparingLong(DataFile::fileSizeBytes));

        int i = 0;
        while (i < allFiles.size()) {
            long firstSize = allFiles.get(i).fileSizeBytes();
            long accumulatedSize = firstSize;
            int j = i + 1;

            while (j < allFiles.size()
                    && accumulatedSize + allFiles.get(j).fileSizeBytes() <= sizeRatio * firstSize) {
                accumulatedSize += allFiles.get(j).fileSizeBytes();
                j++;
            }

            int count = j - i;
            if (count >= minMergeWidth) {
                List<String> paths = new ArrayList<>();
                long totalBytes = 0;
                long totalRows = 0;
                int maxFileLevel = 0;
                for (int k = i; k < j; k++) {
                    DataFile f = allFiles.get(k);
                    paths.add(f.filePath());
                    totalBytes += f.fileSizeBytes();
                    totalRows += f.rowCount();
                    maxFileLevel = Math.max(maxFileLevel, f.compactionLevel());
                }
                int targetLevel = Math.min(maxFileLevel + 1, maxLevel);
                groups.add(new SourceGroup(targetLevel, paths, totalBytes, totalRows));
                i = j;
            } else {
                i++;
            }
        }

        return groups;
    }

    @Override
    public int maxLevel() {
        return maxLevel;
    }

    @Override
    public long maxLevelBytes(int level) {
        return (long) (baseFileSizeBytes * Math.pow(sizeRatio, level));
    }

    @Override
    public int levelFileCountLimit(int level) {
        return Integer.MAX_VALUE;
    }
}
