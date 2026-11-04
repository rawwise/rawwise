package io.github.rawwise.engine.compaction.strategy;

import io.github.rawwise.engine.storage.DataFile;
import java.io.Serializable;
import java.util.List;

public interface CompactionStrategy extends Serializable {

    enum StrategyType {
        TIERED,
        LEVELED,
        UNIVERSAL
    }

    StrategyType strategyType();

    List<SourceGroup> plan(List<DataFile> l0Files, List<DataFile> l1Files, List<DataFile> l2Files);

    int maxLevel();

    long maxLevelBytes(int level);

    int levelFileCountLimit(int level);

    record SourceGroup(
            int targetLevel,
            List<String> sourceFilePaths,
            long estimatedOutputBytes,
            long estimatedOutputRows)
            implements Serializable {
        public SourceGroup {
            sourceFilePaths = sourceFilePaths == null ? List.of() : List.copyOf(sourceFilePaths);
        }
    }
}
