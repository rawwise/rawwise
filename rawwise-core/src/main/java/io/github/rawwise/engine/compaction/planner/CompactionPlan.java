package io.github.rawwise.engine.compaction.planner;

import io.github.rawwise.common.PlanState;
import io.github.rawwise.engine.compaction.strategy.CompactionStrategy;
import java.io.Serializable;
import java.util.List;

public record CompactionPlan(
        String planId,
        String tableId,
        long baseSnapshotId,
        CompactionStrategy.StrategyType strategyType,
        List<SourceGroup> sourceGroups,
        long estimatedInputBytes,
        long estimatedOutputBytes,
        long createdAtMillis,
        PlanState state)
        implements Serializable {

    public CompactionPlan {
        sourceGroups = sourceGroups == null ? List.of() : List.copyOf(sourceGroups);
    }

    public CompactionPlan withState(PlanState newState) {
        return new CompactionPlan(
                planId,
                tableId,
                baseSnapshotId,
                strategyType,
                sourceGroups,
                estimatedInputBytes,
                estimatedOutputBytes,
                createdAtMillis,
                newState);
    }

    public List<String> collectSourceFilePaths() {
        return sourceGroups.stream().flatMap(g -> g.sourceFilePaths().stream()).toList();
    }

    public List<String> collectExpectedNewFilePaths() {
        return sourceGroups.stream()
                .flatMap(
                        g ->
                                g.sourceFilePaths().stream()
                                        .map(
                                                p ->
                                                        p
                                                                + ".compacted.L"
                                                                + g.targetLevel()
                                                                + "_"
                                                                + planId.substring(0, 8)))
                .toList();
    }

    public record SourceGroup(
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
