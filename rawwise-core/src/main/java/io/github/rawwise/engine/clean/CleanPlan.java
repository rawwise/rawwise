package io.github.rawwise.engine.clean;

import io.github.rawwise.common.PlanState;
import io.github.rawwise.engine.clean.strategy.CleanStrategy;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

public record CleanPlan(
        String planId,
        String tableId,
        long auditSnapshotId,
        Set<Long> retainedSnapshotIds,
        CleanStrategy.StrategyType strategyType,
        List<CleanAction> actions,
        long estimatedReclaimedBytes,
        long createdAtMillis,
        PlanState state)
        implements Serializable {

    public CleanPlan {
        if (planId == null || planId.isEmpty()) {
            throw new IllegalArgumentException("planId must not be null or empty");
        }
        if (tableId == null || tableId.isEmpty()) {
            throw new IllegalArgumentException("tableId must not be null or empty");
        }
        if (strategyType == null) {
            throw new IllegalArgumentException("strategyType must not be null");
        }
        if (estimatedReclaimedBytes < 0) {
            throw new IllegalArgumentException("estimatedReclaimedBytes must not be negative");
        }
        actions = actions == null ? List.of() : List.copyOf(actions);
        retainedSnapshotIds =
                retainedSnapshotIds == null ? Set.of() : Set.copyOf(retainedSnapshotIds);
        if (state == null) {
            throw new IllegalArgumentException("state must not be null");
        }
    }

    public CleanPlan withState(PlanState newState) {
        return new CleanPlan(
                planId,
                tableId,
                auditSnapshotId,
                retainedSnapshotIds,
                strategyType,
                actions,
                estimatedReclaimedBytes,
                createdAtMillis,
                newState);
    }

    public List<String> collectDeletableFilePaths() {
        return actions.stream()
                .filter(DeleteFileAction.class::isInstance)
                .map(DeleteFileAction.class::cast)
                .map(DeleteFileAction::filePath)
                .toList();
    }

    public List<String> collectRewriteFilePaths() {
        return actions.stream()
                .filter(RewriteFileAction.class::isInstance)
                .map(RewriteFileAction.class::cast)
                .map(RewriteFileAction::filePath)
                .toList();
    }

    public List<Long> collectExpiredSnapshotIds() {
        return actions.stream()
                .filter(ExpireSnapshotAction.class::isInstance)
                .map(ExpireSnapshotAction.class::cast)
                .map(ExpireSnapshotAction::snapshotId)
                .toList();
    }
}
