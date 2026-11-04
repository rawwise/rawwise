package io.github.rawwise.engine.clean.strategy;

import io.github.rawwise.engine.clean.CleanAction;
import io.github.rawwise.engine.clean.CleanContext;
import io.github.rawwise.engine.clean.DeleteFileAction;
import io.github.rawwise.engine.clean.StorageFileEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class OrphanFileStrategy implements CleanStrategy {

    private static final long serialVersionUID = 1L;

    private final long minOrphanAgeMs;

    public OrphanFileStrategy(long minOrphanAgeMs) {
        if (minOrphanAgeMs < 0) {
            throw new IllegalArgumentException("minOrphanAgeMs must not be negative");
        }
        this.minOrphanAgeMs = minOrphanAgeMs;
    }

    @Override
    public StrategyType strategyType() {
        return StrategyType.ORPHAN_FILE;
    }

    @Override
    public List<CleanAction> plan(CleanContext context) {
        Set<String> referencedPaths = context.allReferencedPaths();
        long cutoffTime = context.currentTimestampMs() - minOrphanAgeMs;
        List<CleanAction> actions = new ArrayList<>();
        for (StorageFileEntry entry : context.storageFileEntries()) {
            if (!referencedPaths.contains(entry.path())) {
                if (entry.lastModifiedMillis() < cutoffTime) {
                    actions.add(new DeleteFileAction(entry.path(), entry.sizeBytes()));
                }
            }
        }
        return actions;
    }

    public long minOrphanAgeMs() {
        return minOrphanAgeMs;
    }
}
