package io.github.rawwise.engine.compaction.recovery;

import io.github.rawwise.engine.compaction.cleanup.FileCleanupManager;
import io.github.rawwise.engine.compaction.cleanup.StoragePathResolver;
import io.github.rawwise.engine.compaction.planner.CompactionPlan;
import io.github.rawwise.engine.storage.spi.Storage;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactionRecoveryManager {

    private final FileCleanupManager cleanupManager;
    private final Storage storage;

    public CompactionRecoveryManager(FileCleanupManager cleanupManager, Storage storage) {
        this.cleanupManager = cleanupManager;
        this.storage = storage;
    }

    public RecoveryResult recoverOrphanedFiles(CompactionPlan plan) {
        List<String> expectedNewFiles = plan.collectExpectedNewFilePaths();
        List<String> existing =
                expectedNewFiles.stream()
                        .filter(
                                p -> {
                                    try {
                                        return storage.exists(StoragePathResolver.resolvePath(p));
                                    } catch (IOException e) {
                                        return false;
                                    }
                                })
                        .toList();
        List<String> valid =
                existing.stream()
                        .filter(
                                p -> {
                                    try {
                                        Storage.StoragePath sp = StoragePathResolver.resolvePath(p);
                                        return storage.exists(sp)
                                                && storage.status(sp).length() > 0;
                                    } catch (IOException e) {
                                        return false;
                                    }
                                })
                        .toList();

        if (valid.size() == expectedNewFiles.size()) {
            log.info(
                    "All {} new files valid for plan {}, re-committing",
                    valid.size(),
                    plan.planId());
            return new RecoveryResult(RecoveryState.COMMIT, valid, plan.collectSourceFilePaths());
        } else {
            log.warn(
                    "Plan {} has {} valid files out of {} expected, cleaning up",
                    plan.planId(),
                    valid.size(),
                    expectedNewFiles.size());
            cleanupManager.immediateCleanup(existing);
            return new RecoveryResult(RecoveryState.ABORT, List.of(), List.of());
        }
    }

    public record RecoveryResult(
            RecoveryState state, List<String> validNewFiles, List<String> oldFiles) {}

    public enum RecoveryState {
        COMMIT,
        ABORT
    }
}
