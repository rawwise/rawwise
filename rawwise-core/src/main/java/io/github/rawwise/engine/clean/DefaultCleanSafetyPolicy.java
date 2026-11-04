package io.github.rawwise.engine.clean;

import io.github.rawwise.engine.clean.strategy.CleanStrategy;
import io.github.rawwise.engine.storage.manifest.SnapshotOperations;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultCleanSafetyPolicy implements CleanSafetyPolicy {

    private final Metastore checkpointRegistry;
    private final AiNativeTableChecker aiNativeChecker;

    public DefaultCleanSafetyPolicy(Metastore checkpointRegistry) {
        this(checkpointRegistry, tableId -> false);
    }

    public DefaultCleanSafetyPolicy(
            Metastore checkpointRegistry, AiNativeTableChecker aiNativeChecker) {
        this.checkpointRegistry = checkpointRegistry;
        this.aiNativeChecker = aiNativeChecker;
    }

    @Override
    public SafetyCheck evaluate(CleanPlan plan, SnapshotOperations snapshotOps) {
        String tableId = plan.tableId();

        if (aiNativeChecker.isAiNative(tableId)) {
            boolean hasDestructive =
                    plan.actions().stream()
                            .anyMatch(
                                    a ->
                                            a instanceof DeleteFileAction
                                                    || a instanceof ExpireSnapshotAction);
            if (hasDestructive) {
                return SafetyCheck.block(
                        "Blocked: table "
                                + tableId
                                + " is AI-native — raw data must never be deleted");
            }
        }

        try {
            long pinnedId = getPinnedSnapshotId(tableId);
            if (pinnedId >= 0) {
                for (CleanAction action : plan.actions()) {
                    if (action instanceof ExpireSnapshotAction esa) {
                        if (esa.snapshotId() == pinnedId) {
                            return SafetyCheck.block(
                                    "Blocked: snapshot "
                                            + esa.snapshotId()
                                            + " is pinned by training checkpoint");
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Failed to check checkpoint pin for table={}, allowing clean", tableId, e);
        }

        if (plan.strategyType() == CleanStrategy.StrategyType.SNAPSHOT_EXPIRY) {
            try {
                Optional<Snapshot> latestOpt = snapshotOps.latestSnapshot(tableId);
                if (latestOpt.isEmpty()) return SafetyCheck.allow();
                long latestSnapshotId = latestOpt.get().snapshotId();
                if (latestSnapshotId != plan.auditSnapshotId()) {
                    return SafetyCheck.replan(
                            "REPLAN: base snapshot changed from "
                                    + plan.auditSnapshotId()
                                    + " to "
                                    + latestSnapshotId);
                }
            } catch (IOException e) {
                log.warn("Failed to get latest snapshot for safety check on table={}", tableId, e);
                return SafetyCheck.block("BLOCK: cannot verify safety for table " + tableId);
            }
        }

        return SafetyCheck.allow();
    }

    private long getPinnedSnapshotId(String tableId) throws IOException {
        if (checkpointRegistry == null) return -1;
        byte[] key = ("checkpoint:" + tableId).getBytes(StandardCharsets.UTF_8);
        Optional<byte[]> val = checkpointRegistry.get(key);
        if (val.isEmpty()) return -1;
        try {
            return Long.parseLong(new String(val.get(), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
