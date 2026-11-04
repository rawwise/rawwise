package io.github.rawwise.engine.clean;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.common.PlanState;
import io.github.rawwise.engine.clean.strategy.CleanStrategy;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.engine.storage.manifest.SnapshotCommitConflictException;
import io.github.rawwise.engine.storage.manifest.SnapshotOperations;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("AI-Native Clean Protection Tests")
class AiNativeCleanSafetyPolicyTest {

    private static final String AI_TABLE = "db.ai_table";
    private static final String NORMAL_TABLE = "db.normal_table";

    private static final AiNativeTableChecker aiChecker = tableId -> AI_TABLE.equals(tableId);
    private static final AiNativeTableChecker neverAiChecker = tableId -> false;

    private final CleanSafetyPolicy aiPolicy = new DefaultCleanSafetyPolicy(null, aiChecker);
    private final CleanSafetyPolicy normalPolicy =
            new DefaultCleanSafetyPolicy(null, neverAiChecker);

    private static CleanPlan planWithDeleteFile(String tableId) {
        return new CleanPlan(
                "plan-1",
                tableId,
                1L,
                Set.of(),
                CleanStrategy.StrategyType.TTL_EXPIRY,
                List.of(new DeleteFileAction("/data/file.parquet", 1024)),
                1024L,
                System.currentTimeMillis(),
                PlanState.PENDING);
    }

    private static CleanPlan planWithExpireSnapshot(String tableId) {
        return new CleanPlan(
                "plan-2",
                tableId,
                1L,
                Set.of(2L),
                CleanStrategy.StrategyType.SNAPSHOT_EXPIRY,
                List.of(new ExpireSnapshotAction(1L)),
                0L,
                System.currentTimeMillis(),
                PlanState.PENDING);
    }

    private static CleanPlan planWithRewriteOnly(String tableId) {
        return new CleanPlan(
                "plan-3",
                tableId,
                1L,
                Set.of(),
                CleanStrategy.StrategyType.TTL_EXPIRY,
                List.of(
                        new RewriteFileAction(
                                "/data/file.parquet", "expire_col", "timestamp", 0L, 0L, 1024L)),
                0L,
                System.currentTimeMillis(),
                PlanState.PENDING);
    }

    private final SnapshotOperations snapshotOps =
            new SnapshotOperations() {
                @Override
                public Optional<Snapshot> latestSnapshot(String tableId) throws IOException {
                    return Optional.empty();
                }

                @Override
                public List<DataFile> listDataFiles(String tableId, long snapshotId)
                        throws IOException {
                    return List.of();
                }

                @Override
                public List<Snapshot> getSnapshots(String tableId) throws IOException {
                    return List.of();
                }

                @Override
                public List<Snapshot> getRecentSnapshots(String tableId, int count)
                        throws IOException {
                    return List.of();
                }

                @Override
                public Snapshot commitSnapshot(
                        String tableId,
                        List<DataFile> newDataFiles,
                        List<LogFile> newLogFiles,
                        long minSeq,
                        long maxSeq)
                        throws SnapshotCommitConflictException, IOException {
                    return null;
                }
            };

    @Nested
    @DisplayName("AI-native table")
    class AiNativeProtection {

        @Test
        @DisplayName("AI-native table: DELETE_FILE -> BLOCK")
        void blockDeleteFileOnAiNativeTable() {
            CleanSafetyPolicy.SafetyCheck result =
                    aiPolicy.evaluate(planWithDeleteFile(AI_TABLE), snapshotOps);
            assertThat(result.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.BLOCK);
            assertThat(result.reason()).contains("AI-native");
        }

        @Test
        @DisplayName("AI-native table: EXPIRE_SNAPSHOT -> BLOCK")
        void blockExpireSnapshotOnAiNativeTable() {
            CleanSafetyPolicy.SafetyCheck result =
                    aiPolicy.evaluate(planWithExpireSnapshot(AI_TABLE), snapshotOps);
            assertThat(result.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.BLOCK);
            assertThat(result.reason()).contains("AI-native");
        }

        @Test
        @DisplayName("AI-native table: REWRITE_FILE -> ALLOW (does not delete original data)")
        void allowRewriteOnAiNativeTable() {
            CleanSafetyPolicy.SafetyCheck result =
                    aiPolicy.evaluate(planWithRewriteOnly(AI_TABLE), snapshotOps);
            assertThat(result.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.ALLOW);
        }
    }

    @Nested
    @DisplayName("table delete")
    class TraditionalTableAllows {

        @Test
        @DisplayName("table: DELETE_FILE -> ALLOW")
        void allowDeleteFileOnNormalTable() {
            CleanSafetyPolicy.SafetyCheck result =
                    normalPolicy.evaluate(planWithDeleteFile(NORMAL_TABLE), snapshotOps);
            assertThat(result.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.ALLOW);
        }

        @Test
        @DisplayName("table: EXPIRE_SNAPSHOT -> ALLOW")
        void allowExpireSnapshotOnNormalTable() {
            CleanSafetyPolicy.SafetyCheck result =
                    normalPolicy.evaluate(planWithExpireSnapshot(NORMAL_TABLE), snapshotOps);
            assertThat(result.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.ALLOW);
        }
    }

    @Nested
    @DisplayName("AiNativeTableChecker is null / default")
    class DefaultChecker {

        @Test
        @DisplayName("Default checker (never AI-native) -> DELETE_FILE ALLOW")
        void defaultCheckerAllowsDelete() {
            CleanSafetyPolicy policy = new DefaultCleanSafetyPolicy(null);
            CleanSafetyPolicy.SafetyCheck result =
                    policy.evaluate(planWithDeleteFile(NORMAL_TABLE), snapshotOps);
            assertThat(result.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.ALLOW);
        }
    }
}
