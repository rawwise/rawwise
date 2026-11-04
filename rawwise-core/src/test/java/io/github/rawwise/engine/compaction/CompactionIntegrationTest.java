package io.github.rawwise.engine.compaction;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.common.PlanState;
import io.github.rawwise.engine.compaction.merger.LatestWinMergeFunction;
import io.github.rawwise.engine.compaction.merger.MergeFunction;
import io.github.rawwise.engine.compaction.merger.VersionedValue;
import io.github.rawwise.engine.compaction.planner.CompactionPlan;
import io.github.rawwise.engine.compaction.strategy.CompactionStrategy;
import io.github.rawwise.engine.compaction.strategy.TieredStrategy;
import io.github.rawwise.engine.storage.ColumnStatistics;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.ParquetDataFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Compactionintegration test")
class CompactionIntegrationTest {

    @Nested
    @DisplayName("TieredStrategy logic")
    class TieredStrategyPlanning {

        @Test
        @DisplayName("L0 not yet threshold does not triggercompaction")
        void testNoCompactionBelowThreshold() {
            TieredStrategy strategy = new TieredStrategy(5, 256L * 1024 * 1024, 3, 10.0);

            List<DataFile> l0Files = createL0Files(3);
            List<CompactionStrategy.SourceGroup> groups =
                    strategy.plan(l0Files, List.of(), List.of());

            assertThat(groups).isEmpty();
        }

        @Test
        @DisplayName("L0 tothreshold triggercompactiontoL1")
        void testCompactionTriggeredAtThreshold() {
            TieredStrategy strategy = new TieredStrategy(5, 256L * 1024 * 1024, 3, 10.0);

            List<DataFile> l0Files = createL0Files(5);
            List<CompactionStrategy.SourceGroup> groups =
                    strategy.plan(l0Files, List.of(), List.of());

            assertThat(groups).hasSize(1);
            assertThat(groups.getFirst().targetLevel()).isEqualTo(1);
            assertThat(groups.getFirst().sourceFilePaths()).hasSize(5);
        }

        @Test
        @DisplayName("L0 exceedsthreshold allL0 andcompaction")
        void testAllL0FilesCompactedWhenAboveThreshold() {
            TieredStrategy strategy = new TieredStrategy(3, 256L * 1024 * 1024, 3, 10.0);

            List<DataFile> l0Files = createL0Files(10);
            List<CompactionStrategy.SourceGroup> groups =
                    strategy.plan(l0Files, List.of(), List.of());

            assertThat(groups).hasSize(1);
            assertThat(groups.getFirst().targetLevel()).isEqualTo(1);
            assertThat(groups.getFirst().sourceFilePaths()).hasSize(10);
        }

        @Test
        @DisplayName("L1exceeds limit trigger compactiontoL2")
        void testCascadeCompactionWhenL1ExceedsLimit() {
            long maxFileSize = 1024;
            TieredStrategy strategy = new TieredStrategy(3, maxFileSize, 3, 2.0);

            List<DataFile> l0Files = createL0Files(3);
            List<DataFile> l1Files = createLeveledFiles(1, 5 * maxFileSize, 1);
            List<CompactionStrategy.SourceGroup> groups =
                    strategy.plan(l0Files, l1Files, List.of());

            assertThat(groups).hasSizeGreaterThanOrEqualTo(2);
            assertThat(groups.getFirst().targetLevel()).isEqualTo(1);
            boolean hasL2Target = groups.stream().anyMatch(g -> g.targetLevel() == 2);
            assertThat(hasL2Target).isTrue();
        }
    }

    @Nested
    @DisplayName("CompactionPlanstatus")
    class CompactionPlanState {

        @Test
        @DisplayName("PlancreateafterstatusisPENDING")
        void testPlanInitialState() {
            CompactionPlan plan = createTestPlan();

            assertThat(plan.state()).isEqualTo(PlanState.PENDING);
        }

        @Test
        @DisplayName("withStatecorrect status")
        void testPlanStateTransition() {
            CompactionPlan plan = createTestPlan();

            CompactionPlan running = plan.withState(PlanState.RUNNING);
            assertThat(running.state()).isEqualTo(PlanState.RUNNING);
            assertThat(running.planId()).isEqualTo(plan.planId());

            CompactionPlan completed = running.withState(PlanState.COMPLETED);
            assertThat(completed.state()).isEqualTo(PlanState.COMPLETED);
        }

        @Test
        @DisplayName("collectSourceFilePaths all path")
        void testCollectSourceFilePaths() {
            CompactionPlan plan =
                    new CompactionPlan(
                            "plan-1",
                            "table-1",
                            0L,
                            CompactionStrategy.StrategyType.TIERED,
                            List.of(
                                    new CompactionPlan.SourceGroup(
                                            1,
                                            List.of(
                                                    "/data/l0/file1.parquet",
                                                    "/data/l0/file2.parquet"),
                                            1024,
                                            100),
                                    new CompactionPlan.SourceGroup(
                                            2, List.of("/data/l1/file3.parquet"), 2048, 200)),
                            3072,
                            2048,
                            System.currentTimeMillis(),
                            PlanState.PENDING);

            List<String> paths = plan.collectSourceFilePaths();
            assertThat(paths)
                    .containsExactly(
                            "/data/l0/file1.parquet",
                            "/data/l0/file2.parquet",
                            "/data/l1/file3.parquet");
        }

        @Test
        @DisplayName("FAILEDstatus plan")
        void testFailedStatePreservesPlanInfo() {
            CompactionPlan plan = createTestPlan();

            CompactionPlan failed = plan.withState(PlanState.RUNNING).withState(PlanState.FAILED);
            assertThat(failed.state()).isEqualTo(PlanState.FAILED);
            assertThat(failed.planId()).isEqualTo(plan.planId());
            assertThat(failed.sourceGroups()).isEqualTo(plan.sourceGroups());
        }
    }

    @Nested
    @DisplayName("LatestWinMergeFunctionmergestrategy")
    class LatestWinMergeTests {

        private final MergeFunction mergeFn = new LatestWinMergeFunction();

        @Test
        @DisplayName("multiitemrecord sequenceNumberofnon-tombstonevalue")
        void testLatestNonTombstoneWins() {
            byte[] key = "key-1".getBytes(StandardCharsets.UTF_8);
            List<VersionedValue> versions =
                    List.of(
                            new VersionedValue(
                                    "value-v1".getBytes(StandardCharsets.UTF_8), 1, 100, false),
                            new VersionedValue(
                                    "value-v2".getBytes(StandardCharsets.UTF_8), 2, 200, false),
                            new VersionedValue(
                                    "value-v3".getBytes(StandardCharsets.UTF_8), 3, 300, false));

            byte[] result = mergeFn.merge(key, versions);
            assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("value-v3");
        }

        @Test
        @DisplayName("recordistombstonereturns when ofnon-tombstonevalue")
        void testTombstoneLatestReturnsSecondNewest() {
            byte[] key = "key-2".getBytes(StandardCharsets.UTF_8);
            List<VersionedValue> versions =
                    List.of(
                            new VersionedValue(
                                    "value-v1".getBytes(StandardCharsets.UTF_8), 1, 100, false),
                            new VersionedValue(
                                    "value-v2".getBytes(StandardCharsets.UTF_8), 2, 200, false),
                            new VersionedValue(new byte[0], 3, 300, true));

            byte[] result = mergeFn.merge(key, versions);
            assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("value-v2");
        }

        @Test
        @DisplayName("middlehastombstone is valuereturns when value")
        void testTombstoneInMiddleLatestNormal() {
            byte[] key = "key-3".getBytes(StandardCharsets.UTF_8);
            List<VersionedValue> versions =
                    List.of(
                            new VersionedValue(
                                    "value-v1".getBytes(StandardCharsets.UTF_8), 1, 100, false),
                            new VersionedValue(new byte[0], 2, 200, true),
                            new VersionedValue(
                                    "value-v3".getBytes(StandardCharsets.UTF_8), 3, 300, false));

            byte[] result = mergeFn.merge(key, versions);
            assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("value-v3");
        }

        @Test
        @DisplayName("istombstonereturns whennull")
        void testAllTombstonesReturnsNull() {
            byte[] key = "key-4".getBytes(StandardCharsets.UTF_8);
            List<VersionedValue> versions =
                    List.of(
                            new VersionedValue(new byte[0], 1, 100, true),
                            new VersionedValue(new byte[0], 2, 200, true),
                            new VersionedValue(new byte[0], 3, 300, true));

            byte[] result = mergeFn.merge(key, versions);
            assertThat(result).isNull();
        }

        @Test
        @DisplayName("singleitemnon-tombstonerecorddirectly")
        void testSingleNonTombstone() {
            byte[] key = "key-5".getBytes(StandardCharsets.UTF_8);
            List<VersionedValue> versions =
                    List.of(
                            new VersionedValue(
                                    "only-value".getBytes(StandardCharsets.UTF_8), 1, 100, false));

            byte[] result = mergeFn.merge(key, versions);
            assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("only-value");
        }
    }

    @Nested
    @DisplayName("KeyRangemerge")
    class KeyRangeMergeTests {

        @Test
        @DisplayName("two KeyRangemergeaftercover")
        void testAdjacentKeyRangeMerge() {
            KeyRange range1 =
                    new KeyRange(
                            "a".getBytes(StandardCharsets.UTF_8), true,
                            "m".getBytes(StandardCharsets.UTF_8), true);
            KeyRange range2 =
                    new KeyRange(
                            "n".getBytes(StandardCharsets.UTF_8), true,
                            "z".getBytes(StandardCharsets.UTF_8), true);

            KeyRange merged = mergeKeyRange(range1, range2);

            assertThat(merged.startKey()).isEqualTo("a".getBytes(StandardCharsets.UTF_8));
            assertThat(merged.endKey()).isEqualTo("z".getBytes(StandardCharsets.UTF_8));
            assertThat(merged.startInclusive()).isTrue();
            assertThat(merged.endInclusive()).isTrue();
        }

        @Test
        @DisplayName("FULL_RANGEand KeyRangemerge isFULL_RANGE")
        void testFullRangeMergeWithAny() {
            KeyRange range =
                    new KeyRange(
                            "a".getBytes(StandardCharsets.UTF_8), true,
                            "z".getBytes(StandardCharsets.UTF_8), true);

            KeyRange merged = mergeKeyRange(KeyRange.FULL_RANGE, range);
            assertThat(merged.isFullRange()).isTrue();

            KeyRange merged2 = mergeKeyRange(range, KeyRange.FULL_RANGE);
            assertThat(merged2.isFullRange()).isTrue();
        }

        @Test
        @DisplayName("null KeyRangemerge another item")
        void testNullKeyRangeMerge() {
            KeyRange range =
                    new KeyRange(
                            "a".getBytes(StandardCharsets.UTF_8), true,
                            "z".getBytes(StandardCharsets.UTF_8), true);

            assertThat(mergeKeyRange(null, range)).isEqualTo(range);
            assertThat(mergeKeyRange(range, null)).isEqualTo(range);
        }
    }

    private List<DataFile> createL0Files(int count) {
        List<DataFile> files = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            files.add(
                    new ParquetDataFile(
                            "/data/l0/file-" + i + ".parquet",
                            1024 * 1024,
                            System.currentTimeMillis(),
                            "test-table",
                            0L,
                            new KeyRange(
                                    ("key-" + i).getBytes(StandardCharsets.UTF_8), true,
                                    ("key-" + (i + 1)).getBytes(StandardCharsets.UTF_8), true),
                            100,
                            0L,
                            i * 100L,
                            i * 100L + 99,
                            Map.of(),
                            ColumnStatistics.EMPTY,
                            0));
        }
        return files;
    }

    private List<DataFile> createLeveledFiles(int level, long fileSizeBytes, int count) {
        List<DataFile> files = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            files.add(
                    new ParquetDataFile(
                            "/data/l" + level + "/file-" + i + ".parquet",
                            fileSizeBytes,
                            System.currentTimeMillis(),
                            "test-table",
                            0L,
                            new KeyRange(
                                    ("key-" + i).getBytes(StandardCharsets.UTF_8), true,
                                    ("key-" + (i + 1)).getBytes(StandardCharsets.UTF_8), true),
                            1000,
                            0L,
                            i * 1000L,
                            i * 1000L + 999,
                            Map.of(),
                            ColumnStatistics.EMPTY,
                            level));
        }
        return files;
    }

    private CompactionPlan createTestPlan() {
        return new CompactionPlan(
                "plan-test-" + System.nanoTime(),
                "test-table",
                0L,
                CompactionStrategy.StrategyType.TIERED,
                List.of(
                        new CompactionPlan.SourceGroup(
                                1,
                                List.of("/data/l0/file1.parquet", "/data/l0/file2.parquet"),
                                2048,
                                200)),
                2048,
                1024,
                System.currentTimeMillis(),
                PlanState.PENDING);
    }

    private KeyRange mergeKeyRange(KeyRange a, KeyRange b) {
        if (a == null) return b;
        if (b == null) return a;
        if (a.isFullRange() || b.isFullRange()) return KeyRange.FULL_RANGE;

        byte[] startKey;
        boolean startInclusive;
        int cmp = compareBytes(a.startKey(), b.startKey());
        if (cmp <= 0) {
            startKey = a.startKey();
            startInclusive =
                    cmp < 0 ? a.startInclusive() : a.startInclusive() && b.startInclusive();
        } else {
            startKey = b.startKey();
            startInclusive = b.startInclusive();
        }

        byte[] endKey;
        boolean endInclusive;
        int cmpEnd = compareBytes(a.endKey(), b.endKey());
        if (cmpEnd >= 0) {
            endKey = a.endKey();
            endInclusive = cmpEnd > 0 ? a.endInclusive() : a.endInclusive() && b.endInclusive();
        } else {
            endKey = b.endKey();
            endInclusive = b.endInclusive();
        }

        return new KeyRange(startKey, startInclusive, endKey, endInclusive);
    }

    private int compareBytes(byte[] a, byte[] b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }
}
