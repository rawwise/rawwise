package io.github.rawwise.engine.clean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import io.github.rawwise.common.PlanState;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.engine.clean.strategy.CleanStrategy;
import io.github.rawwise.engine.compaction.cleanup.GcEntry;
import io.github.rawwise.engine.compaction.cleanup.StoragePathValidator;
import io.github.rawwise.engine.storage.manifest.SnapshotOperations;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@DisplayName("CleanPipeline E2E")
class CleanPipelineTest {

    @Nested
    @DisplayName("StoragePathValidator Security")
    class StoragePathValidatorSecurity {

        @Test
        @DisplayName("SPV-01: Path traversal ../other-table rejected")
        void spv01PathTraversal() {
            Storage.StoragePath sp =
                    new Storage.StoragePath("file", "../other-table/data/file.parquet");
            assertThatThrownBy(() -> StoragePathValidator.validate(sp, "db.table"))
                    .isInstanceOf(SecurityException.class);
        }

        @Test
        @DisplayName("SPV-02: Wrong tableId rejected")
        void spv02WrongTableId() {
            Storage.StoragePath sp = new Storage.StoragePath("s3", "other-table/data/file.parquet");
            assertThatThrownBy(() -> StoragePathValidator.validate(sp, "db.table"))
                    .isInstanceOf(SecurityException.class);
        }

        @Test
        @DisplayName("SPV-03: Forbidden scheme rejected")
        void spv03ForbiddenScheme() {
            Storage.StoragePath sp = new Storage.StoragePath("ftp", "db.table/data/file.parquet");
            assertThatThrownBy(() -> StoragePathValidator.validate(sp, "db.table"))
                    .isInstanceOf(SecurityException.class);
        }

        @Test
        @DisplayName("SPV-05: Normalized path matching original is allowed")
        void spv05NormalizedAllowed() {
            Storage.StoragePath sp = new Storage.StoragePath("s3", "db.table/data/file.parquet");
            assertThatCode(() -> StoragePathValidator.validate(sp, "db.table"))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("SPV-06: Metadata directory deletion rejected")
        void spv06MetadataDirRejected() {
            Storage.StoragePath sp = new Storage.StoragePath("s3", "db.table/metadata/v1.json");
            assertThatThrownBy(() -> StoragePathValidator.validate(sp, "db.table"))
                    .isInstanceOf(SecurityException.class);
        }

        @Test
        @DisplayName("SPV-09: Valid path with matching tableId allowed")
        void spv09ValidPathAllowed() {
            Storage.StoragePath sp = new Storage.StoragePath("s3", "db.table/data/file.parquet");
            assertThatCode(() -> StoragePathValidator.validate(sp, "db.table"))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("SPV-10: Prefix match prevents substring collision")
        void spv10PrefixNotSubstring() {
            Storage.StoragePath sp = new Storage.StoragePath("s3", "db.table_v2/data/file.parquet");
            assertThatThrownBy(() -> StoragePathValidator.validate(sp, "db.table"))
                    .isInstanceOf(SecurityException.class);
        }

        @Test
        @DisplayName("SPV-11: Null StoragePath rejected")
        void spv11NullStoragePath() {
            assertThatThrownBy(() -> StoragePathValidator.validate(null, "db.table"))
                    .isInstanceOf(SecurityException.class);
        }

        @Test
        @DisplayName("SPV-12: Empty tableId rejected")
        void spv12EmptyTableId() {
            Storage.StoragePath sp = new Storage.StoragePath("s3", "db.table/data/file.parquet");
            assertThatThrownBy(() -> StoragePathValidator.validate(sp, ""))
                    .isInstanceOf(SecurityException.class);
        }
    }

    @Nested
    @DisplayName("GcEntry Serialization")
    class GcEntrySerialization {

        @Test
        @DisplayName("GCE-01: Round-trip serialization preserves all fields")
        void gce01RoundTrip() {
            GcEntry original = GcEntry.pending("data/file.parquet", "db.table", 42L, 1000L);
            byte[] bytes = original.toBytes();
            GcEntry restored = GcEntry.fromBytes(bytes);
            assertThat(restored).isEqualTo(original);
        }

        @Test
        @DisplayName("GCE-02: withIncrementedRetry increments retry count")
        void gce02IncrementedRetry() {
            GcEntry entry = GcEntry.pending("data/file.parquet", "db.table", 42L, 1000L);
            assertThat(entry.retryCount()).isEqualTo(0);
            GcEntry updated = entry.withIncrementedRetry();
            assertThat(updated.retryCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("GCE-03: asDeadLetter sets state and reason")
        void gce03DeadLetter() {
            GcEntry entry = GcEntry.pending("data/file.parquet", "db.table", 42L, 1000L);
            GcEntry deadLetter = entry.asDeadLetter("test reason");
            assertThat(deadLetter.state()).isEqualTo(GcEntry.STATE_DEAD_LETTER);
            assertThat(deadLetter.reason()).isEqualTo("test reason");
        }

        @Test
        @DisplayName("GCE-04: Null reason serialization round-trip")
        void gce04NullReason() {
            GcEntry original = GcEntry.pending("data/file.parquet", "db.table", 42L, 1000L);
            assertThat(original.reason()).isNull();
            byte[] bytes = original.toBytes();
            GcEntry restored = GcEntry.fromBytes(bytes);
            assertThat(restored.reason()).isNull();
        }
    }

    @Nested
    @DisplayName("CleanSafetyPolicy")
    class CleanSafetyPolicyTests {

        @Mock private SnapshotOperations snapshotOps;

        @BeforeEach
        void setUp() {
            MockitoAnnotations.openMocks(this);
        }

        private CleanPlan createSnapshotExpiryPlan(
                String tableId, long auditSnapshotId, List<Long> snapshotIdsToExpire) {
            List<CleanAction> actions =
                    snapshotIdsToExpire.stream()
                            .map(ExpireSnapshotAction::new)
                            .map(CleanAction.class::cast)
                            .toList();
            return new CleanPlan(
                    "plan-1",
                    tableId,
                    auditSnapshotId,
                    Set.of(),
                    CleanStrategy.StrategyType.SNAPSHOT_EXPIRY,
                    actions,
                    0L,
                    System.currentTimeMillis(),
                    PlanState.PENDING);
        }

        private Metastore pinnedMetastore(long pinnedSnapshotId) {
            return new Metastore() {
                @Override
                public Optional<byte[]> get(byte[] key) {
                    String keyStr = new String(key, StandardCharsets.UTF_8);
                    if (keyStr.equals("checkpoint:db.table")) {
                        return Optional.of(
                                String.valueOf(pinnedSnapshotId).getBytes(StandardCharsets.UTF_8));
                    }
                    return Optional.empty();
                }

                @Override
                public void put(byte[] key, byte[] value) {}

                @Override
                public void delete(byte[] key) {}

                @Override
                public boolean exists(byte[] key) {
                    return false;
                }

                @Override
                public CloseableIterator<KeyValue> scan(byte[] prefix) {
                    return new EmptyIterator();
                }

                @Override
                public CloseableIterator<KeyValue> scan(byte[] prefix, int limit) {
                    return new EmptyIterator();
                }

                @Override
                public CloseableIterator<KeyValue> scanRange(byte[] start, byte[] end) {
                    return new EmptyIterator();
                }

                @Override
                public CloseableIterator<KeyValue> scanRange(byte[] start, byte[] end, int limit) {
                    return new EmptyIterator();
                }

                @Override
                public boolean compareAndSwap(List<Precondition> p, List<Mutation> m) {
                    return false;
                }

                @Override
                public void batchWrite(List<Mutation> m) {}

                @Override
                public void close() {}
            };
        }

        @Test
        @DisplayName("CSP-01: Checkpoint-pin blocks snapshot expiry")
        void csp01CheckpointPin() throws IOException {
            Metastore checkpointRegistry = pinnedMetastore(42L);
            DefaultCleanSafetyPolicy policy = new DefaultCleanSafetyPolicy(checkpointRegistry);
            CleanPlan plan = createSnapshotExpiryPlan("db.table", 50L, List.of(42L));

            when(snapshotOps.latestSnapshot("db.table"))
                    .thenReturn(Optional.of(Snapshot.of(50L, "append")));

            CleanSafetyPolicy.SafetyCheck check = policy.evaluate(plan, snapshotOps);
            assertThat(check.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.BLOCK);
        }

        @Test
        @DisplayName("CSP-02: Freshness check triggers REPLAN when base snapshot changed")
        void csp02FreshnessCheck() throws IOException {
            DefaultCleanSafetyPolicy policy = new DefaultCleanSafetyPolicy(null);
            CleanPlan plan = createSnapshotExpiryPlan("db.table", 1L, List.of(1L));

            when(snapshotOps.latestSnapshot("db.table"))
                    .thenReturn(Optional.of(Snapshot.of(2L, "append")));

            CleanSafetyPolicy.SafetyCheck check = policy.evaluate(plan, snapshotOps);
            assertThat(check.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.REPLAN_REQUIRED);
        }

        @Test
        @DisplayName("CSP-03: ALLOW when base snapshot matches latest")
        void csp03AllowWhenFresh() throws IOException {
            DefaultCleanSafetyPolicy policy = new DefaultCleanSafetyPolicy(null);
            CleanPlan plan = createSnapshotExpiryPlan("db.table", 1L, List.of(1L));

            when(snapshotOps.latestSnapshot("db.table"))
                    .thenReturn(Optional.of(Snapshot.of(1L, "append")));

            CleanSafetyPolicy.SafetyCheck check = policy.evaluate(plan, snapshotOps);
            assertThat(check.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.ALLOW);
        }

        @Test
        @DisplayName("CSP-04: Non-SNAPSHOT_EXPIRY strategy always ALLOW without checkpoint")
        void csp04NonSnapshotExpiryAlwaysAllow() throws IOException {
            DefaultCleanSafetyPolicy policy = new DefaultCleanSafetyPolicy(null);
            CleanPlan plan =
                    new CleanPlan(
                            "plan-1",
                            "db.table",
                            1L,
                            Set.of(),
                            CleanStrategy.StrategyType.ORPHAN_FILE,
                            List.of(),
                            0L,
                            System.currentTimeMillis(),
                            PlanState.PENDING);

            when(snapshotOps.latestSnapshot("db.table"))
                    .thenReturn(Optional.of(Snapshot.of(99L, "append")));

            CleanSafetyPolicy.SafetyCheck check = policy.evaluate(plan, snapshotOps);
            assertThat(check.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.ALLOW);
        }

        @Test
        @DisplayName("CSP-05: Checkpoint-pin does not block non-matching snapshot")
        void csp05CheckpointPinNonMatching() throws IOException {
            Metastore checkpointRegistry = pinnedMetastore(99L);
            DefaultCleanSafetyPolicy policy = new DefaultCleanSafetyPolicy(checkpointRegistry);
            CleanPlan plan = createSnapshotExpiryPlan("db.table", 50L, List.of(10L));

            when(snapshotOps.latestSnapshot("db.table"))
                    .thenReturn(Optional.of(Snapshot.of(50L, "append")));

            CleanSafetyPolicy.SafetyCheck check = policy.evaluate(plan, snapshotOps);
            assertThat(check.verdict()).isEqualTo(CleanSafetyPolicy.Verdict.ALLOW);
        }
    }

    @Nested
    @DisplayName("CleanMetrics")
    class CleanMetricsTests {

        @Test
        @DisplayName("MET-01: New metrics fields track correctly")
        void met01NewMetrics() {
            CleanMetrics metrics = new CleanMetrics();
            metrics.addBytesScheduledForDeletion(1000L);
            metrics.addBytesPhysicallyDeleted(500L);
            metrics.addBytesInDeadLetter(200L);
            metrics.recordCleanCommitConflict();
            metrics.recordTTLColumnNotFound();

            assertThat(metrics.getBytesScheduledForDeletion()).isEqualTo(1000L);
            assertThat(metrics.getBytesPhysicallyDeleted()).isEqualTo(500L);
            assertThat(metrics.getBytesInDeadLetter()).isEqualTo(200L);
            assertThat(metrics.getCleanCommitConflicts()).isEqualTo(1);
            assertThat(metrics.getTtlColumnNotFoundCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("MET-02: Metrics accumulate across multiple calls")
        void met02Accumulation() {
            CleanMetrics metrics = new CleanMetrics();
            metrics.addBytesScheduledForDeletion(100L);
            metrics.addBytesScheduledForDeletion(200L);
            metrics.recordCleanCommitConflict();
            metrics.recordCleanCommitConflict();

            assertThat(metrics.getBytesScheduledForDeletion()).isEqualTo(300L);
            assertThat(metrics.getCleanCommitConflicts()).isEqualTo(2);
        }

        @Test
        @DisplayName("MET-03: Snapshot string includes all fields")
        void met03SnapshotString() {
            CleanMetrics metrics = new CleanMetrics();
            metrics.addBytesScheduledForDeletion(100L);
            metrics.addBytesPhysicallyDeleted(50L);
            metrics.addBytesInDeadLetter(10L);
            metrics.recordCleanCommitConflict();
            metrics.recordTTLColumnNotFound();

            String snap = metrics.snapshot();
            assertThat(snap).contains("bytesScheduledForDeletion=100");
            assertThat(snap).contains("bytesPhysicallyDeleted=50");
            assertThat(snap).contains("bytesInDeadLetter=10");
            assertThat(snap).contains("cleanCommitConflicts=1");
            assertThat(snap).contains("ttlColumnNotFound=1");
        }
    }

    private static class EmptyIterator implements CloseableIterator<KeyValue> {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public KeyValue next() {
            throw new java.util.NoSuchElementException();
        }

        @Override
        public void close() {}
    }
}
