package io.github.rawwise.engine.txn.orchestrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.RecordHeaderProbe;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.TxnMode;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.clock.LogicalClock;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.engine.txn.state.TxnState;
import io.github.rawwise.engine.txn.store.VersionChainStore;
import io.github.rawwise.engine.txn.wal.InMemoryTxnWALWriter;
import io.github.rawwise.engine.txn.wal.ScanBackedTxnWALReader;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@DisplayName("Txn Pipeline Coverage: 6 Missing Test Scenarios")
class TxnPipelineCoverageTest {

    private static boolean lmdbAvailable = true;

    @BeforeAll
    static void checkLmdbAvailability() {
        Path probeDir = null;
        try {
            probeDir = Files.createTempDirectory("coverage-lmdb-probe-" + UUID.randomUUID());
            LmdbInstance probe =
                    new LmdbInstance("probe-" + UUID.randomUUID(), probeDir, 1024L * 1024, 1);
            probe.close();
        } catch (Throwable t) {
            lmdbAvailable = false;
        } finally {
            if (probeDir != null) {
                try (var stream = Files.walk(probeDir)) {
                    stream.sorted(Comparator.reverseOrder())
                            .forEach(
                                    p -> {
                                        try {
                                            Files.deleteIfExists(p);
                                        } catch (Exception ignored) {
                                        }
                                    });
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void setupClockKey(
            InMemoryMetastore metastore, String catalog, String db, String table)
            throws IOException {
        String clockKey = String.format("/logical_clock/%s/%s/%s", catalog, db, table);
        byte[] zeroBytes = ByteBuffer.allocate(Long.BYTES).putLong(0L).array();
        metastore.put(clockKey.getBytes(StandardCharsets.UTF_8), zeroBytes);
    }

    // ================================================================
    // Test 1: REPEATABLE_READ read-write conflict detection
    // ================================================================

    @Nested
    @DisplayName("Test 1: REPEATABLE_READ Read-Write Conflict Detection")
    class RepeatableReadConflictTest {

        @TempDir Path tempDir;

        private InMemoryMetastore metastore;
        private LmdbInstance lmdbInstance;
        private TxnOrchestratorImpl orchestrator;

        private static final String CATALOG = "rr_cat";
        private static final String DATABASE = "rr_db";
        private static final String TABLE = "rr_table";

        @BeforeEach
        void setUp() throws Exception {
            assumeTrue(lmdbAvailable, "LMDB native library not available");

            metastore = new InMemoryMetastore();
            setupClockKey(metastore, CATALOG, DATABASE, TABLE);

            lmdbInstance =
                    new LmdbInstance(
                            "rr-inst-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024, 5);

            ShardVersionTracker tracker =
                    new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);

            TxnOrchestrator orch =
                    TxnOrchestratorFactory.create(
                            TxnEngineConfig.planA(),
                            metastore,
                            lmdbInstance,
                            CATALOG,
                            DATABASE,
                            TABLE,
                            null,
                            tracker);
            orchestrator = (TxnOrchestratorImpl) orch;
        }

        @AfterEach
        void tearDown() {
            if (orchestrator != null) orchestrator.close();
            if (lmdbInstance != null) lmdbInstance.close();
        }

        @Test
        @DisplayName(
                "T1 reads keyA (REPEATABLE_READ) -> T2 modifies keyA -> T1 writes keyB -> T1 conflicts")
        void testReadWriteConflictDetected() {
            byte[] keyA = bytes("keyA");
            byte[] keyB = bytes("keyB");

            TxnContext setupCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(setupCtx, TABLE, keyA, bytes("initial"), 0);
            TxnResult setupResult = orchestrator.commit(setupCtx);
            assertThat(setupResult).isInstanceOf(TxnResult.CommitOk.class);

            TxnContext t1Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.REPEATABLE_READ);
            Optional<byte[]> t1Read = orchestrator.pointRead(TABLE, keyA, t1Ctx);
            assertThat(t1Read).isPresent();
            assertThat(t1Read.get()).isEqualTo(bytes("initial"));

            TxnContext t2Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(t2Ctx, TABLE, keyA, bytes("modified-by-t2"), 0);
            TxnResult t2Result = orchestrator.commit(t2Ctx);
            assertThat(t2Result).isInstanceOf(TxnResult.CommitOk.class);

            orchestrator.recordWrite(t1Ctx, TABLE, keyB, bytes("t1-write"), 0);
            TxnResult t1Result = orchestrator.commit(t1Ctx);

            assertThat(t1Result)
                    .as(
                            "T1 read keyA, T2 modified keyA → REPEATABLE_READ must detect read-write conflict")
                    .isInstanceOf(TxnResult.CommitConflict.class);
            assertThat(((TxnResult.CommitConflict) t1Result).reason())
                    .contains("Read-write conflict");
        }

        @Test
        @DisplayName(
                "READ_COMMITTED skips read-write check -> T1 succeeds despite T2 modifying read key")
        void testReadCommittedAllowsReadWriteConflict() {
            byte[] keyA = bytes("keyA");
            byte[] keyB = bytes("keyB");

            TxnContext setupCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(setupCtx, TABLE, keyA, bytes("initial"), 0);
            orchestrator.commit(setupCtx);

            TxnContext t1Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.pointRead(TABLE, keyA, t1Ctx);

            TxnContext t2Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(t2Ctx, TABLE, keyA, bytes("modified-by-t2"), 0);
            assertThat(orchestrator.commit(t2Ctx)).isInstanceOf(TxnResult.CommitOk.class);

            orchestrator.recordWrite(t1Ctx, TABLE, keyB, bytes("t1-write"), 0);
            TxnResult t1Result = orchestrator.commit(t1Ctx);

            assertThat(t1Result)
                    .as("READ_COMMITTED does not check read-write conflicts → T1 should succeed")
                    .isInstanceOf(TxnResult.CommitOk.class);
        }
    }

    // ================================================================
    // Test 2: commit() exception path cleanup
    // ================================================================

    @Nested
    @DisplayName("Test 2: commit() Exception Path Cleanup (B-2 fix)")
    @ExtendWith(MockitoExtension.class)
    class CommitExceptionPathTest {

        @Mock private LmdbInstance lmdbInstance;

        private InMemoryMetastore metastore;
        private InMemoryTxnWALWriter walWriter;
        private TxnOrchestratorImpl orchestrator;

        private static final String CATALOG = "exc_cat";
        private static final String DATABASE = "exc_db";
        private static final String TABLE = "exc_table";

        @BeforeEach
        void setUp() throws Exception {
            metastore = new InMemoryMetastore();
            setupClockKey(metastore, CATALOG, DATABASE, TABLE);
            walWriter = new InMemoryTxnWALWriter();

            lenient().when(lmdbInstance.readOnly()).thenReturn(false);
            lenient()
                    .when(lmdbInstance.batchPutWithMarker(any(), any(), any(), any()))
                    .thenThrow(new RuntimeException("Injected LMDB failure"));

            orchestrator =
                    buildMinimalOrchestrator(
                            metastore,
                            lmdbInstance,
                            walWriter,
                            CATALOG,
                            DATABASE,
                            new DefaultPublishStage());
        }

        @AfterEach
        void tearDown() {
            if (orchestrator != null) orchestrator.close();
        }

        @Test
        @DisplayName(
                "DurableStage throws -> commit returns CommitConflict, WAL ABORT written, txn cleaned up")
        void testCommitExceptionWritesWalAbortAndCleansUp() {
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(ctx, TABLE, bytes("k1"), bytes("v1"), 0);

            assertThat(orchestrator.activeTxnCount()).isEqualTo(1);

            TxnResult result = orchestrator.commit(ctx);

            assertThat(result).isInstanceOf(TxnResult.CommitConflict.class);
            assertThat(((TxnResult.CommitConflict) result).reason()).contains("commit failed");

            assertThat(walWriter.scan()).anyMatch(r -> r instanceof TxnWALRecord.TxnAbort);
            assertThat(orchestrator.activeTxnCount()).isZero();
        }
    }

    // ================================================================
    // Test 3: PublishStage failure + WAL ABORT
    // ================================================================

    @Nested
    @DisplayName("Test 3: PublishStage Failure + WAL ABORT")
    @ExtendWith(MockitoExtension.class)
    class PublishStageFailureTest {

        @Mock private LmdbInstance lmdbInstance;

        private InMemoryMetastore metastore;
        private InMemoryTxnWALWriter walWriter;
        private TxnOrchestratorImpl orchestrator;
        private final Map<ByteBuffer, byte[]> lmdbData = new HashMap<>();

        private static final String CATALOG = "pub_cat";
        private static final String DATABASE = "pub_db";
        private static final String TABLE = "pub_table";

        @BeforeEach
        void setUp() throws Exception {
            metastore = new InMemoryMetastore();
            setupClockKey(metastore, CATALOG, DATABASE, TABLE);
            walWriter = new InMemoryTxnWALWriter();
            lmdbData.clear();

            lenient().when(lmdbInstance.readOnly()).thenReturn(false);
            lenient()
                    .when(lmdbInstance.put(any(), any()))
                    .thenAnswer(
                            inv -> {
                                lmdbData.put(
                                        ByteBuffer.wrap(inv.getArgument(0)), inv.getArgument(1));
                                return LmdbInstance.PutStatus.SUCCESS;
                            });
            lenient()
                    .when(lmdbInstance.batchPutWithMarker(any(), any(), any(), any()))
                    .thenAnswer(
                            inv -> {
                                List<byte[]> keys = inv.getArgument(0);
                                List<byte[]> values = inv.getArgument(1);
                                for (int i = 0; i < keys.size(); i++) {
                                    lmdbData.put(ByteBuffer.wrap(keys.get(i)), values.get(i));
                                }
                                lmdbData.put(
                                        ByteBuffer.wrap(inv.getArgument(2)), inv.getArgument(3));
                                return new LmdbInstance.BatchPutResult(
                                        keys.size(), LmdbInstance.PutStatus.SUCCESS);
                            });
            lenient()
                    .when(lmdbInstance.get(any()))
                    .thenAnswer(inv -> lmdbData.get(ByteBuffer.wrap(inv.getArgument(0))));

            PublishStage failingPublishStage =
                    new PublishStage() {
                        @Override
                        public PublishResult execute(
                                TxnContext ctx,
                                List<TxnWALRecord.TxnWriteIntent> writeIntents,
                                Metastore metastore,
                                String catalogName,
                                String databaseName,
                                String tableName,
                                ValidateStage validateStage) {
                            return PublishResult.fail("Injected PublishStage failure");
                        }

                        @Override
                        public boolean createBatchTxnCommitRecord(
                                Metastore metastore,
                                String catalog,
                                String db,
                                String table,
                                String batchId,
                                List<Long> txnIds)
                                throws IOException {
                            throw new IOException("Not implemented");
                        }
                    };

            orchestrator =
                    buildMinimalOrchestrator(
                            metastore,
                            lmdbInstance,
                            walWriter,
                            CATALOG,
                            DATABASE,
                            failingPublishStage);
        }

        @AfterEach
        void tearDown() {
            if (orchestrator != null) orchestrator.close();
        }

        @Test
        @DisplayName(
                "DurableStage succeeds -> PublishStage fails -> WAL ABORT + CommitConflict + marker tombstoned")
        void testPublishStageFailureWritesWalAbort() {
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            long txnId = ctx.txnId();
            orchestrator.recordWrite(ctx, TABLE, bytes("k1"), bytes("v1"), 0);

            TxnResult result = orchestrator.commit(ctx);

            assertThat(result).isInstanceOf(TxnResult.CommitConflict.class);
            assertThat(((TxnResult.CommitConflict) result).reason())
                    .contains("PublishStage failed");

            assertThat(walWriter.scan()).anyMatch(r -> r instanceof TxnWALRecord.TxnAbort);
            assertThat(orchestrator.activeTxnCount()).isZero();

            byte[] markerKey = ("__commit_marker__:" + txnId).getBytes(StandardCharsets.UTF_8);
            byte[] markerValue = lmdbData.get(ByteBuffer.wrap(markerKey));
            boolean tombstoned =
                    markerValue == null || markerValue.length == 0 || markerValue[0] == 0;
            assertThat(tombstoned).as("CommitMarker must be tombstoned").isTrue();
        }
    }

    // ================================================================
    // Test 4: abort() DATA_DURABLE marker protection
    // ================================================================

    @Nested
    @DisplayName("Test 4: abort() DATA_DURABLE Marker Protection")
    class AbortDataDurableTest {

        @TempDir Path tempDir;

        private InMemoryMetastore metastore;
        private LmdbInstance lmdbInstance;
        private TxnOrchestratorImpl orchestrator;

        private static final String CATALOG = "abort_cat";
        private static final String DATABASE = "abort_db";
        private static final String TABLE = "abort_table";

        @BeforeEach
        void setUp() throws Exception {
            assumeTrue(lmdbAvailable, "LMDB native library not available");

            metastore = new InMemoryMetastore();
            setupClockKey(metastore, CATALOG, DATABASE, TABLE);

            lmdbInstance =
                    new LmdbInstance(
                            "abort-inst-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024, 5);

            TxnOrchestrator orch =
                    TxnOrchestratorFactory.create(
                            TxnEngineConfig.planA(),
                            metastore,
                            lmdbInstance,
                            CATALOG,
                            DATABASE,
                            TABLE);
            orchestrator = (TxnOrchestratorImpl) orch;
        }

        @AfterEach
        void tearDown() {
            if (orchestrator != null) orchestrator.close();
            if (lmdbInstance != null) lmdbInstance.close();
        }

        @Test
        @DisplayName("abort() on DATA_DURABLE with active marker -> refused (CommitConflict)")
        void testAbortDataDurableWithMarkerRefused() {
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            long txnId = ctx.txnId();

            byte[] markerKey = ("__commit_marker__:" + txnId).getBytes(StandardCharsets.UTF_8);
            lmdbInstance.put(markerKey, new byte[] {1});

            ctx.transitionTo(TxnState.VALIDATING);
            ctx.transitionTo(TxnState.PREPARING);
            ctx.transitionTo(TxnState.DATA_DURABLE);

            TxnResult result = orchestrator.abort(ctx);

            assertThat(result).isInstanceOf(TxnResult.CommitConflict.class);
            assertThat(((TxnResult.CommitConflict) result).reason())
                    .contains("durable with active marker");
            assertThat(ctx.currentState()).isEqualTo(TxnState.DATA_DURABLE);
        }

        @Test
        @DisplayName("abort() on DATA_DURABLE without marker -> succeeds (AbortResult)")
        void testAbortDataDurableWithoutMarkerSucceeds() {
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);

            ctx.transitionTo(TxnState.VALIDATING);
            ctx.transitionTo(TxnState.PREPARING);
            ctx.transitionTo(TxnState.DATA_DURABLE);

            TxnResult result = orchestrator.abort(ctx);

            assertThat(result).isInstanceOf(TxnResult.AbortResult.class);
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTED);
        }

        @Test
        @DisplayName("abort() on ACTIVE (normal) -> succeeds (AbortResult)")
        void testAbortActiveSucceeds() {
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(ctx, TABLE, bytes("k1"), bytes("v1"), 0);

            TxnResult result = orchestrator.abort(ctx);

            assertThat(result).isInstanceOf(TxnResult.AbortResult.class);
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTED);
        }
    }

    // ================================================================
    // Test 5: recoverWithWAL marker present + replay PublishStage
    // ================================================================

    @Nested
    @DisplayName("Test 5: recoverWithWAL Marker Present + Replay PublishStage")
    class RecoverWithWalMarkerPresentTest {

        @TempDir Path tempDir;

        private InMemoryMetastore metastore;
        private LmdbInstance lmdbInstance;
        private InMemoryTxnWALWriter walWriter;
        private ShardVersionTracker tracker;

        private static final String CATALOG = "rec_cat";
        private static final String DATABASE = "rec_db";
        private static final String TABLE = "rec_table";

        @BeforeEach
        void setUp() throws IOException {
            assumeTrue(lmdbAvailable, "LMDB native library not available");

            metastore = new InMemoryMetastore();
            setupClockKey(metastore, CATALOG, DATABASE, TABLE);

            lmdbInstance =
                    new LmdbInstance(
                            "rec-marker-inst-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024, 5);

            walWriter = new InMemoryTxnWALWriter();
            tracker = new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
        }

        @AfterEach
        void tearDown() {
            if (lmdbInstance != null) lmdbInstance.close();
        }

        private TxnOrchestratorImpl createOrchestrator(LogicalClock clock) {
            VersionChainStore versionChainStore = new VersionChainStore();
            ValidateStage validateStage = new ValidateStage();
            WriteStage writeStage = new DefaultWriteStage();
            DurableStage durableStage = new DefaultDurableStage();
            DefaultPublishStage publishStage = new DefaultPublishStage();

            validateStage.setTracker(tracker);
            publishStage.setTracker(tracker);

            TxnOrchestratorImpl orch =
                    new TxnOrchestratorImpl(
                            clock,
                            metastore,
                            lmdbInstance,
                            versionChainStore,
                            walWriter,
                            validateStage,
                            writeStage,
                            durableStage,
                            publishStage,
                            CATALOG,
                            DATABASE);
            orch.setWalReader(new ScanBackedTxnWALReader(walWriter));
            orch.setTracker(tracker);
            return orch;
        }

        @Test
        @DisplayName(
                "DurableStage completeafter -> recover() detect marker -> replay PublishStage write WAL COMMITTED")
        void testRecoveryReplaysPublishStageWhenMarkerPresent() {
            LogicalClock clock = new AtomicLongClock(0);

            long txnId = clock.next();
            walWriter.writeBegin(
                    new TxnWALRecord.TxnBeginRecord(
                            txnId,
                            clock.now(),
                            IsolationLevel.READ_COMMITTED,
                            Set.of(TABLE),
                            System.currentTimeMillis()));

            byte[] keyBytes = bytes("crashed-key");
            TxnWALRecord.TxnWriteIntent intent =
                    new TxnWALRecord.TxnWriteIntent(
                            txnId,
                            Set.of(TABLE),
                            TABLE,
                            0,
                            0L,
                            IsolationLevel.READ_COMMITTED,
                            0,
                            keyBytes,
                            bytes("crashed-value"),
                            TxnWALRecord.WriteType.UPSERT,
                            0L,
                            0,
                            0L);
            walWriter.writeWriteIntent(intent);

            DefaultDurableStage durableStage = new DefaultDurableStage();
            TxnContext minimalCtx =
                    new TxnContext(
                            txnId,
                            io.github.rawwise.engine.txn.isolate.ReadSnapshot.forLevel(
                                    IsolationLevel.READ_COMMITTED, 0, 0),
                            IsolationLevel.READ_COMMITTED,
                            Set.of(TABLE),
                            new io.github.rawwise.engine.txn.WriteBuffer(),
                            new io.github.rawwise.engine.txn.TxnReadCache());

            DurableStage.DurableResult durableResult =
                    durableStage.execute(
                            minimalCtx, List.of(intent), lmdbInstance, TxnMode.DEFERRED, walWriter);
            assertThat(durableResult.commitMarkerWritten()).isTrue();

            byte[] markerKey = ("__commit_marker__:" + txnId).getBytes(StandardCharsets.UTF_8);
            assertThat(lmdbInstance.get(markerKey)[0]).isEqualTo((byte) 1);

            assertThat(tracker.getCommitTs(keyBytes)).isZero();

            TxnOrchestratorImpl recoveryOrch = createOrchestrator(new AtomicLongClock(0));
            try {
                String recoveryResult = recoveryOrch.recover(txnId);

                assertThat(recoveryResult).isEqualTo("recovered-committed:" + txnId);
                assertThat(tracker.getCommitTs(keyBytes)).isEqualTo(txnId);
                assertThat(walWriter.scan())
                        .anyMatch(
                                r ->
                                        r instanceof TxnWALRecord.TxnCommitted
                                                && ((TxnWALRecord.TxnCommitted) r).txnId()
                                                        == txnId);

                String tcrKey =
                        "/txn_commit_record/"
                                + CATALOG
                                + "/"
                                + DATABASE
                                + "/"
                                + TABLE
                                + "/"
                                + txnId;
                try {
                    assertThat(metastore.get(tcrKey.getBytes(StandardCharsets.UTF_8)))
                            .as("TxnCommitRecord must be written to metastore during replay")
                            .isPresent();
                } catch (IOException e) {
                    throw new AssertionError("Metastore read failed", e);
                }

                // M-3 verify: replayPublishStage update latestSnapshot
                String latestKey = "/snapshot_latest/" + CATALOG + "/" + DATABASE + "/" + TABLE;
                try {
                    Optional<byte[]> latest =
                            metastore.get(latestKey.getBytes(StandardCharsets.UTF_8));
                    assertThat(latest).as("latestSnapshot must be updated by replay").isPresent();
                    assertThat(ByteBuffer.wrap(latest.get()).getLong()).isEqualTo(txnId);
                } catch (IOException e) {
                    throw new AssertionError("Metastore read failed", e);
                }
            } finally {
                recoveryOrch.close();
            }
        }

        @Test
        @DisplayName(
                "WAL has WriteIntent marker missing -> recover() recovered-aborted + write WAL ABORT")
        void testRecoveryAbortsWhenMarkerMissing() {
            LogicalClock clock = new AtomicLongClock(0);

            long txnId = clock.next();
            walWriter.writeBegin(
                    new TxnWALRecord.TxnBeginRecord(
                            txnId,
                            clock.now(),
                            IsolationLevel.READ_COMMITTED,
                            Set.of(TABLE),
                            System.currentTimeMillis()));

            byte[] keyBytes = bytes("incomplete-key");
            walWriter.writeWriteIntent(
                    new TxnWALRecord.TxnWriteIntent(
                            txnId,
                            Set.of(TABLE),
                            TABLE,
                            0,
                            0L,
                            IsolationLevel.READ_COMMITTED,
                            0,
                            keyBytes,
                            bytes("v"),
                            TxnWALRecord.WriteType.UPSERT,
                            0L,
                            0,
                            0L));

            TxnOrchestratorImpl recoveryOrch = createOrchestrator(new AtomicLongClock(0));
            try {
                String recoveryResult = recoveryOrch.recover(txnId);

                assertThat(recoveryResult).isEqualTo("recovered-aborted:" + txnId);
                assertThat(walWriter.scan())
                        .anyMatch(
                                r ->
                                        r instanceof TxnWALRecord.TxnAbort
                                                && ((TxnWALRecord.TxnAbort) r).txnId() == txnId);
                assertThat(tracker.getCommitTs(keyBytes)).isZero();
            } finally {
                recoveryOrch.close();
            }
        }

        // B-1 verify: concurrent recovery + later commit should notroll back version number
        @Test
        @DisplayName(
                "B-1: when tracker inalreadyhasupdateversion ,replay should notroll back version number")
        void testReplayDoesNotRegressTrackerVersion() {
            LogicalClock clock = new AtomicLongClock(0);

            // simulate T1 beforealready DurableStage
            long t1TxnId = clock.next();
            walWriter.writeBegin(
                    new TxnWALRecord.TxnBeginRecord(
                            t1TxnId,
                            clock.now(),
                            IsolationLevel.READ_COMMITTED,
                            Set.of(TABLE),
                            System.currentTimeMillis()));

            byte[] keyBytes = bytes("contended-key");
            TxnWALRecord.TxnWriteIntent intent =
                    new TxnWALRecord.TxnWriteIntent(
                            t1TxnId,
                            Set.of(TABLE),
                            TABLE,
                            0,
                            0L,
                            IsolationLevel.READ_COMMITTED,
                            0,
                            keyBytes,
                            bytes("t1-value"),
                            TxnWALRecord.WriteType.UPSERT,
                            0L,
                            0,
                            0L);
            walWriter.writeWriteIntent(intent);

            DefaultDurableStage durableStage = new DefaultDurableStage();
            TxnContext minimalCtx =
                    new TxnContext(
                            t1TxnId,
                            io.github.rawwise.engine.txn.isolate.ReadSnapshot.forLevel(
                                    IsolationLevel.READ_COMMITTED, 0, 0),
                            IsolationLevel.READ_COMMITTED,
                            Set.of(TABLE),
                            new io.github.rawwise.engine.txn.WriteBuffer(),
                            new io.github.rawwise.engine.txn.TxnReadCache());
            durableStage.execute(
                    minimalCtx, List.of(intent), lmdbInstance, TxnMode.DEFERRED, walWriter);

            // simulate recovery before tracker alreadybeupdateto version(T2 already success commit)
            long higherTxnId = t1TxnId + 100;
            tracker.setCommitTs(keyBytes, higherTxnId, higherTxnId);
            assertThat(tracker.getCommitTs(keyBytes)).isEqualTo(higherTxnId);

            // execute recovery
            TxnOrchestratorImpl recoveryOrch = createOrchestrator(new AtomicLongClock(0));
            try {
                String result = recoveryOrch.recover(t1TxnId);
                assertThat(result).isEqualTo("recovered-committed:" + t1TxnId);

                // B-1: tracker versionshould notbefallbackto t1TxnId
                assertThat(tracker.getCommitTs(keyBytes))
                        .as("tracker must preserve higher version (no regression)")
                        .isEqualTo(higherTxnId);
            } finally {
                recoveryOrch.close();
            }
        }
    }

    // ================================================================
    // Test 6: H-2 + H-1 (recover active txn / cleanup mid-commit)
    // ================================================================

    @Nested
    @DisplayName("Test 6: recover() Active Txn + cleanupExpiredTransactions State Guard (H-1, H-2)")
    class RecoverActiveAndCleanupGuardTest {

        @TempDir Path tempDir;

        private InMemoryMetastore metastore;
        private LmdbInstance lmdbInstance;
        private TxnOrchestratorImpl orchestrator;

        private static final String CATALOG = "act_cat";
        private static final String DATABASE = "act_db";
        private static final String TABLE = "act_table";

        @BeforeEach
        void setUp() throws Exception {
            assumeTrue(lmdbAvailable, "LMDB native library not available");

            metastore = new InMemoryMetastore();
            setupClockKey(metastore, CATALOG, DATABASE, TABLE);

            lmdbInstance =
                    new LmdbInstance(
                            "active-inst-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024, 5);

            TxnOrchestrator orch =
                    TxnOrchestratorFactory.create(
                            TxnEngineConfig.planA(),
                            metastore,
                            lmdbInstance,
                            CATALOG,
                            DATABASE,
                            TABLE);
            orchestrator = (TxnOrchestratorImpl) orch;
        }

        @AfterEach
        void tearDown() {
            if (orchestrator != null) orchestrator.close();
            if (lmdbInstance != null) lmdbInstance.close();
        }

        @Test
        @DisplayName(
                "H-2: recover() on a still-active txn -> aborts ctx and returns aborted-active-txn")
        void testRecoverActiveTxnAborts() {
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            long txnId = ctx.txnId();
            orchestrator.recordWrite(ctx, TABLE, bytes("k"), bytes("v"), 0);

            assertThat(orchestrator.activeTxnCount()).isEqualTo(1);

            String result = orchestrator.recover(txnId);

            assertThat(result).isEqualTo("aborted-active-txn:" + txnId);
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTED);
            assertThat(orchestrator.activeTxnCount()).isZero();
        }

        @Test
        @DisplayName("H-1: cleanupExpiredTransactions skips txn in mid-commit state (DATA_DURABLE)")
        void testCleanupSkipsMidCommitTransaction() {
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            long txnId = ctx.txnId();

            // write marker status to DATA_DURABLE, simulate commit pathalready
            byte[] markerKey = ("__commit_marker__:" + txnId).getBytes(StandardCharsets.UTF_8);
            lmdbInstance.put(markerKey, new byte[] {1});

            ctx.transitionTo(TxnState.VALIDATING);
            ctx.transitionTo(TxnState.PREPARING);
            ctx.transitionTo(TxnState.DATA_DURABLE);

            // trigger cleanup(should not mid-commit transaction)
            orchestrator.cleanupExpiredTransactions();

            // ctx in indexin, statusmaintain DATA_DURABLE
            assertThat(ctx.currentState())
                    .as("cleanup must not abort mid-commit transaction")
                    .isEqualTo(TxnState.DATA_DURABLE);

            // marker active(not yetbe abort tombstone)
            byte[] markerValue = lmdbInstance.get(markerKey);
            assertThat(markerValue).isNotNull();
            assertThat(markerValue.length).isGreaterThan(0);
            assertThat(markerValue[0])
                    .as("marker must NOT be tombstoned by cleanup")
                    .isEqualTo((byte) 1);
        }

        @Test
        @DisplayName("H-1: cleanupExpiredTransactions still aborts ACTIVE transactions")
        void testCleanupAbortsActiveTransaction() {
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(ctx, TABLE, bytes("k"), bytes("v"), 0);

            assertThat(ctx.currentState()).isEqualTo(TxnState.ACTIVE);

            // timeout is , trigger cleanup
            try {
                java.lang.reflect.Field f =
                        TxnOrchestratorImpl.class.getDeclaredField("txnTimeoutIndex");
                f.setAccessible(true);
                @SuppressWarnings("unchecked")
                java.util.concurrent.ConcurrentHashMap<Long, Long> idx =
                        (java.util.concurrent.ConcurrentHashMap<Long, Long>) f.get(orchestrator);
                idx.put(ctx.txnId(), 1L);
            } catch (Exception e) {
                throw new AssertionError(e);
            }

            orchestrator.cleanupExpiredTransactions();

            assertThat(ctx.currentState())
                    .as("ACTIVE expired txn must be aborted by cleanup")
                    .isEqualTo(TxnState.ABORTED);
        }
    }

    // ================================================================
    // Helpers
    // ================================================================

    private static TxnOrchestratorImpl buildMinimalOrchestrator(
            InMemoryMetastore metastore,
            LmdbInstance lmdbInstance,
            InMemoryTxnWALWriter walWriter,
            String catalog,
            String database,
            PublishStage publishStage) {
        LogicalClock clock = new AtomicLongClock(0);
        VersionChainStore versionChainStore = new VersionChainStore();
        ValidateStage validateStage = new ValidateStage();
        WriteStage writeStage = new DefaultWriteStage();
        DurableStage durableStage = new DefaultDurableStage();

        TxnOrchestratorImpl orchestrator =
                new TxnOrchestratorImpl(
                        clock,
                        metastore,
                        lmdbInstance,
                        versionChainStore,
                        walWriter,
                        validateStage,
                        writeStage,
                        durableStage,
                        publishStage,
                        catalog,
                        database);
        orchestrator.setWalReader(new ScanBackedTxnWALReader(walWriter));
        return orchestrator;
    }
}
