package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorFactory;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorImpl;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.*;

@DisplayName("Per-key OCC E2E tests")
class PerKeyOccE2ETest {

    @org.junit.jupiter.api.io.TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private LmdbInstance lmdbInstance;
    private TxnOrchestratorImpl orchestrator;
    private ShardVersionTracker tracker;

    private static final String CATALOG = "perkey";
    private static final String DATABASE = "default";
    private static final String TABLE = "occ_table";

    @BeforeEach
    void setUp() throws Exception {
        metastore = new InMemoryMetastore();
        byte[] clockKey =
                String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE)
                        .getBytes(StandardCharsets.UTF_8);
        metastore.put(clockKey, new byte[] {0, 0, 0, 0, 0, 0, 0, 1});

        lmdbInstance =
                new LmdbInstance(
                        "perkey-occ-inst-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024);

        tracker = new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);

        OccConfig occConfig = new OccConfig(true);
        TxnOrchestrator orch =
                TxnOrchestratorFactory.create(
                        TxnEngineConfig.planA(),
                        metastore,
                        lmdbInstance,
                        CATALOG,
                        DATABASE,
                        TABLE,
                        null,
                        tracker,
                        occConfig);
        orchestrator = (TxnOrchestratorImpl) orch;
    }

    @AfterEach
    void tearDown() {
        if (orchestrator != null) orchestrator.close();
        if (lmdbInstance != null) lmdbInstance.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Two concurrent writes to same key: one succeeds, one conflicts")
    void testConcurrentWriteToSameKeyConflict() {
        byte[] keyA = bytes("keyA");

        TxnContext setupCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(setupCtx, TABLE, keyA, bytes("v1"), 0);
        TxnResult setupResult = orchestrator.commit(setupCtx);
        assertThat(setupResult).isInstanceOf(TxnResult.CommitOk.class);

        TxnContext t1Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.pointRead(TABLE, keyA, t1Ctx);
        orchestrator.recordWrite(t1Ctx, TABLE, keyA, bytes("v2"), 0);

        TxnContext t2Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(t2Ctx, TABLE, keyA, bytes("v3"), 0);
        TxnResult t2Result = orchestrator.commit(t2Ctx);
        assertThat(t2Result).isInstanceOf(TxnResult.CommitOk.class);

        TxnResult t1Result = orchestrator.commit(t1Ctx);
        assertThat(t1Result)
                .as("T1 wrote keyA but T2 already modified it → per-key OCC must detect conflict")
                .isInstanceOf(TxnResult.CommitConflict.class);
    }

    @Test
    @DisplayName("Non-conflicting keys: both transactions succeed")
    void testNonConflictingKeysBothSucceed() {
        byte[] keyA = bytes("keyA");
        byte[] keyB = bytes("keyB");

        TxnContext setupCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(setupCtx, TABLE, keyA, bytes("v1"), 0);
        orchestrator.recordWrite(setupCtx, TABLE, keyB, bytes("v1"), 0);
        orchestrator.commit(setupCtx);

        TxnContext t1Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(t1Ctx, TABLE, keyA, bytes("v2"), 0);

        TxnContext t2Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(t2Ctx, TABLE, keyB, bytes("v2"), 0);

        TxnResult t1Result = orchestrator.commit(t1Ctx);
        TxnResult t2Result = orchestrator.commit(t2Ctx);

        assertThat(t1Result).isInstanceOf(TxnResult.CommitOk.class);
        assertThat(t2Result).isInstanceOf(TxnResult.CommitOk.class);
    }

    @Test
    @DisplayName("Conflict retry: failed txn retries and succeeds after conflicting txn completes")
    void testConflictRetrySucceeds() {
        byte[] keyA = bytes("keyA");

        TxnContext setupCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(setupCtx, TABLE, keyA, bytes("v1"), 0);
        orchestrator.commit(setupCtx);

        TxnContext t2Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(t2Ctx, TABLE, keyA, bytes("fast-writer"), 0);
        TxnResult t2Result = orchestrator.commit(t2Ctx);
        assertThat(t2Result).isInstanceOf(TxnResult.CommitOk.class);

        TxnContext retryCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.pointRead(TABLE, keyA, retryCtx);
        orchestrator.recordWrite(retryCtx, TABLE, keyA, bytes("retry-writer"), 0);
        TxnResult retryResult = orchestrator.commit(retryCtx);

        assertThat(retryResult)
                .as("Retry txn after reading latest version should succeed")
                .isInstanceOf(TxnResult.CommitOk.class);

        long finalTs = tracker.getCommitTs(keyA);
        assertThat(finalTs).isGreaterThan(0);
    }

    @Test
    @DisplayName("Per-key OCC disabled: falls back to existing manual OCC")
    void testPerKeyOccDisabledFallsBack() {
        byte[] keyA = bytes("keyA");

        ShardVersionTracker legacyTracker =
                new ShardVersionTracker(99, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
        OccConfig disabledConfig = new OccConfig(false);
        TxnOrchestrator orch =
                TxnOrchestratorFactory.create(
                        TxnEngineConfig.planA(),
                        metastore,
                        lmdbInstance,
                        CATALOG,
                        DATABASE,
                        TABLE + "_legacy",
                        null,
                        legacyTracker,
                        disabledConfig);

        TxnContext setupCtx = orch.begin(Set.of(TABLE + "_legacy"), IsolationLevel.READ_COMMITTED);
        orch.recordWrite(setupCtx, TABLE + "_legacy", keyA, bytes("v1"), 0);
        orch.commit(setupCtx);

        TxnContext t1Ctx = orch.begin(Set.of(TABLE + "_legacy"), IsolationLevel.READ_COMMITTED);
        orch.recordWrite(t1Ctx, TABLE + "_legacy", keyA, bytes("v2"), 0);
        TxnResult t1Result = orch.commit(t1Ctx);
        assertThat(t1Result).isInstanceOf(TxnResult.CommitOk.class);

        ((TxnOrchestratorImpl) orch).close();
    }
}
