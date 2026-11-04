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

/**
 * Regression test locking the invariant: OLTP commit MUST NOT advance per-table snapshotId.
 *
 * <p>SnapshotId is only advanced by flush (LazyUpdate / LSM) and ingest paths, never by OLTP
 * row-level commits. This test verifies:
 *
 * <ol>
 *   <li>OLTP write -> commit succeeds
 *   <li>Data is readable via LMDB (snapshotId=0 sentinel semantics)
 *   <li>TxnOrchestrator has no SnapshotManager dependency
 * </ol>
 */
@DisplayName("OLTP commit does not advance snapshot")
class OltpNoSnapshotCommitE2ETest {

    @org.junit.jupiter.api.io.TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private LmdbInstance lmdbInstance;
    private TxnOrchestratorImpl orchestrator;

    private static final String CATALOG = "oltp";
    private static final String DATABASE = "default";
    private static final String TABLE = "nosnap";

    @BeforeEach
    void setUp() throws Exception {
        metastore = new InMemoryMetastore();
        byte[] clockKey =
                String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE)
                        .getBytes(StandardCharsets.UTF_8);
        metastore.put(clockKey, new byte[] {0, 0, 0, 0, 0, 0, 0, 1});

        lmdbInstance =
                new LmdbInstance("oltp-nosnap-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024);

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

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("OLTP INSERT -> commit succeeds -> data readable via LMDB (snapshotId=0)")
    void testOltpCommitDoesNotAdvanceSnapshot() {
        byte[] keyA = bytes("keyA");
        byte[] value = bytes("hello-oltp");

        TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(ctx, TABLE, keyA, value, 0);
        TxnResult result = orchestrator.commit(ctx);

        assertThat(result).as("OLTP commit must succeed").isInstanceOf(TxnResult.CommitOk.class);

        byte[] lmdbValue = lmdbInstance.get(keyA);
        assertThat(lmdbValue)
                .as("Data must be readable via LMDB direct path (snapshotId=0 sentinel semantics)")
                .isNotNull();
    }

    @Test
    @DisplayName("Multiple OLTP commits: all succeed, no snapshot interference")
    void testMultipleOltpCommitsAllSucceed() {
        for (int i = 0; i < 5; i++) {
            byte[] key = bytes("key" + i);
            byte[] val = bytes("val" + i);

            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(ctx, TABLE, key, val, 0);
            TxnResult result = orchestrator.commit(ctx);

            assertThat(result)
                    .as("OLTP commit %d must succeed", i)
                    .isInstanceOf(TxnResult.CommitOk.class);
        }

        for (int i = 0; i < 5; i++) {
            assertThat(lmdbInstance.get(bytes("key" + i)))
                    .as("All data must be readable via LMDB after %d OLTP commits", 5)
                    .isNotNull();
        }
    }

    @Test
    @DisplayName("TxnOrchestrator has no SnapshotManager dependency (structural invariant)")
    void testTxnOrchestratorHasNoSnapshotManagerDependency() {
        Class<?> clz = orchestrator.getClass();
        java.lang.reflect.Field[] fields = clz.getDeclaredFields();

        for (java.lang.reflect.Field f : fields) {
            assertThat(f.getType().getName())
                    .as(
                            "TxnOrchestrator must not have SnapshotManager dependency: field '%s'",
                            f.getName())
                    .doesNotContain("SnapshotManager");
        }
    }
}
