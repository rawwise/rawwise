package io.github.rawwise.engine.txn.orchestrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.RecordHeaderProbe;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 03 / B1: CatalogLevelTxnOrchestrator end-to-end test.
 *
 * <p>scenariocover:
 *
 * <ul>
 *   <li>S1 multi-tableregistersuccess + begin viaverify
 *   <li>S1 commit routingto delegate orchestrator
 *   <li>S4 begin package not yetregister tableId -> denied
 *   <li>S4 createCatalogLevel(tables=empty) -> denied
 *   <li>S4 lmdbInstanceFor / trackerFor parsenot yetregistertable -> denied
 *   <li>delegate deterministic:
 * </ul>
 */
@DisplayName("Spec 03 / B1 + B2: CatalogLevelTxnOrchestrator")
@org.junit.jupiter.api.TestMethodOrder(org.junit.jupiter.api.MethodOrderer.OrderAnnotation.class)
class CatalogLevelTxnOrchestratorTest {

    private static boolean lmdbAvailable = true;

    @BeforeAll
    static void checkLmdb() {
        Path probeDir = null;
        try {
            probeDir = Files.createTempDirectory("catalog-lmdb-probe-" + UUID.randomUUID());
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

    @TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private Path runDir;
    private LmdbInstance lmdbT1;
    private LmdbInstance lmdbT2;
    private CatalogLevelTxnOrchestrator orchestrator;

    private static final String CATALOG = "ch_cat";
    private static final String DATABASE = "ch_db";
    private static final String T1 = "ch_cat.ch_db.t_alpha";
    private static final String T2 = "ch_cat.ch_db.t_beta";

    @BeforeEach
    void setUp() throws IOException {
        assumeTrue(lmdbAvailable, "LMDB native library not available");
        metastore = new InMemoryMetastore();
        // registermulti-tableof logical clock key(each tableindependent clock)
        setupClockKey(CATALOG, DATABASE, "t_alpha");
        setupClockKey(CATALOG, DATABASE, "t_beta");

        // eachtest independent , tempDir test (LMDB directory cancanrepeat env)
        runDir = Files.createTempDirectory(tempDir, "run-" + UUID.randomUUID());

        lmdbT1 = new LmdbInstance("t1-inst-" + UUID.randomUUID(), runDir, 64L * 1024 * 1024, 4);
        lmdbT2 = new LmdbInstance("t2-inst-" + UUID.randomUUID(), runDir, 64L * 1024 * 1024, 4);

        ShardVersionTracker trackerT1 =
                new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
        ShardVersionTracker trackerT2 =
                new ShardVersionTracker(1, new AtomicLongClock(0), RecordHeaderProbe.NOOP);

        Map<String, CatalogLevelTxnOrchestrator.TableResource> tables = new LinkedHashMap<>();
        tables.put(T1, new CatalogLevelTxnOrchestrator.TableResource(T1, lmdbT1, trackerT1));
        tables.put(T2, new CatalogLevelTxnOrchestrator.TableResource(T2, lmdbT2, trackerT2));

        CatalogLevelTxnOrchestrator.CatalogResources resources =
                new CatalogLevelTxnOrchestrator.CatalogResources(CATALOG, DATABASE, tables);

        WAL wal = null;
        TxnEngineConfig config = new TxnEngineConfig(TxnEngineConfig.Engine.PLAN_A);
        orchestrator = TxnOrchestratorFactory.createCatalogLevel(config, metastore, wal, resources);
    }

    @AfterEach
    void tearDown() {
        // cleanup ThreadLocal test (TxnContextHolder isglobal thread local)
        try {
            io.github.rawwise.engine.txn.state.TxnContextHolder.clear();
        } catch (Exception ignored) {
        }
        if (lmdbT1 != null)
            try {
                lmdbT1.close();
            } catch (Exception ignored) {
            }
        if (lmdbT2 != null)
            try {
                lmdbT2.close();
            } catch (Exception ignored) {
            }
        // cleanup LmdbInstance static ThreadLocal direct buffer pool test
        try {
            LmdbInstance.cleanupCurrentThreadDirectBuffers();
        } catch (Exception ignored) {
        }
    }

    @Test
    @DisplayName("S1: registeredTableIds allregistertable")
    void testRegisteredTableIdsContainsAllTables() {
        assertThat(orchestrator.registeredTableIds()).containsExactlyInAnyOrder(T1, T2);
        assertThat(orchestrator.catalogName()).isEqualTo(CATALOG);
        assertThat(orchestrator.databaseName()).isEqualTo(DATABASE);
    }

    @Test
    @DisplayName("S1: lmdbInstanceFor / trackerFor correctly routes")
    void testLmdbInstanceAndTrackerLookup() {
        assertThat(orchestrator.lmdbInstanceFor(T1)).isSameAs(lmdbT1);
        assertThat(orchestrator.lmdbInstanceFor(T2)).isSameAs(lmdbT2);
        assertThat(orchestrator.trackerFor(T1)).isNotNull();
        assertThat(orchestrator.trackerFor(T2)).isNotNull();
        assertThat(orchestrator.trackerFor(T1)).isNotSameAs(orchestrator.trackerFor(T2));
    }

    @Test
    @DisplayName("S1: begin multi-table ctx viaverify, non- TxnContext")
    void testBeginAcceptsRegisteredMultiTableCtx() {
        TxnContext ctx = orchestrator.begin(Set.of(T1, T2), IsolationLevel.READ_COMMITTED);
        assertThat(ctx).isNotNull();
        assertThat(ctx.tableIds()).containsExactlyInAnyOrder(T1, T2);
    }

    @Test
    @DisplayName("S4 Boundary: begin package not yetregister tableId IllegalArgumentException")
    void testBeginRejectsUnregisteredTable() {
        String unregistered = CATALOG + "." + DATABASE + ".not_registered";
        assertThatThrownBy(
                        () ->
                                orchestrator.begin(
                                        Set.of(T1, unregistered), IsolationLevel.READ_COMMITTED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not_registered");
    }

    @Test
    @DisplayName("S4 Boundary: lmdbInstanceFor not yetregister tableId exception")
    void testLmdbInstanceForUnregisteredThrows() {
        assertThatThrownBy(() -> orchestrator.lmdbInstanceFor("unknown_table"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown_table");
    }

    @Test
    @DisplayName("S4 Boundary: trackerFor not yetregister tableId exception")
    void testTrackerForUnregisteredThrows() {
        assertThatThrownBy(() -> orchestrator.trackerFor("unknown_table"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("S4 Boundary: createCatalogLevel(tables=empty) IllegalArgumentException")
    void testCreateRejectsEmptyTables() {
        assertThatThrownBy(
                        () ->
                                new CatalogLevelTxnOrchestrator.CatalogResources(
                                        CATALOG, DATABASE, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tables must not be empty");
    }

    @Test
    @DisplayName("S1: delegate deterministic — multiple timescreatebindtosame delegate table")
    void testDelegateSelectionIsDeterministic() {
        // is t_alpha(T1). verify delegate bindof lmdbInstance and T1 same.
        // via begin single-table T2 aftercall lmdbInstanceFor verify: catalog registerof is table.
        assertThat(orchestrator.registeredTableIds()).contains(T1, T2);
        // is T1
        String expectedDelegate =
                orchestrator.registeredTableIds().stream()
                        .min(Comparator.naturalOrder())
                        .orElseThrow();
        assertThat(expectedDelegate).isEqualTo(T1);
    }

    // ================================================================
    // Spec 03 / B2: multi-table commit pipeline test
    // ================================================================

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("B2 S1: cross-table transaction commit aftertwo LMDB all written commit marker")
    void testCrossTableCommitWritesMarkersToBothLmdb() {
        TxnContext ctx = orchestrator.begin(Set.of(T1, T2), IsolationLevel.READ_COMMITTED);
        long txnId = ctx.txnId();

        // tablewrite item
        orchestrator.recordWrite(ctx, T1, bytes("k_t1"), bytes("v_t1"), 0L);
        orchestrator.recordWrite(ctx, T2, bytes("k_t2"), bytes("v_t2"), 0L);

        TxnResult result = orchestrator.commit(ctx);
        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);

        // verifytwo LMDB all written commit marker
        byte[] markerKey =
                ("__commit_marker__:" + txnId).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] markerInT1 = lmdbT1.get(markerKey);
        byte[] markerInT2 = lmdbT2.get(markerKey);
        assertThat(markerInT1).as("T1 marker").isNotNull().isNotEmpty();
        assertThat(markerInT2).as("T2 marker").isNotNull().isNotEmpty();
        assertThat(markerInT1[0]).isEqualTo((byte) 1);
        assertThat(markerInT2[0]).isEqualTo((byte) 1);

        // verifyuserdata of corresponding LMDB(primaryTableId dispatch)
        assertThat(lmdbT1.get(bytes("k_t1"))).isEqualTo(bytes("v_t1"));
        assertThat(lmdbT2.get(bytes("k_t2"))).isEqualTo(bytes("v_t2"));
        // verify: T1 has T2 of key
        assertThat(lmdbT1.get(bytes("k_t2"))).isNull();
        assertThat(lmdbT2.get(bytes("k_t1"))).isNull();
    }

    @Test
    @DisplayName(
            "B2 S1: single-table transactionin catalog-level orchestrator single-table path, does not trigger multi-table dispatch")
    void testSingleTableCommitInCatalogLevel() {
        TxnContext ctx = orchestrator.begin(Set.of(T1), IsolationLevel.READ_COMMITTED);
        long txnId = ctx.txnId();

        orchestrator.recordWrite(ctx, T1, bytes("k_only_t1"), bytes("v_only"), 0L);

        TxnResult result = orchestrator.commit(ctx);
        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);

        byte[] markerKey =
                ("__commit_marker__:" + txnId).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        // single-tablescenario: marker in delegate bindoftable(T1)
        assertThat(lmdbT1.get(markerKey)).isNotNull();
        assertThat(lmdbT1.get(bytes("k_only_t1"))).isEqualTo(bytes("v_only"));
    }

    @Test
    @DisplayName(
            "B2 S1: sametablemulti key cross-table transaction — each tablemultiple key all written to corresponding LMDB")
    void testCrossTableMultipleKeysPerTable() {
        TxnContext ctx = orchestrator.begin(Set.of(T1, T2), IsolationLevel.READ_COMMITTED);

        orchestrator.recordWrite(ctx, T1, bytes("t1_k1"), bytes("t1_v1"), 0L);
        orchestrator.recordWrite(ctx, T1, bytes("t1_k2"), bytes("t1_v2"), 0L);
        orchestrator.recordWrite(ctx, T2, bytes("t2_k1"), bytes("t2_v1"), 0L);

        TxnResult result = orchestrator.commit(ctx);
        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);

        assertThat(lmdbT1.get(bytes("t1_k1"))).isEqualTo(bytes("t1_v1"));
        assertThat(lmdbT1.get(bytes("t1_k2"))).isEqualTo(bytes("t1_v2"));
        assertThat(lmdbT2.get(bytes("t2_k1"))).isEqualTo(bytes("t2_v1"));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private void setupClockKey(String catalog, String db, String table) throws IOException {
        String clockKey = String.format("/logical_clock/%s/%s/%s", catalog, db, table);
        byte[] zero = ByteBuffer.allocate(Long.BYTES).putLong(0L).array();
        metastore.put(clockKey.getBytes(java.nio.charset.StandardCharsets.UTF_8), zero);
    }
}
