package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorFactory;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("P9e: TxnEngine Grayscale Switch E2E")
class TxnEngineSwitchE2ETest {

    private InMemoryMetastore metastore;

    @Mock private LmdbInstance lmdbInstance;

    private static final String CATALOG = "test_catalog";
    private static final String DATABASE = "test_db";
    private static final String LEGACY_TABLE = "legacy_table";
    private static final String PLAN_A_TABLE = "plan_a_table";

    @BeforeEach
    void setUp() throws Exception {
        metastore = new InMemoryMetastore();

        String legacyClockKey =
                String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, LEGACY_TABLE);
        byte[] zeroBytes = ByteBuffer.allocate(Long.BYTES).putLong(0L).array();
        metastore.put(legacyClockKey.getBytes(StandardCharsets.UTF_8), zeroBytes);

        String planAClockKey =
                String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, PLAN_A_TABLE);
        metastore.put(planAClockKey.getBytes(StandardCharsets.UTF_8), zeroBytes);

        lenient().when(lmdbInstance.readOnly()).thenReturn(false);
        lenient().when(lmdbInstance.put(any(), any())).thenReturn(LmdbInstance.PutStatus.SUCCESS);
        lenient()
                .when(lmdbInstance.batchPut(any(), any()))
                .thenReturn(new LmdbInstance.BatchPutResult(0, LmdbInstance.PutStatus.SUCCESS));
        lenient()
                .when(lmdbInstance.batchPutWithMarker(any(), any(), any(), any()))
                .thenReturn(new LmdbInstance.BatchPutResult(0, LmdbInstance.PutStatus.SUCCESS));
        byte[] markerBytes = new byte[] {1};
        lenient().when(lmdbInstance.get(any())).thenReturn(markerBytes);
    }

    @Nested
    @DisplayName("Legacy engine CRUD")
    class LegacyEngineCrud {

        @Test
        @DisplayName("LEGACY engine: begin -> write -> commit succeeds")
        void testLegacyEngineBeginWriteCommit() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, LEGACY_TABLE);

            TxnContext ctx =
                    orchestrator.begin(Set.of(LEGACY_TABLE), IsolationLevel.READ_COMMITTED);
            assertThat(ctx.txnId()).isGreaterThan(0);

            byte[] key = "legacy-key-1".getBytes();
            byte[] value = "legacy-value-1".getBytes();
            orchestrator.recordWrite(ctx, LEGACY_TABLE, key, value, 0);

            TxnResult result = orchestrator.commit(ctx);
            assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
        }

        @Test
        @DisplayName("LEGACY engine: empty transaction (no writes) commits successfully")
        void testLegacyEngineEmptyCommit() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, LEGACY_TABLE);

            TxnContext ctx =
                    orchestrator.begin(Set.of(LEGACY_TABLE), IsolationLevel.READ_COMMITTED);
            TxnResult result = orchestrator.commit(ctx);
            assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
        }

        @Test
        @DisplayName("LEGACY engine: rollback aborts transaction")
        void testLegacyEngineRollback() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, LEGACY_TABLE);

            TxnContext ctx =
                    orchestrator.begin(Set.of(LEGACY_TABLE), IsolationLevel.READ_COMMITTED);
            assertThat(ctx.txnId()).isGreaterThan(0);

            byte[] key = "legacy-rollback-key".getBytes();
            byte[] value = "legacy-rollback-value".getBytes();
            orchestrator.recordWrite(ctx, LEGACY_TABLE, key, value, 0);

            orchestrator.abort(ctx);
            assertThat(ctx.currentState())
                    .isEqualTo(io.github.rawwise.engine.txn.state.TxnState.ABORTED);
        }

        @Test
        @DisplayName("LEGACY engine: multiple sequential transactions produce monotonic txnIds")
        void testLegacyEngineMonotonicTxnIds() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, LEGACY_TABLE);

            long prevTxnId = 0;
            for (int i = 0; i < 10; i++) {
                TxnContext ctx =
                        orchestrator.begin(Set.of(LEGACY_TABLE), IsolationLevel.READ_COMMITTED);
                assertThat(ctx.txnId()).isGreaterThan(prevTxnId);
                prevTxnId = ctx.txnId();
                orchestrator.commit(ctx);
            }
        }
    }

    @Nested
    @DisplayName("Plan-A engine CRUD")
    class PlanAEngineCrud {

        @Test
        @DisplayName("PLAN_A engine: begin -> write -> commit succeeds")
        void testPlanAEngineBeginWriteCommit() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.planA();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, PLAN_A_TABLE);

            TxnContext ctx =
                    orchestrator.begin(Set.of(PLAN_A_TABLE), IsolationLevel.READ_COMMITTED);
            assertThat(ctx.txnId()).isGreaterThan(0);

            byte[] key = "plan-a-key-1".getBytes();
            byte[] value = "plan-a-value-1".getBytes();
            orchestrator.recordWrite(ctx, PLAN_A_TABLE, key, value, 0);

            TxnResult result = orchestrator.commit(ctx);
            assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
        }

        @Test
        @DisplayName("PLAN_A engine: empty transaction commits successfully")
        void testPlanAEngineEmptyCommit() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.planA();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, PLAN_A_TABLE);

            TxnContext ctx =
                    orchestrator.begin(Set.of(PLAN_A_TABLE), IsolationLevel.READ_COMMITTED);
            TxnResult result = orchestrator.commit(ctx);
            assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
        }

        @Test
        @DisplayName("PLAN_A engine: rollback aborts transaction")
        void testPlanAEngineRollback() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.planA();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, PLAN_A_TABLE);

            TxnContext ctx =
                    orchestrator.begin(Set.of(PLAN_A_TABLE), IsolationLevel.READ_COMMITTED);
            assertThat(ctx.txnId()).isGreaterThan(0);

            byte[] key = "plan-a-rollback-key".getBytes();
            byte[] value = "plan-a-rollback-value".getBytes();
            orchestrator.recordWrite(ctx, PLAN_A_TABLE, key, value, 0);

            orchestrator.abort(ctx);
            assertThat(ctx.currentState())
                    .isEqualTo(io.github.rawwise.engine.txn.state.TxnState.ABORTED);
        }

        @Test
        @DisplayName("PLAN_A engine: multiple sequential transactions produce monotonic txnIds")
        void testPlanAEngineMonotonicTxnIds() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.planA();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, PLAN_A_TABLE);

            long prevTxnId = 0;
            for (int i = 0; i < 10; i++) {
                TxnContext ctx =
                        orchestrator.begin(Set.of(PLAN_A_TABLE), IsolationLevel.READ_COMMITTED);
                assertThat(ctx.txnId()).isGreaterThan(prevTxnId);
                prevTxnId = ctx.txnId();
                orchestrator.commit(ctx);
            }
        }
    }

    @Nested
    @DisplayName("Engine config validation")
    class EngineConfigValidation {

        @Test
        @DisplayName("default config resolves to LEGACY engine")
        void testDefaultConfigIsLegacy() {
            TxnEngineConfig config = new TxnEngineConfig();
            assertThat(config.getEngine()).isEqualTo(TxnEngineConfig.Engine.LEGACY);
        }

        @Test
        @DisplayName("legacy() factory method creates LEGACY engine config")
        void testLegacyFactoryMethod() {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            assertThat(config.getEngine()).isEqualTo(TxnEngineConfig.Engine.LEGACY);
        }

        @Test
        @DisplayName("planA() factory method creates PLAN_A engine config")
        void testPlanAFactoryMethod() {
            TxnEngineConfig config = TxnEngineConfig.planA();
            assertThat(config.getEngine()).isEqualTo(TxnEngineConfig.Engine.PLAN_A);
        }

        @Test
        @DisplayName("unknown system property falls back to LEGACY")
        void testUnknownSystemPropertyFallsBackToLegacy() {
            String original = System.getProperty("rawwise.txn.engine");
            try {
                System.setProperty("rawwise.txn.engine", "unknown-engine");
                TxnEngineConfig config = new TxnEngineConfig();
                assertThat(config.getEngine()).isEqualTo(TxnEngineConfig.Engine.LEGACY);
            } finally {
                if (original != null) {
                    System.setProperty("rawwise.txn.engine", original);
                } else {
                    System.clearProperty("rawwise.txn.engine");
                }
            }
        }

        @Test
        @DisplayName("plan-a system property selects PLAN_A engine")
        void testPlanASystemPropertySelectsPlanA() {
            String original = System.getProperty("rawwise.txn.engine");
            try {
                System.setProperty("rawwise.txn.engine", "plan-a");
                TxnEngineConfig config = new TxnEngineConfig();
                assertThat(config.getEngine()).isEqualTo(TxnEngineConfig.Engine.PLAN_A);
            } finally {
                if (original != null) {
                    System.setProperty("rawwise.txn.engine", original);
                } else {
                    System.clearProperty("rawwise.txn.engine");
                }
            }
        }

        @Test
        @DisplayName("legacy system property selects LEGACY engine")
        void testLegacySystemPropertySelectsLegacy() {
            String original = System.getProperty("rawwise.txn.engine");
            try {
                System.setProperty("rawwise.txn.engine", "legacy");
                TxnEngineConfig config = new TxnEngineConfig();
                assertThat(config.getEngine()).isEqualTo(TxnEngineConfig.Engine.LEGACY);
            } finally {
                if (original != null) {
                    System.setProperty("rawwise.txn.engine", original);
                } else {
                    System.clearProperty("rawwise.txn.engine");
                }
            }
        }

        @Test
        @DisplayName("logStartupInfo() does not throw for LEGACY engine")
        void testLogStartupInfoLegacyDoesNotThrow() {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            config.logStartupInfo();
        }

        @Test
        @DisplayName("logStartupInfo() does not throw for PLAN_A engine")
        void testLogStartupInfoPlanADoesNotThrow() {
            TxnEngineConfig config = TxnEngineConfig.planA();
            config.logStartupInfo();
        }

        @Test
        @DisplayName("default txnTimeoutMs is 300000")
        void testDefaultTxnTimeoutMs() {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            assertThat(config.getTxnTimeoutMs()).isEqualTo(300_000L);
        }

        @Test
        @DisplayName("custom txnTimeoutMs is preserved")
        void testCustomTxnTimeoutMs() {
            TxnEngineConfig config = new TxnEngineConfig(TxnEngineConfig.Engine.LEGACY, 60_000L);
            assertThat(config.getTxnTimeoutMs()).isEqualTo(60_000L);
        }
    }

    @Nested
    @DisplayName("Engine isolation")
    class EngineIsolation {

        @Test
        @DisplayName("LEGACY and PLAN_A orchestrators use independent txnId spaces")
        void testIndependentTxnIdSpaces() throws Exception {
            TxnEngineConfig legacyConfig = TxnEngineConfig.legacy();
            TxnOrchestrator legacyOrch =
                    TxnOrchestratorFactory.create(
                            legacyConfig, metastore, lmdbInstance, CATALOG, DATABASE, LEGACY_TABLE);

            TxnEngineConfig planAConfig = TxnEngineConfig.planA();
            TxnOrchestrator planAOrch =
                    TxnOrchestratorFactory.create(
                            planAConfig, metastore, lmdbInstance, CATALOG, DATABASE, PLAN_A_TABLE);

            TxnContext legacyCtx =
                    legacyOrch.begin(Set.of(LEGACY_TABLE), IsolationLevel.READ_COMMITTED);
            TxnContext planACtx =
                    planAOrch.begin(Set.of(PLAN_A_TABLE), IsolationLevel.READ_COMMITTED);

            assertThat(legacyCtx.txnId()).isGreaterThan(0);
            assertThat(planACtx.txnId()).isGreaterThan(0);

            legacyOrch.commit(legacyCtx);
            planAOrch.commit(planACtx);
        }

        @Test
        @DisplayName(
                "LEGACY and PLAN_A engines can run transactions concurrently on different tables")
        void testConcurrentEnginesOnDifferentTables() throws Exception {
            TxnEngineConfig legacyConfig = TxnEngineConfig.legacy();
            TxnOrchestrator legacyOrch =
                    TxnOrchestratorFactory.create(
                            legacyConfig, metastore, lmdbInstance, CATALOG, DATABASE, LEGACY_TABLE);

            TxnEngineConfig planAConfig = TxnEngineConfig.planA();
            TxnOrchestrator planAOrch =
                    TxnOrchestratorFactory.create(
                            planAConfig, metastore, lmdbInstance, CATALOG, DATABASE, PLAN_A_TABLE);

            for (int i = 0; i < 5; i++) {
                TxnContext legacyCtx =
                        legacyOrch.begin(Set.of(LEGACY_TABLE), IsolationLevel.READ_COMMITTED);
                legacyOrch.recordWrite(
                        legacyCtx,
                        LEGACY_TABLE,
                        ("legacy-key-" + i).getBytes(),
                        ("legacy-value-" + i).getBytes(),
                        0);
                TxnResult legacyResult = legacyOrch.commit(legacyCtx);
                assertThat(legacyResult).isInstanceOf(TxnResult.CommitOk.class);

                TxnContext planACtx =
                        planAOrch.begin(Set.of(PLAN_A_TABLE), IsolationLevel.READ_COMMITTED);
                planAOrch.recordWrite(
                        planACtx,
                        PLAN_A_TABLE,
                        ("plan-a-key-" + i).getBytes(),
                        ("plan-a-value-" + i).getBytes(),
                        0);
                TxnResult planAResult = planAOrch.commit(planACtx);
                assertThat(planAResult).isInstanceOf(TxnResult.CommitOk.class);
            }
        }
    }
}
