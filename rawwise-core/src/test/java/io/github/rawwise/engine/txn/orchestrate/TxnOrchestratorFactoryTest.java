package io.github.rawwise.engine.txn.orchestrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.IsolationLevel;
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

/**
 * E2E business scenario tests for TxnOrchestratorFactory.
 *
 * <p>Validates the complete pipeline: factory creates orchestrator with correct clock type, then
 * orchestrator runs full transaction lifecycle (begin, write, commit).
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("TxnOrchestratorFactory E2E Scenarios")
class TxnOrchestratorFactoryTest {

    private InMemoryMetastore metastore;

    @Mock private LmdbInstance lmdbInstance;

    private static final String CATALOG = "test_catalog";
    private static final String DATABASE = "test_db";
    private static final String TABLE = "test_table";

    @BeforeEach
    void setUp() throws Exception {
        metastore = new InMemoryMetastore();

        // Seed clock key for PLAN_A MetastoreClockImpl.
        // MetastoreClockImpl's CAS uses ValueEquals precondition, which
        // requires the key to exist in the store. Without this seed,
        // store.get() returns null and Arrays.equals(null, expected) fails.
        String clockKey = String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE);
        byte[] zeroBytes = ByteBuffer.allocate(Long.BYTES).putLong(0L).array();
        metastore.put(clockKey.getBytes(StandardCharsets.UTF_8), zeroBytes);

        lenient().when(lmdbInstance.readOnly()).thenReturn(false);
        lenient().when(lmdbInstance.put(any(), any())).thenReturn(LmdbInstance.PutStatus.SUCCESS);
        lenient()
                .when(lmdbInstance.batchPut(any(), any()))
                .thenReturn(new LmdbInstance.BatchPutResult(0, LmdbInstance.PutStatus.SUCCESS));
        byte[] markerBytes = new byte[] {1};
        lenient().when(lmdbInstance.get(any())).thenReturn(markerBytes);
    }

    // ================================================================
    // S1: Happy Path — full transaction lifecycle
    // ================================================================

    @Nested
    @DisplayName("S1: Full transaction lifecycle via factory")
    class FullTransactionLifecycle {

        @Test
        @DisplayName("LEGACY engine: factory creates orchestrator, begin->commit succeeds")
        void testLegacyEngineFactoryCreatesOrchestratorAndCommits() throws Exception {
            // Create orchestrator via factory
            TxnEngineConfig config = TxnEngineConfig.legacy();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, TABLE);

            assertThat(orchestrator).isNotNull();
            assertThat(orchestrator).isInstanceOf(TxnOrchestratorImpl.class);

            // Run full transaction: begin -> commit
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            assertThat(ctx.txnId()).isGreaterThan(0);

            // Record a write
            byte[] key = "test-key".getBytes();
            byte[] value = "test-value".getBytes();
            orchestrator.recordWrite(ctx, TABLE, key, value, 0);

            TxnResult result = orchestrator.commit(ctx);
            assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
        }

        @Test
        @DisplayName("PLAN_A engine: factory creates orchestrator with Metastore clock")
        void testPlanAEngineFactoryCreatesOrchestratorWithMetastoreClock() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.planA();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, TABLE);

            assertThat(orchestrator).isNotNull();
            assertThat(orchestrator).isInstanceOf(TxnOrchestratorImpl.class);

            // Run transaction lifecycle
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            assertThat(ctx.txnId()).isGreaterThan(0);
            assertThat(ctx.currentState())
                    .isEqualTo(io.github.rawwise.engine.txn.state.TxnState.ACTIVE);

            byte[] key = "plan-a-key".getBytes();
            byte[] value = "plan-a-value".getBytes();
            orchestrator.recordWrite(ctx, TABLE, key, value, 0);

            TxnResult result = orchestrator.commit(ctx);
            assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
        }
    }

    // ================================================================
    // S2: Concurrency — multiple orchestrators from factory
    // ================================================================

    @Nested
    @DisplayName("S2: Concurrent orchestrator usage")
    class ConcurrentOrchestratorUsage {

        @Test
        @DisplayName("multiple orchestrators from factory operate independently")
        void testMultipleOrchestratorsOperateIndependently() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.legacy();

            TxnOrchestrator orch1 =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, TABLE);
            TxnOrchestrator orch2 =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, "table2");

            assertThat(orch1).isNotSameAs(orch2);

            TxnContext ctx1 = orch1.begin(Set.of("table1"), IsolationLevel.READ_COMMITTED);
            TxnContext ctx2 = orch2.begin(Set.of("table2"), IsolationLevel.READ_COMMITTED);

            assertThat(ctx1.txnId()).isGreaterThan(0);
            assertThat(ctx2.txnId()).isGreaterThan(0);
            // Different orchestrator instances have independent clocks, so txnIds
            // may or may not be different — we just verify both are positive
        }

        @Test
        @DisplayName("LEGACY clock produces monotonic txnIds across multiple transactions")
        void testLegacyClockProducesMonotonicTxnIds() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, TABLE);

            long prevTxnId = 0;
            for (int i = 0; i < 100; i++) {
                TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
                assertThat(ctx.txnId()).isGreaterThan(prevTxnId);
                prevTxnId = ctx.txnId();
            }
        }
    }

    // ================================================================
    // S3: Fault Recovery — factory creates correct clock recovery path
    // ================================================================

    @Nested
    @DisplayName("S3: Recovery scenarios through factory")
    class Recovery {

        @Test
        @DisplayName(
                "LEGACY engine: recover returns no-state-found for committed txn (LEGACY has no WAL persistence)")
        void testLegacyEngineRecoverResolvesCommittedTxn() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, TABLE);

            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            long txnId = ctx.txnId();

            byte[] key = "recover-test-key".getBytes();
            byte[] value = "recover-test-value".getBytes();
            orchestrator.recordWrite(ctx, TABLE, key, value, 0);

            TxnResult result = orchestrator.commit(ctx);
            assertThat(result).isInstanceOf(TxnResult.CommitOk.class);

            // LEGACY mode uses InMemoryTxnWALWriter — no persistence,
            // so recover returns no-state-found even for committed txns.
            // Override the lenient mock: commit marker is not persisted in LEGACY.
            when(lmdbInstance.get(any())).thenReturn(null);
            String recoveryStatus = orchestrator.recover(txnId);
            assertThat(recoveryStatus).isEqualTo("no-state-found:" + txnId);
        }
    }

    // ================================================================
    // S4: Boundary — factory handles edge configurations
    // ================================================================

    @Nested
    @DisplayName("S4: Boundary configurations")
    class Boundary {

        @Test
        @DisplayName("legacy() factory method creates LEGACY engine config")
        void testDefaultConfigResolvesToLegacy() {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            assertThat(config.getEngine()).isEqualTo(TxnEngineConfig.Engine.LEGACY);
        }

        @Test
        @DisplayName("factory creates working orchestrator for empty table transaction")
        void testEmptyTableTransactionViaFactory() throws Exception {
            TxnEngineConfig config = TxnEngineConfig.legacy();
            TxnOrchestrator orchestrator =
                    TxnOrchestratorFactory.create(
                            config, metastore, lmdbInstance, CATALOG, DATABASE, TABLE);

            // Transaction with no writes should commit successfully
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            TxnResult result = orchestrator.commit(ctx);
            assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
        }
    }
}
