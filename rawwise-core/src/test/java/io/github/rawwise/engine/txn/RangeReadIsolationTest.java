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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("Range Read Isolation E2E")
class RangeReadIsolationTest {

    private InMemoryMetastore metastore;

    @Mock private LmdbInstance lmdbInstance;

    private static final String CATALOG = "range_catalog";
    private static final String DATABASE = "range_db";
    private static final String TABLE = "range_table";

    @BeforeEach
    void setUp() throws Exception {
        metastore = new InMemoryMetastore();

        String clockKey = String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE);
        byte[] zeroBytes = ByteBuffer.allocate(Long.BYTES).putLong(0L).array();
        metastore.put(clockKey.getBytes(StandardCharsets.UTF_8), zeroBytes);

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

    private TxnOrchestrator createOrchestrator() {
        return TxnOrchestratorFactory.create(
                TxnEngineConfig.legacy(), metastore, lmdbInstance, CATALOG, DATABASE, TABLE);
    }

    private byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private Set<String> keysToStrings(List<?> results) {
        return results.stream()
                .map(
                        kv -> {
                            try {
                                byte[] key = (byte[]) kv.getClass().getMethod("key").invoke(kv);
                                return new String(key, StandardCharsets.UTF_8);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                .collect(Collectors.toSet());
    }

    private void commitWrites(TxnOrchestrator orchestrator, byte[]... kvPairs) {
        TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        for (int i = 0; i < kvPairs.length; i += 2) {
            orchestrator.recordWrite(ctx, TABLE, kvPairs[i], kvPairs[i + 1], 0);
        }
        TxnResult result = orchestrator.commit(ctx);
        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
    }

    @Nested
    @DisplayName("S1: Happy Path")
    class HappyPathScenarios {

        @Test
        @DisplayName("Range read returns all committed data in [k1, k5]")
        void testRangeReadReturnsCommittedData() {
            TxnOrchestrator orchestrator = createOrchestrator();

            commitWrites(
                    orchestrator,
                    bytes("k1"),
                    bytes("v1"),
                    bytes("k2"),
                    bytes("v2"),
                    bytes("k3"),
                    bytes("v3"),
                    bytes("k4"),
                    bytes("v4"),
                    bytes("k5"),
                    bytes("v5"));

            TxnContext readerCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            var results = orchestrator.rangeRead(TABLE, bytes("k1"), bytes("k5"), readerCtx);

            assertThat(results).hasSize(5);
            assertThat(keysToStrings(results))
                    .containsExactlyInAnyOrder("k1", "k2", "k3", "k4", "k5");

            orchestrator.commit(readerCtx);
        }

        @Test
        @DisplayName("Range read excludes uncommitted data from another transaction")
        void testRangeReadExcludesUncommittedData() {
            TxnOrchestrator orchestrator = createOrchestrator();

            commitWrites(
                    orchestrator,
                    bytes("k1"),
                    bytes("v1"),
                    bytes("k2"),
                    bytes("v2"),
                    bytes("k4"),
                    bytes("v4"),
                    bytes("k5"),
                    bytes("v5"));

            TxnContext writerCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(writerCtx, TABLE, bytes("k3"), bytes("uncommitted-v3"), 0);

            TxnContext readerCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            var results = orchestrator.rangeRead(TABLE, bytes("k1"), bytes("k5"), readerCtx);

            assertThat(results).hasSize(4);
            assertThat(keysToStrings(results)).containsExactlyInAnyOrder("k1", "k2", "k4", "k5");
            assertThat(keysToStrings(results)).doesNotContain("k3");

            orchestrator.commit(readerCtx);
            orchestrator.abort(writerCtx);
        }
    }

    @Nested
    @DisplayName("S2: Concurrency")
    class ConcurrencyScenarios {

        @Test
        @DisplayName("Concurrent range read maintains consistent snapshot under REPEATABLE_READ")
        void testConcurrentRangeReadConsistentSnapshot() {
            TxnOrchestrator orchestrator = createOrchestrator();

            commitWrites(
                    orchestrator,
                    bytes("k1"),
                    bytes("v1"),
                    bytes("k2"),
                    bytes("v2"),
                    bytes("k4"),
                    bytes("v4"),
                    bytes("k5"),
                    bytes("v5"));

            TxnContext txnA = orchestrator.begin(Set.of(TABLE), IsolationLevel.REPEATABLE_READ);

            var firstRead = orchestrator.rangeRead(TABLE, bytes("k1"), bytes("k5"), txnA);
            assertThat(firstRead).hasSize(4);
            assertThat(keysToStrings(firstRead)).containsExactlyInAnyOrder("k1", "k2", "k4", "k5");

            TxnContext txnB = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(txnB, TABLE, bytes("k3"), bytes("v3"), 0);
            TxnResult txnBResult = orchestrator.commit(txnB);
            assertThat(txnBResult).isInstanceOf(TxnResult.CommitOk.class);

            var secondRead = orchestrator.rangeRead(TABLE, bytes("k1"), bytes("k5"), txnA);
            assertThat(secondRead).hasSize(4);
            assertThat(keysToStrings(secondRead)).containsExactlyInAnyOrder("k1", "k2", "k4", "k5");
            assertThat(keysToStrings(secondRead)).doesNotContain("k3");

            orchestrator.commit(txnA);
        }
    }

    @Nested
    @DisplayName("S4: Boundary")
    class BoundaryScenarios {

        @Test
        @DisplayName("Range read on empty range returns empty list")
        void testRangeReadEmptyRange() {
            TxnOrchestrator orchestrator = createOrchestrator();

            commitWrites(
                    orchestrator,
                    bytes("k1"),
                    bytes("v1"),
                    bytes("k2"),
                    bytes("v2"),
                    bytes("k3"),
                    bytes("v3"));

            TxnContext readerCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            var results = orchestrator.rangeRead(TABLE, bytes("x10"), bytes("x20"), readerCtx);

            assertThat(results).isEmpty();

            orchestrator.commit(readerCtx);
        }

        @Test
        @DisplayName("Range read with start == end returns single key")
        void testRangeReadSingleKey() {
            TxnOrchestrator orchestrator = createOrchestrator();

            commitWrites(
                    orchestrator,
                    bytes("k1"),
                    bytes("v1"),
                    bytes("k2"),
                    bytes("v2"),
                    bytes("k3"),
                    bytes("v3"));

            TxnContext readerCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            var results = orchestrator.rangeRead(TABLE, bytes("k2"), bytes("k2"), readerCtx);

            assertThat(results).hasSize(1);
            assertThat(keysToStrings(results)).containsExactly("k2");
            assertThat(results.get(0).value()).isEqualTo(bytes("v2"));

            orchestrator.commit(readerCtx);
        }
    }
}
