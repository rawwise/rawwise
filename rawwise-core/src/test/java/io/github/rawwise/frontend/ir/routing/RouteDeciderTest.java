package io.github.rawwise.frontend.ir.routing;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.engine.txn.ReadSnapshot;
import io.github.rawwise.frontend.ir.*;
import io.github.rawwise.model.record.*;
import io.github.rawwise.model.record.PrimitiveType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;

@DisplayName("RouteDecider")
class RouteDeciderTest {

    private FastPathExecutor fastPathExecutor;
    private RouteDecider decider;
    private TableMetaIR meta;

    @BeforeEach
    void setUp() {
        fastPathExecutor = new FastPathExecutor();
        decider = new RouteDecider(fastPathExecutor);
        StructField idField = new StructField("id", PrimitiveType.LONG);
        StructType schema = new StructType(List.of(idField));
        meta = new TableMetaIR("t", 1L, TableType.PRIMARY_KEY, schema, List.of("id"), Map.of(), 1);
    }

    @Test
    @DisplayName("PointQueryIR -> FAST_PATH when executors available")
    void pointQueryFastPath() {
        fastPathExecutor.registerExecutor("e1", new StubExecutor());
        var ir =
                new PointQueryIR(
                        meta,
                        null,
                        0L,
                        new ReadSnapshot(0L, 0L),
                        Map.of("id", 1L),
                        new byte[] {1},
                        List.of(),
                        "e1",
                        0,
                        0L);
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.FAST_PATH);
    }

    @Test
    @DisplayName("BatchPointQueryIR -> FAST_PATH when executors available")
    void batchPointQueryFastPath() {
        fastPathExecutor.registerExecutor("e1", new StubExecutor());
        var ir =
                new BatchPointQueryIR(
                        meta,
                        null,
                        0L,
                        new ReadSnapshot(0L, 0L),
                        List.of(new byte[] {1}),
                        List.of(),
                        "e1",
                        0,
                        0L);
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.FAST_PATH);
    }

    @Test
    @DisplayName("MetadataQueryIR.CountAll -> METADATA_ONLY")
    void metadataCountAll() {
        var ir = new MetadataQueryIR.CountAll(meta, 1L);
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.METADATA_ONLY);
    }

    @Test
    @DisplayName("MetadataQueryIR.TableInfo -> METADATA_ONLY")
    void metadataTableInfo() {
        var ir = new MetadataQueryIR.TableInfo(meta, 1L);
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.METADATA_ONLY);
    }

    @Test
    @DisplayName("ScanQueryIR -> LOGICAL_PLAN")
    void scanLogicalPlan() {
        var ir = new ScanQueryIR(meta, 1L, List.of(), null, List.of(), 0, 0, null);
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.LOGICAL_PLAN);
    }

    @Test
    @DisplayName("AggregateQueryIR -> LOGICAL_PLAN")
    void aggregateLogicalPlan() {
        var ir = new AggregateQueryIR(List.of(meta), 1L, List.of(), List.of(), null, null);
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.LOGICAL_PLAN);
    }

    @Test
    @DisplayName("WriteQueryIR -> LOGICAL_PLAN")
    void writeLogicalPlan() {
        var ir = new WriteQueryIR(meta, WriteType.INSERT, List.of(), null);
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.LOGICAL_PLAN);
    }

    @Test
    @DisplayName("DDLIR -> LOGICAL_PLAN")
    void ddlLogicalPlan() {
        var ir = new DDLIR(meta, DDLType.CREATE_TABLE, Map.of());
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.LOGICAL_PLAN);
    }

    @Test
    @DisplayName("Placeholder meta -> LOGICAL_PLAN for metadata query")
    void decide_withPlaceholderMeta_shouldReturnLogicalPlan() {
        StructType placeholderSchema =
                new StructType(List.of(new StructField("placeholder", PrimitiveType.STRING)));
        TableMetaIR placeholderMeta =
                new TableMetaIR(
                        "t", 0L, TableType.PRIMARY_KEY, placeholderSchema, List.of(), Map.of(), 0);
        var ir = new MetadataQueryIR.CountAll(placeholderMeta, 1L);
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.LOGICAL_PLAN);
    }

    @Test
    @DisplayName("PointQueryIR with no fast path executors -> LOGICAL_PLAN")
    void decide_withNoFastPathExecutors_shouldReturnLogicalPlan() {
        var ir =
                new PointQueryIR(
                        meta,
                        null,
                        0L,
                        new ReadSnapshot(0L, 0L),
                        Map.of("id", 1L),
                        new byte[] {1},
                        List.of(),
                        "e1",
                        0,
                        0L);
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.LOGICAL_PLAN);
    }

    @Test
    @DisplayName("PointQueryIR with fast path executors -> FAST_PATH")
    void decide_withFastPathExecutors_shouldReturnFastPath() {
        fastPathExecutor.registerExecutor("e1", new StubExecutor());
        var ir =
                new PointQueryIR(
                        meta,
                        null,
                        0L,
                        new ReadSnapshot(0L, 0L),
                        Map.of("id", 1L),
                        new byte[] {1},
                        List.of(),
                        "e1",
                        0,
                        0L);
        assertThat(decider.decide(ir)).isEqualTo(ExecutionPath.FAST_PATH);
    }

    private static class StubExecutor implements FastPathExecutor.ExecutorStub {
        @Override
        public java.util.Optional<byte[]> pointQuery(
                String tableId, byte[] key, long snapshotTs, long shardEpoch) {
            return java.util.Optional.empty();
        }

        @Override
        public List<PointQueryResult> batchPointQuery(
                String tableId, List<byte[]> keys, long snapshotTs, long shardEpoch) {
            return List.of();
        }

        @Override
        public long getShardEpoch(String tableId, int shardId) {
            return 0L;
        }

        @Override
        public FastPathExecutor.PointWriteResult pointUpdate(
                String tableId,
                java.util.Map<String, Comparable<?>> pkValues,
                java.util.Map<String, Comparable<?>> setClauses,
                java.util.List<String> primaryKeyCols,
                long snapshotTs,
                long shardEpoch) {
            return FastPathExecutor.PointWriteResult.notFound();
        }

        @Override
        public FastPathExecutor.PointWriteResult pointDelete(
                String tableId,
                java.util.Map<String, Comparable<?>> pkValues,
                java.util.List<String> primaryKeyCols,
                long snapshotTs,
                long shardEpoch) {
            return FastPathExecutor.PointWriteResult.notFound();
        }
    }
}
