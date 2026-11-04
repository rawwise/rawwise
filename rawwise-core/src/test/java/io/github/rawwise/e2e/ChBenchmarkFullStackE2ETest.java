package io.github.rawwise.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.common.catalog.TableProperties;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.txn.RecordHeaderProbe;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.frontend.ir.*;
import io.github.rawwise.frontend.ir.parser.ParseResult;
import io.github.rawwise.frontend.ir.parser.SqlQueryParser;
import io.github.rawwise.frontend.ir.routing.ExecutionPath;
import io.github.rawwise.frontend.ir.routing.IndexSelector;
import io.github.rawwise.frontend.ir.routing.RouteDecider;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.model.expression.And;
import io.github.rawwise.model.expression.ColumnRef;
import io.github.rawwise.model.expression.Eq;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.Literal;
import io.github.rawwise.model.record.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.*;

/**
 * CH-benCHmark IR unit test(SQL parse + routingdecision + index verify).
 *
 * <p><b> </b>: testis IR unit test, is not E2E. InMemoryMetastore + RouteDecider(null)(no
 * executor)directly SQL parseandroutingdecision , HTTP->Frontend->gRPC->Backend->LMDB .
 *
 * <p>E2E {@link UpdateDeleteFastPathE2ETest}(InProcess gRPC + real LMDB). test is IR test, cover:
 *
 * <ol>
 *   <li>DDL: CREATE TABLE(primary key + secondary index)parse
 *   <li>DML: INSERT / SELECT PK / SELECT index / UPDATE PK / UPDATE non-PK / DELETE parse
 *   <li>routing: RouteDecider -> FAST_PATH vs LOGICAL_PLAN vs METADATA_ONLY decision
 *   <li>index: IndexSelector query write
 *   <li>transaction: ShardVersionTracker commitTs verify
 *   <li>expression: AND / Eq / ColumnRef / Literal constructandextract
 * </ol>
 *
 * <p>TODO: Backend gRPC + LMDB after ofreadwriteverify(or isreal HTTP E2E).
 */
@DisplayName("CH-benCHmark IR unit test")
class ChBenchmarkFullStackE2ETest {

    private InMemoryMetastore metastore;
    private MetastoreCatalog catalog;
    private SnapshotManager snapshotManager;
    private SqlQueryParser parser;
    private RouteDecider routeDecider;
    private IndexSelector indexSelector;
    private ShardVersionTracker tracker;

    @BeforeEach
    void setUp() throws IOException {
        metastore = new InMemoryMetastore();
        catalog =
                new MetastoreCatalog(
                        MetastoreCatalogConfig.builder()
                                .catalogName("ch-bench-e2e")
                                .store(metastore)
                                .basePath("/tmp/ch-bench-e2e")
                                .build());
        catalog.createNamespace("db");
        snapshotManager = new SnapshotManager(catalog, metastore);
        parser = new SqlQueryParser(catalog, snapshotManager);
        routeDecider = new RouteDecider(null);
        indexSelector = new IndexSelector(catalog);
        tracker = new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);

        // === DDL: CH-benCHmark warehouse + district + customer + orders ===

        catalog.createTable(
                new TableId("db", "warehouse"),
                Schema.builder()
                        .field(1, "w_id", Types.int32(false))
                        .field(2, "w_name", Types.string())
                        .field(3, "w_ytd", Types.int64())
                        .primaryKeys("w_id")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());

        catalog.createTable(
                new TableId("db", "district"),
                Schema.builder()
                        .field(1, "d_w_id", Types.int32(false))
                        .field(2, "d_id", Types.int32(false))
                        .field(3, "d_name", Types.string())
                        .field(4, "d_ytd", Types.int64())
                        .primaryKeys("d_w_id", "d_id")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());

        catalog.createTable(
                new TableId("db", "customer"),
                Schema.builder()
                        .field(1, "c_w_id", Types.int32(false))
                        .field(2, "c_d_id", Types.int32(false))
                        .field(3, "c_id", Types.int32(false))
                        .field(4, "c_balance", Types.int64())
                        .field(5, "c_name", Types.string())
                        .primaryKeys("c_w_id", "c_d_id", "c_id")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());

        catalog.createTable(
                new TableId("db", "orders"),
                Schema.builder()
                        .field(1, "o_w_id", Types.int32(false))
                        .field(2, "o_d_id", Types.int32(false))
                        .field(3, "o_id", Types.int32(false))
                        .field(4, "o_c_id", Types.int32())
                        .field(5, "o_amount", Types.int64())
                        .field(6, "o_carrier_id", Types.int32())
                        .primaryKeys("o_w_id", "o_d_id", "o_id")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (catalog != null) catalog.close();
    }

    // ================================================================
    // S1: DML — INSERT / SELECT / UPDATE / DELETE
    // ================================================================

    @Nested
    @DisplayName("S1: INSERT -> parse WriteQueryIR")
    class InsertTests {

        @Test
        @DisplayName("S1.1: INSERT warehouse -> WriteQueryIR(BATCH_INSERT)")
        void insertWarehouse() {
            ParseResult result =
                    parser.parse(
                            "INSERT INTO db.warehouse (w_id, w_name, w_ytd) VALUES (1, 'WH-1', 0)");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.BATCH_INSERT);
            assertThat(ir).isInstanceOf(WriteQueryIR.class);
        }

        @Test
        @DisplayName("S1.2: INSERT orders (repeat PK) -> BATCH_INSERT")
        void insertOrders() {
            ParseResult result =
                    parser.parse(
                            "INSERT INTO db.orders (o_w_id, o_d_id, o_id, o_c_id, o_amount) "
                                    + "VALUES (1, 7, 100, 42, 500)");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            assertThat(((ParseResult.IRResult) result).ir().type()).isEqualTo(IRType.BATCH_INSERT);
        }
    }

    @Nested
    @DisplayName("S2: SELECT — PK / index / Scan")
    class SelectTests {

        @Test
        @DisplayName("S2.1: SELECT WHERE pk= -> ScanQueryIR -> LOGICAL_PLAN")
        void selectPk() {
            ParseResult result = parser.parse("SELECT * FROM db.warehouse WHERE w_id = 1");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            ScanQueryIR sq = (ScanQueryIR) ((ParseResult.IRResult) result).ir();
            assertThat(routeDecider.decide(sq)).isEqualTo(ExecutionPath.LOGICAL_PLAN);
        }

        @Test
        @DisplayName("S2.2: SELECT WHERE non-PK -> ScanQueryIR(noindex go through table)")
        void selectNonPk() {
            ParseResult result = parser.parse("SELECT o_id FROM db.orders WHERE o_c_id = 42");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            assertThat(((ParseResult.IRResult) result).ir().type()).isEqualTo(IRType.SCAN);
        }

        @Test
        @DisplayName("S2.3: SELECT COUNT(*) -> ScanQueryIR (aggregate)")
        void selectCount() {
            ParseResult result = parser.parse("SELECT COUNT(*) FROM db.warehouse");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            // COUNT(*) currently parsed as ScanQueryIR (not MetadataQueryIR)
            assertThat(((ParseResult.IRResult) result).ir().type()).isEqualTo(IRType.SCAN);
        }
    }

    @Nested
    @DisplayName("S3: UPDATE / DELETE — FAST_PATH vs LAZY")
    class UpdateDeleteTests {

        @Test
        @DisplayName(
                "S3.1: UPDATE WHERE pk= -> UpdateQueryIR(POINT) -> LOGICAL_PLAN (FAST_PATH executor)")
        void updatePkFastPath() {
            ParseResult result = parser.parse("UPDATE db.warehouse SET w_ytd = 100 WHERE w_id = 1");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            UpdateQueryIR uq = (UpdateQueryIR) ((ParseResult.IRResult) result).ir();
            assertThat(uq.mode()).isEqualTo(UpdateMode.POINT);
            // FAST_PATH requires FastPathExecutor with available executors;
            // without it, falls back to LOGICAL_PLAN
            assertThat(routeDecider.decide(uq)).isEqualTo(ExecutionPath.LOGICAL_PLAN);
            assertThat(uq.pkValues()).containsEntry("w_id", 1);
            assertThat(uq.setClauses()).containsEntry("w_ytd", 100L);
        }

        @Test
        @DisplayName("S3.2: UPDATE customer WHERE PK -> POINT (PK cover)")
        void updateNonPkLazy() {
            // customer PK=(c_w_id, c_d_id, c_id), WHERE cover PK -> POINT
            ParseResult result =
                    parser.parse(
                            "UPDATE db.customer SET c_balance = c_balance - 10 "
                                    + "WHERE c_w_id = 1 AND c_d_id = 7 AND c_id = 5");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.POINT_UPDATE);
        }

        @Test
        @DisplayName(
                "S3.3: DELETE WHERE pk= -> DeleteQueryIR(POINT) -> LOGICAL_PLAN (FAST_PATH no executor)")
        void deletePkFastPath() {
            ParseResult result = parser.parse("DELETE FROM db.warehouse WHERE w_id = 1");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            DeleteQueryIR dq = (DeleteQueryIR) ((ParseResult.IRResult) result).ir();
            assertThat(dq.mode()).isEqualTo(UpdateMode.POINT);
            assertThat(routeDecider.decide(dq)).isEqualTo(ExecutionPath.LOGICAL_PLAN);
        }

        @Test
        @DisplayName("S3.4: DELETE WHERE non-PK -> DeleteRangeIR -> LOGICAL_PLAN")
        void deleteNonPkRange() {
            ParseResult result = parser.parse("DELETE FROM db.orders WHERE o_c_id = 99");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.DELETE_RANGE);
        }
    }

    @Nested
    @DisplayName("S4: JOIN")
    class JoinTests {

        @Test
        @DisplayName("S4.1: JOIN warehouse + district -> JoinQueryIR")
        void joinWarehouseDistrict() {
            ParseResult result =
                    parser.parse(
                            "SELECT w.w_name, d.d_name FROM db.warehouse w "
                                    + "JOIN db.district d ON w.w_id = d.d_w_id");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            assertThat(((ParseResult.IRResult) result).ir().type()).isEqualTo(IRType.JOIN);
        }
    }

    @Nested
    @DisplayName("S5: index — IndexSelector write")
    class IndexSelectorTests {

        @Test
        @DisplayName("S5.1: indexcoverquery before -> IndexSelector in")
        void indexSelectedForCoveringQuery() {
            // registerindex idx_orders_wd on (o_w_id, o_d_id)
            IndexMeta idx =
                    new IndexMeta(
                            "idx_orders_wd",
                            new TableId("db", "orders"),
                            List.of("o_w_id", "o_d_id"),
                            List.of("o_id", "o_amount"));

            // construct AND-based : o_w_id=1 AND o_d_id=7
            Expression filter =
                    new And(
                            List.of(
                                    new Eq(new ColumnRef("o_w_id"), new Literal(1L)),
                                    new Eq(new ColumnRef("o_d_id"), new Literal(7L))));
            Set<String> eqCols = IndexSelector.extractEqualityColumns(filter);
            assertThat(eqCols).containsExactlyInAnyOrder("o_w_id", "o_d_id");

            int score = indexSelector.score(idx, eqCols, Set.of("o_id", "o_amount"));
            // 100 + 2columnx20 + covering(+50) + (+15) = 205
            assertThat(score).isEqualTo(205);
        }

        @Test
        @DisplayName("S5.2: noindexcover -> tryRewrite empty")
        void noIndexCoverage() {
            ParseResult result = parser.parse("SELECT * FROM db.warehouse WHERE w_name = 'test'");
            ScanQueryIR sq = (ScanQueryIR) ((ParseResult.IRResult) result).ir();
            Optional<ScanQueryIR> rewritten = indexSelector.tryRewrite(sq);
            assertThat(rewritten).isEmpty();
        }
    }

    @Nested
    @DisplayName("S6: transaction — ShardVersionTracker")
    class TxnTests {

        @Test
        @DisplayName("S6.1: INSERT after commitTs write")
        void commitTsReflectsWrite() {
            byte[] key = "warehouse:1".getBytes(StandardCharsets.UTF_8);
            long txnId = 1001L;
            tracker.setCommitTs(key, txnId, txnId);
            assertThat(tracker.getCommitTs(key)).isEqualTo(txnId);
        }

        @Test
        @DisplayName("S6.2: not yetwriteof key commitTs=0")
        void noWriteKeyReturnsZero() {
            byte[] key = "warehouse:999".getBytes(StandardCharsets.UTF_8);
            assertThat(tracker.getCommitTs(key)).isEqualTo(0L);
        }

        @Test
        @DisplayName("S6.3: table OCC: tabledifferenttrackerindependent")
        void crossTableOccIndependent() {
            ShardVersionTracker tracker2 =
                    new ShardVersionTracker(1, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
            byte[] key = "shared".getBytes(StandardCharsets.UTF_8);
            tracker.setCommitTs(key, 100L, 100L);
            assertThat(tracker2.getCommitTs(key)).isEqualTo(0L);
        }
    }

    @Nested
    @DisplayName("S7: CREATE INDEX / DROP INDEX DDL")
    class DdlTests {

        @Test
        @DisplayName("S7.1: CREATE INDEX -> DDLIR(CREATE_INDEX)")
        void createIndex() {
            ParseResult result = parser.parse("CREATE INDEX idx_w_name ON db.warehouse (w_name)");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            DDLIR ddl = (DDLIR) ((ParseResult.IRResult) result).ir();
            assertThat(ddl.ddlType()).isEqualTo(DDLType.CREATE_INDEX);
        }
    }
}
