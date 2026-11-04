package io.github.rawwise.frontend.ir.parser;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.common.catalog.TableProperties;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.frontend.ir.DeleteQueryIR;
import io.github.rawwise.frontend.ir.DeleteRangeIR;
import io.github.rawwise.frontend.ir.IRType;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.frontend.ir.UpdateMode;
import io.github.rawwise.frontend.ir.UpdateQueryIR;
import io.github.rawwise.frontend.ir.UpdateRangeIR;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.model.record.PartitionSpec;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.TableId;
import io.github.rawwise.model.record.Types;
import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Spec 01 Phase 2 E2E: non-primary key UPDATE/DELETE -> UpdateRangeIR/DeleteRangeIR (LOGICAL_PLAN
 * -> LazyUpdate pipeline).
 *
 * <p>coverage scenario:
 *
 * <ul>
 *   <li>S1 Happy Path: non- PK etcvalue UPDATE/DELETE produce UpdateRangeIR/DeleteRangeIR(non-
 *       UpdateQueryIR)
 *   <li>S2 no WHERE: UPDATE table SET col=v produce rangeFilter=null
 *   <li>S3 scenario: sametabledifferent SQL, PK go through POINT, non- PK go through RANGE
 *   <li>S4 tableno PK: all UPDATE/DELETE allgo through RANGE
 *   <li>S5 back-compat: Phase 1 PK etcvalue produce UpdateQueryIR(POINT)
 * </ul>
 */
@DisplayName("LazyUpdate IR Parser E2E — Spec 01 Phase 2")
class LazyUpdateParserE2ETest {

    private InMemoryMetastore metastore;
    private MetastoreCatalog catalog;
    private SnapshotManager snapshotManager;
    private SqlQueryParser parser;

    @BeforeEach
    void setUp() throws IOException {
        metastore = new InMemoryMetastore();
        catalog =
                new MetastoreCatalog(
                        MetastoreCatalogConfig.builder()
                                .catalogName("lazy-update-e2e")
                                .store(metastore)
                                .basePath("/tmp/lazy-update-e2e")
                                .build());
        catalog.createNamespace("db");
        snapshotManager = new SnapshotManager(catalog, metastore);
        parser = new SqlQueryParser(catalog, snapshotManager);

        catalog.createTable(
                new TableId("db", "customer"),
                Schema.builder()
                        .field(1, "c_id", Types.int32(false))
                        .field(2, "c_w_id", Types.int32())
                        .field(3, "c_d_id", Types.int32())
                        .field(4, "c_balance", Types.int64())
                        .field(5, "c_name", Types.string())
                        .primaryKeys("c_id")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());

        catalog.createTable(
                new TableId("db", "nopk"),
                Schema.builder()
                        .field(1, "col_a", Types.int32())
                        .field(2, "col_b", Types.string())
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (catalog != null) catalog.close();
    }

    @Nested
    @DisplayName("S1: non-PKetcvalue UPDATE -> UpdateRangeIR")
    class NonPkUpdateTests {

        @Test
        @DisplayName("S1.1: UPDATE ... WHERE c_w_id=1 AND c_d_id=7 -> UpdateRangeIR (non-PK)")
        void nonPkUpdateProducesUpdateRangeIR() {
            ParseResult result =
                    parser.parse(
                            "UPDATE db.customer SET c_balance = 500 "
                                    + "WHERE c_w_id = 1 AND c_d_id = 7");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.UPDATE_RANGE);
            assertThat(ir).isInstanceOf(UpdateRangeIR.class);

            UpdateRangeIR uir = (UpdateRangeIR) ir;
            assertThat(uir.setClauses()).containsEntry("c_balance", 500L);
            assertThat(uir.rangeFilter()).isNotNull();
        }

        @Test
        @DisplayName("S1.2: DELETE ... WHERE c_w_id=1 AND c_d_id=7 -> DeleteRangeIR (non-PK)")
        void nonPkDeleteProducesDeleteRangeIR() {
            ParseResult result =
                    parser.parse("DELETE FROM db.customer WHERE c_w_id = 1 AND c_d_id = 7");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.DELETE_RANGE);
            assertThat(ir).isInstanceOf(DeleteRangeIR.class);

            DeleteRangeIR dir = (DeleteRangeIR) ir;
            assertThat(dir.rangeFilter()).isNotNull();
        }
    }

    @Nested
    @DisplayName("S2: no WHERE -> UpdateRangeIR(no filter)")
    class NoWhereTests {

        @Test
        @DisplayName("S2.1: UPDATE table SET col=v(no WHERE)-> rangeFilter=null")
        void updateWithoutWhereNullFilter() {
            ParseResult result = parser.parse("UPDATE db.customer SET c_balance = 0");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.UPDATE_RANGE);
            assertThat(((UpdateRangeIR) ir).rangeFilter()).isNull();
        }
    }

    @Nested
    @DisplayName("S3: scenario — PK go through POINT, non- PK go through RANGE")
    class MixedPathTests {

        @Test
        @DisplayName("S3.1: UPDATE ... WHERE c_id=1 -> UpdateQueryIR(POINT)")
        void pkUpdateStillPoint() {
            ParseResult result =
                    parser.parse("UPDATE db.customer SET c_balance = 999 WHERE c_id = 1");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.POINT_UPDATE);
            assertThat(ir).isInstanceOf(UpdateQueryIR.class);
            UpdateQueryIR uir = (UpdateQueryIR) ir;
            assertThat(uir.mode()).isEqualTo(UpdateMode.POINT);
            assertThat(uir.pkValues()).containsEntry("c_id", 1);
        }

        @Test
        @DisplayName("S3.2: DELETE ... WHERE c_id=1 -> DeleteQueryIR(POINT)")
        void pkDeleteStillPoint() {
            ParseResult result = parser.parse("DELETE FROM db.customer WHERE c_id = 1");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.POINT_DELETE);
            assertThat(ir).isInstanceOf(DeleteQueryIR.class);
        }

        @Test
        @DisplayName("S3.3: compound PK covered -> still POINT mode")
        void compoundPkFullyCoveredStillPoint() {
            // createrepeat PK table
            catalog.createTable(
                    new TableId("db", "district"),
                    Schema.builder()
                            .field(1, "d_w_id", Types.int32(false))
                            .field(2, "d_id", Types.int32(false))
                            .field(3, "d_name", Types.string())
                            .primaryKeys("d_w_id", "d_id")
                            .build(),
                    PartitionSpec.unpartitioned(),
                    TableProperties.empty());

            ParseResult result =
                    parser.parse(
                            "UPDATE db.district SET d_name = 'X' "
                                    + "WHERE d_w_id = 1 AND d_id = 5");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.POINT_UPDATE);

            UpdateQueryIR uir = (UpdateQueryIR) ir;
            assertThat(uir.mode()).isEqualTo(UpdateMode.POINT);
            assertThat(uir.pkValues()).containsEntry("d_w_id", 1);
            assertThat(uir.pkValues()).containsEntry("d_id", 5);
        }

        @Test
        @DisplayName("S3.4: repeat PK cover 1 column -> RANGE(non- PK etcvalue)")
        void compoundPkPartialCoveredGoesRange() {
            catalog.createTable(
                    new TableId("db", "warehouse"),
                    Schema.builder()
                            .field(1, "w_id", Types.int32(false))
                            .field(2, "w_tax", Types.float64())
                            .primaryKeys("w_id")
                            .build(),
                    PartitionSpec.unpartitioned(),
                    TableProperties.empty());

            ParseResult result =
                    parser.parse("UPDATE db.warehouse SET w_tax = 0.05 WHERE w_id = 1");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.POINT_UPDATE);
        }
    }

    @Nested
    @DisplayName("S4: tableno PK — all UPDATE/DELETE go through RANGE")
    class NoPkTableTests {

        @Test
        @DisplayName("S4.1: no PK tableof UPDATE -> UpdateRangeIR")
        void noPkTableUpdateGoesRange() {
            ParseResult result =
                    parser.parse("UPDATE db.nopk SET col_b = 'updated' WHERE col_a = 42");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.UPDATE_RANGE);
            assertThat(ir).isInstanceOf(UpdateRangeIR.class);
        }

        @Test
        @DisplayName("S4.2: no PK tableof DELETE -> DeleteRangeIR")
        void noPkTableDeleteGoesRange() {
            ParseResult result = parser.parse("DELETE FROM db.nopk WHERE col_a = 42");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.DELETE_RANGE);
            assertThat(ir).isInstanceOf(DeleteRangeIR.class);
        }
    }
}
