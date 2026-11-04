package io.github.rawwise.frontend.ir.adaptor;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.frontend.ir.*;
import io.github.rawwise.model.record.PrimitiveType;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.StructType;
import java.util.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.*;

@DisplayName("SparkIRAdaptor Write/Update/Delete/DDL")
class SparkIRAdaptorWriteTest {

    private static SparkSession spark;
    private static SparkIRAdaptor adaptor;

    @BeforeAll
    static void initSpark() {
        spark =
                SparkSession.builder()
                        .appName("SparkIRAdaptorWriteTest")
                        .master("local[1]")
                        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-adaptor-write")
                        .getOrCreate();
        adaptor = new SparkIRAdaptor();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static TableMetaIR testTableMeta() {
        StructType schema =
                new StructType(
                        List.of(
                                new StructField("id", PrimitiveType.LONG),
                                new StructField("name", PrimitiveType.STRING)));
        return new TableMetaIR(
                "test_db.test_table",
                1L,
                TableType.PRIMARY_KEY,
                schema,
                List.of("id"),
                Map.of(),
                1);
    }

    // ========================================================================
    // WriteQueryIR
    // ========================================================================

    @Test
    @DisplayName(
            "writeInsertProducesLogicalPlan: INSERT adapt yields non-null plan with non-empty nodeName")
    void writeInsertProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        List<Map<String, Comparable<?>>> rows =
                List.of(Map.of("id", 1L, "name", (Comparable<?>) "alice"));
        WriteQueryIR wq = new WriteQueryIR(meta, WriteType.INSERT, rows, null);

        LogicalPlan plan = adaptor.adapt(wq);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }

    @Test
    @DisplayName("writeBatchInsertProducesLogicalPlan: BATCH_INSERT adapt yields non-null plan")
    void writeBatchInsertProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        List<Map<String, Comparable<?>>> rows =
                List.of(
                        Map.of("id", 1L, "name", (Comparable<?>) "alice"),
                        Map.of("id", 2L, "name", (Comparable<?>) "bob"));
        WriteQueryIR wq = new WriteQueryIR(meta, WriteType.BATCH_INSERT, rows, null);

        LogicalPlan plan = adaptor.adapt(wq);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }

    @Test
    @DisplayName("writeInsertSelectProducesLogicalPlan: INSERT_SELECT adapt yields non-null plan")
    void writeInsertSelectProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        // INSERT_SELECT: insert from a scan sub-query
        ScanQueryIR scan = new ScanQueryIR(meta, 0L, List.of(), null, null, 0, 0, null);
        WriteQueryIR wq = new WriteQueryIR(meta, WriteType.INSERT_SELECT, List.of(), scan);

        LogicalPlan plan = adaptor.adapt(wq);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }

    // ========================================================================
    // UpdateRangeIR
    // ========================================================================

    @Test
    @DisplayName(
            "updateRangeNoFilterProducesLogicalPlan: UPDATE_RANGE without filter yields relation plan")
    void updateRangeNoFilterProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        UpdateRangeIR ur = new UpdateRangeIR(meta, Map.of("name", (Comparable<?>) "bob"), null);

        LogicalPlan plan = adaptor.adapt(ur);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }

    // ========================================================================
    // DeleteRangeIR
    // ========================================================================

    @Test
    @DisplayName(
            "deleteRangeNoFilterProducesLogicalPlan: DELETE_RANGE without filter yields relation plan")
    void deleteRangeNoFilterProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        DeleteRangeIR dr = new DeleteRangeIR(meta, null);

        LogicalPlan plan = adaptor.adapt(dr);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }

    // ========================================================================
    // DDLIR
    // ========================================================================

    @Test
    @DisplayName("ddlCreateTableProducesLogicalPlan: CREATE_TABLE adapt yields non-null plan")
    void ddlCreateTableProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        DDLIR dd = new DDLIR(meta, DDLType.CREATE_TABLE, Map.of());

        LogicalPlan plan = adaptor.adapt(dd);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }

    @Test
    @DisplayName("ddlDropTableProducesLogicalPlan: DROP_TABLE adapt yields non-null plan")
    void ddlDropTableProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        DDLIR dd = new DDLIR(meta, DDLType.DROP_TABLE, Map.of());

        LogicalPlan plan = adaptor.adapt(dd);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }

    @Test
    @DisplayName("ddlAlterTableProducesLogicalPlan: ALTER_TABLE adapt yields non-null plan")
    void ddlAlterTableProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        DDLIR dd = new DDLIR(meta, DDLType.ALTER_TABLE, Map.of());

        LogicalPlan plan = adaptor.adapt(dd);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }

    @Test
    @DisplayName("ddlTruncateProducesLogicalPlan: TRUNCATE adapt yields non-null plan")
    void ddlTruncateProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        DDLIR dd = new DDLIR(meta, DDLType.TRUNCATE, Map.of());

        LogicalPlan plan = adaptor.adapt(dd);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }

    @Test
    @DisplayName("ddlRefreshProducesLogicalPlan: REFRESH adapt yields non-null plan")
    void ddlRefreshProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        DDLIR dd = new DDLIR(meta, DDLType.REFRESH, Map.of());

        LogicalPlan plan = adaptor.adapt(dd);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }
}
