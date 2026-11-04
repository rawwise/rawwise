package io.github.rawwise.frontend.ir.adaptor;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.frontend.ir.*;
import io.github.rawwise.model.expression.*;
import io.github.rawwise.model.record.PrimitiveType;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.StructType;
import java.util.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Offset;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.junit.jupiter.api.*;

@DisplayName("SparkIRAdaptor")
class SparkIRAdaptorTest {

    private static SparkSession spark;
    private static SparkIRAdaptor adaptor;

    @BeforeAll
    static void initSpark() {
        spark =
                SparkSession.builder()
                        .appName("SparkIRAdaptorTest")
                        .master("local[1]")
                        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test-adaptor")
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

    @Test
    @DisplayName(
            "scanQueryProducesLogicalPlan: ScanQueryIR adapt should yield non-null plan with non-empty nodeName")
    void scanQueryProducesLogicalPlan() {
        TableMetaIR meta = testTableMeta();
        ScanQueryIR scan = new ScanQueryIR(meta, 0L, List.of(), null, null, 0, 0, null);

        LogicalPlan plan = adaptor.adapt(scan);

        assertThat(plan).isNotNull();
        assertThat(plan.nodeName()).isNotEmpty();
    }

    @Test
    @DisplayName(
            "metadataQueryThrows: MetadataQueryIR.CountAll adapt should throw UnsupportedOperationException")
    void metadataQueryThrows() {
        TableMetaIR meta = testTableMeta();
        MetadataQueryIR.CountAll mq = new MetadataQueryIR.CountAll(meta, -1L);

        assertThatThrownBy(() -> adaptor.adapt(mq))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("not supported");
    }

    @Test
    @DisplayName("toSparkExpression_unsupportedType_shouldThrowIRAdaptException")
    void toSparkExpression_unsupportedType_shouldThrowIRAdaptException() {
        Expression unsupported = new Like(new ColumnRef("name"), "%test%");

        assertThatThrownBy(() -> adaptor.toSparkExpression(unsupported))
                .isInstanceOf(IRAdaptException.class)
                .hasMessageContaining("Unsupported expression type");
    }

    @Test
    @DisplayName("toSparkLogicalAnd_nullSubExpression_shouldThrowIRAdaptException")
    void toSparkLogicalAnd_nullSubExpression_shouldThrowIRAdaptException() {
        Expression unsupported = new Like(new ColumnRef("name"), "%test%");
        Expression andExpr = new And(new ColumnRef("id"), unsupported);

        assertThatThrownBy(() -> adaptor.toSparkExpression(andExpr))
                .isInstanceOf(IRAdaptException.class);
    }

    @Test
    @DisplayName("toSparkLogicalOr_nullSubExpression_shouldThrowIRAdaptException")
    void toSparkLogicalOr_nullSubExpression_shouldThrowIRAdaptException() {
        Expression unsupported = new Like(new ColumnRef("name"), "%test%");
        Expression orExpr = new Or(new ColumnRef("id"), unsupported);

        assertThatThrownBy(() -> adaptor.toSparkExpression(orExpr))
                .isInstanceOf(IRAdaptException.class);
    }

    @Test
    @DisplayName("applyFilter_nullAdaptation_shouldThrowIRAdaptException")
    void applyFilter_nullAdaptation_shouldThrowIRAdaptException() {
        TableMetaIR meta = testTableMeta();
        Expression unsupported = new Like(new ColumnRef("name"), "%test%");
        ScanQueryIR scan = new ScanQueryIR(meta, 0L, List.of(), unsupported, null, 0, 0, null);

        assertThatThrownBy(() -> adaptor.adapt(scan))
                .isInstanceOf(IRAdaptException.class)
                .hasMessageContaining("Unsupported expression type");
    }

    @Test
    @DisplayName("applyOffset_withPositiveOffset_shouldAddOffsetNode")
    void applyOffset_withPositiveOffset_shouldAddOffsetNode() {
        TableMetaIR meta = testTableMeta();
        ScanQueryIR scan = new ScanQueryIR(meta, 0L, List.of(), null, null, 0, 10, null);

        LogicalPlan plan = adaptor.adapt(scan);

        assertThat(plan).isInstanceOf(Offset.class);
        Offset offset = (Offset) plan;
        assertThat(offset.offsetExpr()).isNotNull();
    }

    @Test
    @DisplayName("applyOffset_withZeroOffset_shouldReturnOriginalPlan")
    void applyOffset_withZeroOffset_shouldReturnOriginalPlan() {
        TableMetaIR meta = testTableMeta();
        ScanQueryIR scan = new ScanQueryIR(meta, 0L, List.of(), null, null, 0, 0, null);

        LogicalPlan plan = adaptor.adapt(scan);

        assertThat(plan).isNotInstanceOf(Offset.class);
    }

    @Test
    @DisplayName("applyProjections_withAlias_shouldPreserveAlias")
    void applyProjections_withAlias_shouldPreserveAlias() {
        TableMetaIR meta = testTableMeta();
        ProjectionColumn aliasedCol = new ProjectionColumn("id", 0, PrimitiveType.LONG, "user_id");
        ScanQueryIR scan = new ScanQueryIR(meta, 0L, List.of(aliasedCol), null, null, 0, 0, null);

        LogicalPlan plan = adaptor.adapt(scan);

        assertThat(plan).isInstanceOf(Project.class);
        Project project = (Project) plan;
        var projectList = scala.collection.JavaConverters.seqAsJavaList(project.projectList());
        assertThat(projectList).hasSize(1);
        assertThat(projectList.get(0)).isInstanceOf(Alias.class);
        Alias alias = (Alias) projectList.get(0);
        assertThat(alias.name()).isEqualTo("user_id");
    }
}
