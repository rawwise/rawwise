package io.github.rawwise.frontend.ir.adaptor;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.frontend.ir.*;
import io.github.rawwise.model.record.PrimitiveType;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.StructType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;

@DisplayName("Multi-Engine Adaptor")
class MultiEngineAdaptorTest {

    private TableMetaIR meta;

    @BeforeEach
    void setUp() {
        StructType schema =
                new StructType(
                        List.of(
                                new StructField("id", PrimitiveType.LONG),
                                new StructField("name", PrimitiveType.STRING)));
        meta =
                new TableMetaIR(
                        "test.t", 1L, TableType.PRIMARY_KEY, schema, List.of("id"), Map.of(), 1);
    }

    @Test
    @DisplayName("IRAdaptorRegistry resolves spark, trino, duckdb")
    void registryResolvesAllEngines() {
        assertThat(IRAdaptorRegistry.create("spark")).isInstanceOf(SparkIRAdaptor.class);
        assertThat(IRAdaptorRegistry.create("trino")).isInstanceOf(TrinoIRAdaptor.class);
        assertThat(IRAdaptorRegistry.create("duckdb")).isInstanceOf(DuckDBIRAdaptor.class);
    }

    @Test
    @DisplayName("TrinoIRAdaptor handles ScanQueryIR")
    @SuppressWarnings("unchecked")
    void trinoAdaptorHandlesScan() {
        IRAdaptor<Object> adaptor = IRAdaptorRegistry.create("trino");
        ScanQueryIR sq = new ScanQueryIR(meta, 1L, List.of(), null, List.of(), 10, 0, null);
        Object result = adaptor.adapt(sq);
        assertThat(result).isNotNull();
        assertThat(result.toString()).contains("test.t");
    }

    @Test
    @DisplayName("DuckDBIRAdaptor produces SQL for ScanQueryIR")
    @SuppressWarnings("unchecked")
    void duckDBAdaptorProducesSqlForScan() {
        IRAdaptor<String> adaptor = IRAdaptorRegistry.create("duckdb");
        ScanQueryIR sq = new ScanQueryIR(meta, 1L, List.of(), null, List.of(), 10, 0, null);
        String sql = adaptor.adapt(sq);
        assertThat(sql).contains("SELECT * FROM test.t");
        assertThat(sql).contains("LIMIT 10");
    }

    @Test
    @DisplayName("TrinoIRAdaptor throws for MetadataQueryIR")
    @SuppressWarnings("unchecked")
    void trinoAdaptorThrowsForMetadata() {
        IRAdaptor<Object> adaptor = IRAdaptorRegistry.create("trino");
        MetadataQueryIR.CountAll mq = new MetadataQueryIR.CountAll(meta, 1L);
        assertThatThrownBy(() -> adaptor.adapt(mq))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("Unknown engine throws IllegalArgumentException")
    void unknownEngineThrows() {
        assertThatThrownBy(() -> IRAdaptorRegistry.create("unknown"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No IR adaptor registered");
    }
}
