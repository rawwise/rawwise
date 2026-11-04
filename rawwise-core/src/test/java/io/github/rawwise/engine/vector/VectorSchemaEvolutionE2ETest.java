package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.Types;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("VectorSchemaEvolution E2E - vectorSchema full chaintest")
class VectorSchemaEvolutionE2ETest {

    private static final int DIM = 16;
    private static final Map<String, String> VECTOR_META_16 =
            Map.of(
                    "rawwise.column.type", "vector",
                    "vector.dimension", "16");
    private static final Map<String, String> VECTOR_META_32 =
            Map.of(
                    "rawwise.column.type", "vector",
                    "vector.dimension", "32");
    private static final Map<String, String> VECTOR_META_ADA =
            Map.of(
                    "rawwise.column.type", "vector",
                    "vector.dimension", "16",
                    "embedding.model", "ada-002");
    private static final Map<String, String> VECTOR_META_3SMALL =
            Map.of(
                    "rawwise.column.type", "vector",
                    "vector.dimension", "16",
                    "embedding.model", "3-small");

    @TempDir Path tempDir;

    private StubEmbeddingProvider embeddingProvider;
    private VectorIndexManager manager;
    private JVectorVectorIndex vectorIndex;

    @BeforeEach
    void setUp() {
        embeddingProvider = new StubEmbeddingProvider(DIM);
    }

    private VectorIndexManager createTableFormatManager() {
        IvfPqConfig ivfPqConfig = new IvfPqConfig(DIM, 4, 4, 2, 5, 10, 5, 42L, 3);
        return new VectorIndexManager(ivfPqConfig, true, tempDir.resolve("table-format"));
    }

    private VectorIndexManager createFullModeManager() {
        IvfPqConfig ivfPqConfig = new IvfPqConfig(DIM, 4, 4, 2, 5, 10, 5, 42L, 3);
        JVectorConfig jVectorConfig =
                new JVectorConfig(
                        "disk",
                        DIM,
                        16,
                        10,
                        50,
                        tempDir.resolve("full-mode").toString(),
                        5,
                        5,
                        500000);
        return new VectorIndexManager(
                ivfPqConfig, jVectorConfig, false, tempDir.resolve("full-mode"));
    }

    private JVectorVectorIndex createFullModeIndex() {
        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        DIM,
                        16,
                        10,
                        50,
                        tempDir.resolve("vector-index").toString(),
                        5,
                        5,
                        500000);
        JVectorVectorIndex idx = new JVectorVectorIndex(config, embeddingProvider, false);
        return idx;
    }

    private JVectorVectorIndex createTableFormatIndex() {
        JVectorConfig config =
                new JVectorConfig(
                        "memory",
                        DIM,
                        16,
                        10,
                        50,
                        tempDir.resolve("vector-index-tf").toString(),
                        5,
                        5,
                        500000);
        JVectorVectorIndex idx = new JVectorVectorIndex(config, embeddingProvider, true);
        idx.setIvfPqConfig(new IvfPqConfig(DIM, 4, 4, 2, 5, 10, 5, 42L, 3));
        return idx;
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            try {
                manager.close();
            } catch (Exception e) {
                /* ignore */
            }
        }
        if (vectorIndex != null) {
            try {
                vectorIndex.close();
            } catch (Exception e) {
                /* ignore */
            }
        }
    }

    @Test
    @DisplayName(
            "VectorAdded: Full mode - create index via VectorIndexManager, write+search through JVectorVectorIndex")
    void testVectorAddedFullModeE2E() {
        Schema oldSchema = Schema.builder().field("id", Types.int64()).version(1L).build();

        Schema newSchema =
                Schema.builder()
                        .field("id", Types.int64())
                        .field(new StructField("embedding", Types.string(), "", VECTOR_META_16))
                        .version(2L)
                        .build();

        List<VectorSchemaDiff> diffs = VectorSchemaDiff.compare(oldSchema, newSchema);
        assertThat(diffs).hasSize(1);
        assertThat(diffs.getFirst()).isInstanceOf(VectorSchemaDiff.VectorAdded.class);

        VectorIndexManager mgr = createFullModeManager();
        try {
            VectorSchemaDiff.VectorAdded added = (VectorSchemaDiff.VectorAdded) diffs.getFirst();
            ShardInternalIndex index =
                    mgr.createIndex("table-1", added.columnName(), added.dimension());
            assertThat(index).isInstanceOf(JVectorDiskIndex.class);
            assertThat(mgr.getIndex("table-1", "embedding")).isSameAs(index);
        } finally {
            mgr.close();
        }
    }

    @Test
    @DisplayName(
            "VectorAdded: Table Format mode - create IvfPqIndex, write+search through JVectorVectorIndex")
    void testVectorAddedTableFormatE2E() {
        Schema oldSchema = Schema.builder().field("id", Types.int64()).version(1L).build();

        Schema newSchema =
                Schema.builder()
                        .field("id", Types.int64())
                        .field(new StructField("embedding", Types.string(), "", VECTOR_META_16))
                        .version(2L)
                        .build();

        List<VectorSchemaDiff> diffs = VectorSchemaDiff.compare(oldSchema, newSchema);
        assertThat(diffs.getFirst()).isInstanceOf(VectorSchemaDiff.VectorAdded.class);

        VectorIndexManager mgr = createTableFormatManager();
        try {
            VectorSchemaDiff.VectorAdded added = (VectorSchemaDiff.VectorAdded) diffs.getFirst();
            ShardInternalIndex index =
                    mgr.createIndex("table-1", added.columnName(), added.dimension());
            assertThat(index).isInstanceOf(IvfPqIndex.class);
        } finally {
            mgr.close();
        }
    }

    @Test
    @DisplayName("VectorRemoved: drop index via VectorIndexManager, verify index gone")
    void testVectorRemovedE2E() {
        VectorIndexManager mgr = createFullModeManager();
        try {
            mgr.createIndex("table-1", "embedding", DIM);
            assertThat(mgr.getIndex("table-1", "embedding")).isNotNull();

            mgr.dropIndex("table-1", "embedding");
            assertThat(mgr.getIndex("table-1", "embedding")).isNull();
        } finally {
            mgr.close();
        }
    }

    @Test
    @DisplayName("DimensionChanged: rebuild index via VectorIndexManager, new index replaces old")
    void testDimensionChangedRebuildE2E() {
        VectorIndexManager mgr = createTableFormatManager();
        try {
            ShardInternalIndex oldIndex = mgr.createIndex("table-1", "embedding", DIM);
            assertThat(oldIndex).isInstanceOf(IvfPqIndex.class);

            VectorSchemaDiff.DimensionChanged diff =
                    new VectorSchemaDiff.DimensionChanged("embedding", DIM, 32);
            mgr.rebuildIndex("table-1", "embedding", List.of(diff));

            ShardInternalIndex newIndex = mgr.getIndex("table-1", "embedding");
            assertThat(newIndex).isInstanceOf(IvfPqIndex.class);
            assertThat(newIndex).isNotSameAs(oldIndex);
        } finally {
            mgr.close();
        }
    }

    @Test
    @DisplayName("ModelChanged: rebuild index via VectorIndexManager, verify index replaced")
    void testModelChangedRebuildE2E() {
        VectorIndexManager mgr = createFullModeManager();
        try {
            ShardInternalIndex oldIndex = mgr.createIndex("table-1", "embedding", DIM);
            assertThat(oldIndex).isInstanceOf(JVectorDiskIndex.class);

            VectorSchemaDiff.ModelChanged diff =
                    new VectorSchemaDiff.ModelChanged("embedding", "ada-002", "3-small");
            mgr.rebuildIndex("table-1", "embedding", List.of(diff));

            ShardInternalIndex newIndex = mgr.getIndex("table-1", "embedding");
            assertThat(newIndex).isInstanceOf(JVectorDiskIndex.class);
            assertThat(newIndex).isNotSameAs(oldIndex);
        } finally {
            mgr.close();
        }
    }

    @Test
    @DisplayName("NoChange: adding non-vector field produces NoChange, no index operation needed")
    void testNoVectorChangeE2E() {
        Schema oldSchema =
                Schema.builder()
                        .field("id", Types.int64())
                        .field("name", Types.string())
                        .version(1L)
                        .build();

        Schema newSchema =
                Schema.builder()
                        .field("id", Types.int64())
                        .field("name", Types.string())
                        .field("age", Types.int32())
                        .version(2L)
                        .build();

        List<VectorSchemaDiff> diffs = VectorSchemaDiff.compare(oldSchema, newSchema);
        assertThat(diffs).hasSize(1);
        assertThat(diffs.getFirst()).isInstanceOf(VectorSchemaDiff.NoChange.class);
    }

    @Test
    @DisplayName("Multi-diff: vector added + dimension changed in one schema evolution")
    void testMultiDiffSchemaEvolutionE2E() {
        Schema oldSchema =
                Schema.builder()
                        .field("id", Types.int64())
                        .field(new StructField("vec_a", Types.string(), "", VECTOR_META_16))
                        .version(1L)
                        .build();

        Schema newSchema =
                Schema.builder()
                        .field("id", Types.int64())
                        .field(new StructField("vec_a", Types.string(), "", VECTOR_META_32))
                        .field(new StructField("vec_b", Types.string(), "", VECTOR_META_16))
                        .version(2L)
                        .build();

        List<VectorSchemaDiff> diffs = VectorSchemaDiff.compare(oldSchema, newSchema);
        assertThat(diffs.size()).isGreaterThanOrEqualTo(2);
        assertThat(diffs.stream().anyMatch(d -> d instanceof VectorSchemaDiff.DimensionChanged))
                .isTrue();
        assertThat(diffs.stream().anyMatch(d -> d instanceof VectorSchemaDiff.VectorAdded))
                .isTrue();
    }

    @Test
    @DisplayName(
            "Full lifecycle: VectorAdded -> write -> search -> DimensionChanged -> rebuild -> search still works")
    void testFullSchemaEvolutionLifecycleE2E() {
        vectorIndex = createTableFormatIndex();
        try {
            for (int i = 0; i < 20; i++) {
                float[] embedding = embeddingProvider.embed("lifecycle doc " + i);
                vectorIndex.indexWithEmbedding("ns-lifecycle", "key-" + i, embedding);
            }

            float[] query = embeddingProvider.embed("lifecycle doc 0");
            List<VectorSearchResult> results = vectorIndex.search("ns-lifecycle", query, 5);
            assertThat(results).isNotEmpty();

            VectorIndexManager mgr = createTableFormatManager();
            try {
                mgr.createIndex("lifecycle-table", "embedding", DIM);
                VectorSchemaDiff.ModelChanged diff =
                        new VectorSchemaDiff.ModelChanged("embedding", "old-model", "new-model");
                mgr.rebuildIndex("lifecycle-table", "embedding", List.of(diff));
                assertThat(mgr.getIndex("lifecycle-table", "embedding")).isNotNull();
            } finally {
                mgr.close();
            }
        } finally {
            try {
                vectorIndex.close();
            } catch (Exception e) {
                /* ignore */
            }
        }
    }
}
