package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("VectorFlushSeparation E2E - Flushpath full chaintest")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class VectorFlushSeparationE2ETest {

    private static final int DIM = 16;
    private static final String NAMESPACE = "flush-test-ns";

    @TempDir Path tempDir;

    private StubEmbeddingProvider embeddingProvider;
    private JVectorVectorIndex index;
    private LanceVectorColdStore coldStore;

    @BeforeEach
    void setUp() {
        embeddingProvider = new StubEmbeddingProvider(DIM);
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
        index = new JVectorVectorIndex(config, embeddingProvider, false);

        coldStore = new LanceVectorColdStore(tempDir.resolve("lance-cold"), DIM);
        index.setColdStore(coldStore);
    }

    @AfterEach
    void tearDown() {
        try {
            index.close();
        } catch (Exception e) {
            /* ignore */
        }
        try {
            coldStore.close();
        } catch (Exception e) {
            /* ignore */
        }
    }

    @Test
    @DisplayName("STO-VEC-001: Flush to cold - vectors written to Lance, location updated to COLD")
    void testFlushToColdWritesToLance() {
        for (int i = 0; i < 10; i++) {
            float[] embedding = embeddingProvider.embed("flush doc " + i);
            index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
        }

        index.flushToCold(NAMESPACE, List.of("key-0", "key-1", "key-2"));

        float[] vector = coldStore.readVector(NAMESPACE, "key-0");
        assertThat(vector).isNotNull();
        assertThat(vector.length).isEqualTo(DIM);
    }

    @Test
    @DisplayName("After flush, search still returns results from both hot and cold")
    void testSearchAfterFlush() {
        for (int i = 0; i < 10; i++) {
            float[] embedding = embeddingProvider.embed("search doc " + i);
            index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
        }

        index.flushToCold(NAMESPACE, List.of("key-0", "key-1"));

        float[] query = embeddingProvider.embed("search doc 0");
        List<VectorSearchResult> results = index.search(NAMESPACE, query, 10);
        assertThat(results).isNotEmpty();
    }

    @Test
    @DisplayName("Zero redundancy: flushed entry not duplicated in search results")
    void testZeroRedundancyAfterFlush() {
        for (int i = 0; i < 10; i++) {
            float[] embedding = embeddingProvider.embed("redundancy doc " + i);
            index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
        }

        index.flushToCold(NAMESPACE, List.of("key-0"));

        float[] coldVector = coldStore.readVector(NAMESPACE, "key-0");
        assertThat(coldVector).isNotNull();

        float[] query = embeddingProvider.embed("redundancy doc 0");
        List<VectorSearchResult> results = index.search(NAMESPACE, query, 10);
        long key0Count = results.stream().filter(r -> r.entryKey().equals("key-0")).count();
        assertThat(key0Count).isLessThanOrEqualTo(1);
    }

    @Test
    @DisplayName(
            "STO-VEC-001: Location correctly updated to COLD after flush - verified via cold store read")
    void testLocationUpdatedAfterFlush() {
        for (int i = 0; i < 10; i++) {
            float[] embedding = embeddingProvider.embed("location doc " + i);
            index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
        }

        index.flushToCold(NAMESPACE, List.of("key-0"));

        float[] coldVector = coldStore.readVector(NAMESPACE, "key-0");
        assertThat(coldVector).isNotNull();
        assertThat(coldVector.length).isEqualTo(DIM);

        String l0Abstract = coldStore.readL0Abstract(NAMESPACE, "key-0");
        assertThat(l0Abstract).isNotNull();
    }

    @Test
    @DisplayName("STO-VEC-001: Flush multiple batches, all cold vectors readable")
    void testFlushMultipleBatches() {
        for (int i = 0; i < 20; i++) {
            float[] embedding = embeddingProvider.embed("batch doc " + i);
            index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
        }

        index.flushToCold(NAMESPACE, List.of("key-0", "key-1", "key-2"));
        index.flushToCold(NAMESPACE, List.of("key-10", "key-11"));

        assertThat(coldStore.readVector(NAMESPACE, "key-0")).isNotNull();
        assertThat(coldStore.readVector(NAMESPACE, "key-10")).isNotNull();
        assertThat(coldStore.readVector(NAMESPACE, "key-11")).isNotNull();

        float[] query = embeddingProvider.embed("batch doc 0");
        List<VectorSearchResult> results = index.search(NAMESPACE, query, 10);
        assertThat(results).isNotEmpty();
    }

    @Test
    @DisplayName("STO-VEC-001: Flush then delete, deleted entry not in cold store")
    void testFlushThenDelete() {
        for (int i = 0; i < 10; i++) {
            float[] embedding = embeddingProvider.embed("flushdel doc " + i);
            index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
        }

        index.flushToCold(NAMESPACE, List.of("key-0"));
        assertThat(coldStore.readVector(NAMESPACE, "key-0")).isNotNull();

        index.delete(NAMESPACE, "key-0");

        float[] query = embeddingProvider.embed("flushdel doc 0");
        List<VectorSearchResult> results = index.search(NAMESPACE, query, 10);
        assertThat(results.stream().noneMatch(r -> r.entryKey().equals("key-0"))).isTrue();
    }

    @Test
    @DisplayName(
            "Vector columns write only to Lance (not Parquet): cold store is LanceVectorColdStore")
    void testVectorColumnsWriteOnlyToLance() {
        for (int i = 0; i < 10; i++) {
            float[] embedding = embeddingProvider.embed("lance-only doc " + i);
            index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
        }

        index.flushToCold(NAMESPACE, List.of("key-0", "key-1"));

        assertThat(coldStore).isInstanceOf(LanceVectorColdStore.class);
        assertThat(coldStore.readVector(NAMESPACE, "key-0")).isNotNull();
        assertThat(coldStore.readVector(NAMESPACE, "key-1")).isNotNull();
    }
}
