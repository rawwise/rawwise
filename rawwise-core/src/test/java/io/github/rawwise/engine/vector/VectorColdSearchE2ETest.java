package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Cold Storage Search E2E")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class VectorColdSearchE2ETest {

    private static final int DIM = 16;
    private static final String NAMESPACE = "test-ns";

    @TempDir Path tempDir;

    private StubEmbeddingProvider embedder;

    @BeforeEach
    void setUp() {
        embedder = new StubEmbeddingProvider(DIM);
    }

    private JVectorVectorIndex createFullModeIndex() {
        JVectorConfig config =
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
        return new JVectorVectorIndex(config, embedder, false);
    }

    @Test
    @DisplayName("Flush then search: cold data searchable via coldStore")
    void testColdDataInSearchAfterFlush() {
        LanceVectorColdStore coldStore =
                new LanceVectorColdStore(tempDir.resolve("cold-flush-store"), DIM);

        List<ColdVectorEntry> entries =
                java.util.Arrays.asList(
                        new ColdVectorEntry("x", embedder.embed("unique-data"), "unique-data"),
                        new ColdVectorEntry("y", embedder.embed("other-data"), "other-data"));
        coldStore.write("ns", "frag-1", entries);

        float[] query = embedder.embed("unique-data");
        List<VectorSearchResult> results = coldStore.search("ns", query, 2);

        assertThat(results).isNotEmpty();
        assertThat(results.get(0).entryKey()).isEqualTo("x");
        coldStore.close();
    }

    @Test
    @DisplayName("Cold store brute-force search delivers correct top result")
    void testColdStoreSearchDirectly() {
        LanceVectorColdStore coldStore =
                new LanceVectorColdStore(tempDir.resolve("cold-direct"), DIM);

        List<ColdVectorEntry> entries =
                java.util.Arrays.asList(
                        new ColdVectorEntry("a", embedder.embed("hello world"), "hello world"),
                        new ColdVectorEntry("b", embedder.embed("foo bar baz"), "foo bar baz"),
                        new ColdVectorEntry("c", embedder.embed("hello nebula"), "hello nebula"));
        coldStore.write("test", "frag-1", entries);

        float[] query = embedder.embed("hello world");
        List<VectorSearchResult> results = coldStore.search("test", query, 3);

        assertThat(results).isNotEmpty();
        assertThat(results.get(0).entryKey()).isEqualTo("a");
    }

    @Test
    @DisplayName("Flush all data, search still works through cold store")
    void testFlushEverythingThenSearch() {
        try (JVectorVectorIndex index = createFullModeIndex()) {
            LanceVectorColdStore coldStore =
                    new LanceVectorColdStore(tempDir.resolve("cold-flush-all"), DIM);
            index.setColdStore(coldStore);

            for (int i = 0; i < 15; i++) {
                float[] emb = embedder.embed("flush-all doc " + i);
                index.indexWithEmbedding(NAMESPACE, "key-" + i, emb);
            }

            java.util.List<String> allKeys = new java.util.ArrayList<>();
            for (int i = 0; i < 15; i++) allKeys.add("key-" + i);
            index.flushToCold(NAMESPACE, allKeys);

            float[] query = embedder.embed("flush-all doc 5");
            List<VectorSearchResult> results = index.search(NAMESPACE, query, 5);

            assertThat(results).isNotEmpty();
            assertThat(results.stream().anyMatch(r -> r.entryKey().equals("key-5"))).isTrue();
        }
    }

    @Test
    @DisplayName("Delete from cold store: deleted entry excluded from cold search")
    void testDeleteExcludesFromColdSearch() {
        LanceVectorColdStore coldStore =
                new LanceVectorColdStore(tempDir.resolve("cold-delete"), DIM);

        List<ColdVectorEntry> entries =
                java.util.Arrays.asList(
                        new ColdVectorEntry("x", embedder.embed("delete me"), "delete me"),
                        new ColdVectorEntry("y", embedder.embed("keep me"), "keep me"));
        coldStore.write("test", "frag-1", entries);
        coldStore.delete("test", List.of("x"));

        float[] query = embedder.embed("delete me");
        List<VectorSearchResult> results = coldStore.search("test", query, 2);

        assertThat(results.stream().noneMatch(r -> r.entryKey().equals("x"))).isTrue();
    }
}
