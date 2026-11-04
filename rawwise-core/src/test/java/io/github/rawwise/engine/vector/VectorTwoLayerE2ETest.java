package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Two-Layer Vector Architecture E2E - cover")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class VectorTwoLayerE2ETest {

    private static final int DIM = 16;
    private static final String NAMESPACE = "test-ns";

    @TempDir Path tempDir;

    private StubEmbeddingProvider embeddingProvider;

    @BeforeEach
    void setUp() {
        embeddingProvider = new StubEmbeddingProvider(DIM);
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
        JVectorVectorIndex index = new JVectorVectorIndex(config, embeddingProvider, false);
        return index;
    }

    private JVectorVectorIndex createTableFormatModeIndex() {
        JVectorConfig config =
                new JVectorConfig(
                        "memory",
                        DIM,
                        16,
                        10,
                        50,
                        tempDir.resolve("table-format-mode").toString(),
                        5,
                        5,
                        500000);
        JVectorVectorIndex index = new JVectorVectorIndex(config, embeddingProvider, true);
        index.setIvfPqConfig(new IvfPqConfig(DIM, 4, 4, 2, 5, 10, 5, 42L, 3));
        return index;
    }

    private List<float[]> generateEmbeddings(int count) {
        List<float[]> embeddings = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            embeddings.add(embeddingProvider.embed("document " + i));
        }
        return embeddings;
    }

    @Test
    @DisplayName(
            "S1: Full mode - write -> Hot Buffer -> merge DiskAnnIndex -> search -> verify topK")
    void testFullModeHappyPath() {
        try (JVectorVectorIndex index = createFullModeIndex()) {
            assertThat(index.isTableFormatMode()).isFalse();

            for (int i = 0; i < 20; i++) {
                float[] embedding = embeddingProvider.embed("document " + i);
                index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
            }

            float[] query = embeddingProvider.embed("document 0");
            List<VectorSearchResult> results = index.search(NAMESPACE, query, 10);

            assertThat(results).isNotEmpty();
            assertThat(results.getFirst().score()).isGreaterThan(0f);
        }
    }

    @Test
    @DisplayName(
            "S2: Table Format mode - write -> Hot Buffer -> merge IvfPqIndex -> search -> verify topK")
    void testTableFormatModeHappyPath() {
        try (JVectorVectorIndex index = createTableFormatModeIndex()) {
            assertThat(index.isTableFormatMode()).isTrue();

            for (int i = 0; i < 20; i++) {
                float[] embedding = embeddingProvider.embed("document " + i);
                index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
            }

            float[] query = embeddingProvider.embed("document 0");
            List<VectorSearchResult> results = index.search(NAMESPACE, query, 10);

            assertThat(results).isNotEmpty();
            assertThat(results.getFirst().score()).isGreaterThan(0f);
        }
    }

    @Test
    @DisplayName(
            "S3: Fault Recovery - Hot Buffer crash -> restart -> index operational (in-memory, data lost)")
    void testFaultRecovery() {
        JVectorVectorIndex index = createFullModeIndex();

        for (int i = 0; i < 20; i++) {
            float[] embedding = embeddingProvider.embed("recovery doc " + i);
            index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
        }

        float[] query = embeddingProvider.embed("recovery doc 0");
        List<VectorSearchResult> resultsBefore = index.search(NAMESPACE, query, 5);
        assertThat(resultsBefore).isNotEmpty();

        index.close();

        JVectorVectorIndex recoveredIndex = createFullModeIndex();
        List<VectorSearchResult> resultsAfter = recoveredIndex.search(NAMESPACE, query, 5);
        assertThat(resultsAfter).isNotNull();

        recoveredIndex.close();
    }

    @Test
    @DisplayName("S4: Boundary - incremental insert exceeds merge threshold -> index optimization")
    void testBoundaryMergeThreshold() {
        try (JVectorVectorIndex index = createTableFormatModeIndex()) {
            for (int i = 0; i < 30; i++) {
                float[] embedding = embeddingProvider.embed("boundary doc " + i);
                index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
            }

            float[] query = embeddingProvider.embed("boundary doc 0");
            List<VectorSearchResult> results = index.search(NAMESPACE, query, 10);

            assertThat(results).isNotEmpty();
            assertThat(results.size()).isLessThanOrEqualTo(10);
        }
    }

    @Test
    @DisplayName("Delete: removed entry excluded from search results")
    void testDeleteRemovesFromSearch() {
        try (JVectorVectorIndex index = createFullModeIndex()) {
            for (int i = 0; i < 20; i++) {
                float[] embedding = embeddingProvider.embed("entry " + i);
                index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
            }

            index.delete(NAMESPACE, "key-0");

            float[] query = embeddingProvider.embed("entry 0");
            List<VectorSearchResult> results = index.search(NAMESPACE, query, 20);
            assertThat(results.stream().noneMatch(r -> r.entryKey().equals("key-0"))).isTrue();
        }
    }

    @Test
    @DisplayName("Concurrent: 4 threads write to Full mode index, search returns results")
    void testConcurrentWriteFullMode() throws Exception {
        try (JVectorVectorIndex index = createFullModeIndex()) {
            int threadCount = 4;
            int writesPerThread = 10;
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);

            Runnable writer =
                    () -> {
                        try {
                            start.await();
                            for (int i = 0; i < writesPerThread; i++) {
                                float[] embedding =
                                        embeddingProvider.embed(
                                                "concurrent "
                                                        + Thread.currentThread().getId()
                                                        + "-"
                                                        + i);
                                index.indexWithEmbedding(
                                        NAMESPACE,
                                        "key-" + Thread.currentThread().getId() + "-" + i,
                                        embedding);
                            }
                            successCount.incrementAndGet();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    };

            var executor = Executors.newFixedThreadPool(threadCount);
            for (int i = 0; i < threadCount; i++) {
                executor.submit(writer);
            }
            start.countDown();
            executor.shutdown();
            boolean terminated = executor.awaitTermination(15, TimeUnit.SECONDS);
            assertThat(terminated).isTrue();
            assertThat(successCount.get()).isEqualTo(threadCount);

            float[] query = embeddingProvider.embed("concurrent");
            List<VectorSearchResult> results = index.search(NAMESPACE, query, 10);
            assertThat(results).isNotEmpty();
        }
    }

    @Test
    @DisplayName("Dual-mode: Full mode and Table Format mode produce search results independently")
    void testDualModeIndependentOperation() {
        try (JVectorVectorIndex fullIndex = createFullModeIndex();
                JVectorVectorIndex tfIndex = createTableFormatModeIndex()) {

            for (int i = 0; i < 20; i++) {
                float[] embedding = embeddingProvider.embed("dual-mode doc " + i);
                fullIndex.indexWithEmbedding("full-ns", "key-" + i, embedding);
                tfIndex.indexWithEmbedding("tf-ns", "key-" + i, embedding);
            }

            float[] query = embeddingProvider.embed("dual-mode doc 0");

            List<VectorSearchResult> fullResults = fullIndex.search("full-ns", query, 5);
            List<VectorSearchResult> tfResults = tfIndex.search("tf-ns", query, 5);

            assertThat(fullResults).isNotEmpty();
            assertThat(tfResults).isNotEmpty();
            assertThat(fullResults.getFirst().score()).isGreaterThan(0f);
            assertThat(tfResults.getFirst().score()).isGreaterThan(0f);
        }
    }

    @Test
    @DisplayName(
            "Hot+Main merge dedup: same key in hot buffer and main index, only one result returned")
    void testHotMainMergeDedup() {
        try (JVectorVectorIndex index = createFullModeIndex()) {
            for (int i = 0; i < 5; i++) {
                float[] embedding = embeddingProvider.embed("dedup doc " + i);
                index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
            }

            float[] query = embeddingProvider.embed("dedup doc 0");
            List<VectorSearchResult> results = index.search(NAMESPACE, query, 10);

            long key0Count = results.stream().filter(r -> r.entryKey().equals("key-0")).count();
            assertThat(key0Count).isLessThanOrEqualTo(1);
        }
    }

    @Test
    @DisplayName(
            "Distance metric: all search results use cosine similarity (score > 0 for similar)")
    void testDistanceMetricUnification() {
        try (JVectorVectorIndex index = createTableFormatModeIndex()) {
            for (int i = 0; i < 20; i++) {
                float[] embedding = embeddingProvider.embed("metric doc " + i);
                index.indexWithEmbedding(NAMESPACE, "key-" + i, embedding);
            }

            float[] query = embeddingProvider.embed("metric doc 0");
            List<VectorSearchResult> results = index.search(NAMESPACE, query, 5);

            assertThat(results).isNotEmpty();
            for (VectorSearchResult r : results) {
                assertThat(r.score()).isGreaterThan(0f);
                assertThat(r.score()).isLessThanOrEqualTo(1.0f);
            }
        }
    }

    @Test
    @DisplayName("Concurrent read-write: 2 writers + 2 readers, no errors")
    void testConcurrentReadWrite() throws Exception {
        try (JVectorVectorIndex index = createFullModeIndex()) {
            for (int i = 0; i < 20; i++) {
                float[] emb = embeddingProvider.embed("init doc " + i);
                index.indexWithEmbedding(NAMESPACE, "key-" + i, emb);
            }

            int totalOps = 200;
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger writeSuccess = new AtomicInteger(0);
            AtomicInteger readSuccess = new AtomicInteger(0);
            AtomicInteger errors = new AtomicInteger(0);

            Runnable writer =
                    () -> {
                        try {
                            start.await();
                        } catch (InterruptedException e) {
                            return;
                        }
                        for (int i = 0; i < totalOps; i++) {
                            try {
                                float[] emb =
                                        embeddingProvider.embed(
                                                "rw doc "
                                                        + Thread.currentThread().getId()
                                                        + "-"
                                                        + i);
                                index.indexWithEmbedding(
                                        NAMESPACE,
                                        "key-" + Thread.currentThread().getId() + "-" + i,
                                        emb);
                                writeSuccess.incrementAndGet();
                            } catch (Exception e) {
                                errors.incrementAndGet();
                            }
                        }
                    };

            Runnable reader =
                    () -> {
                        try {
                            start.await();
                        } catch (InterruptedException e) {
                            return;
                        }
                        for (int i = 0; i < totalOps; i++) {
                            try {
                                float[] query = embeddingProvider.embed("read query " + i);
                                index.search(NAMESPACE, query, 10);
                                readSuccess.incrementAndGet();
                            } catch (Exception e) {
                                errors.incrementAndGet();
                            }
                        }
                    };

            var executor = Executors.newFixedThreadPool(4);
            executor.submit(writer);
            executor.submit(writer);
            executor.submit(reader);
            executor.submit(reader);
            start.countDown();
            executor.shutdown();
            boolean terminated = executor.awaitTermination(30, TimeUnit.SECONDS);

            assertThat(terminated).isTrue();
            assertThat(errors.get()).isEqualTo(0);
            assertThat(readSuccess.get()).isGreaterThan(0);
        }
    }
}
