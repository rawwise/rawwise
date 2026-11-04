package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Smoke test for the ann-benchmarks integration: generates a small synthetic dataset (with cluster
 * structure so recall is meaningful), loads it via {@link AnnBenchDatasetLoader}, runs the
 * benchmark, and verifies the plumbing works end-to-end without downloading real datasets.
 *
 * <p>This does NOT validate real recall numbers - use {@link AnnBenchStandardBenchmarkTest} with
 * real ann-benchmarks data for that. It only validates: file format parsing, recall computation,
 * latency measurement, dataset loader, and test framework integration.
 */
@DisplayName("ann-benchmarks Smoke Test (synthetic data)")
class AnnBenchSmokeTest {

    @TempDir Path tempDir;

    @Test
    @DisplayName("Generate synthetic dataset -> load -> verify metadata")
    void testGenerateAndLoad() throws Exception {
        Path dataDir = tempDir.resolve("synthetic");
        SyntheticAnnBenchGenerator.GenConfig cfg = SyntheticAnnBenchGenerator.GenConfig.small();
        SyntheticAnnBenchGenerator.generate(dataDir, cfg);

        assertThat(AnnBenchDatasetLoader.isAvailable(dataDir)).isTrue();

        AnnBenchDatasetLoader.Dataset ds = AnnBenchDatasetLoader.load(dataDir);
        assertThat(ds.meta().dim()).isEqualTo(cfg.dim());
        assertThat(ds.meta().trainCount()).isEqualTo(cfg.trainCount());
        assertThat(ds.meta().testCount()).isEqualTo(cfg.queryCount());
        assertThat(ds.meta().truthK()).isEqualTo(cfg.truthK());
        assertThat(ds.train()).hasSize(cfg.trainCount());
        assertThat(ds.test()).hasSize(cfg.queryCount());
        assertThat(ds.groundTruth().length).isEqualTo(cfg.queryCount());
        assertThat(ds.groundTruth()[0]).hasSize(cfg.truthK());
        assertThat(ds.train().get(0)).hasSize(cfg.dim());
    }

    @Test
    @DisplayName("Recall computation: exact search should give ~1.0 recall")
    void testRecallComputationExact() {
        int[] groundTruth = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        java.util.List<String> exactResults =
                java.util.List.of(
                        "train-0", "train-1", "train-2", "train-3", "train-4", "train-5", "train-6",
                        "train-7", "train-8", "train-9");
        double recall = AnnBenchDatasetLoader.recallAtK(groundTruth, 10, exactResults, 10);
        assertThat(recall).isEqualTo(1.0);

        java.util.List<String> partialResults =
                java.util.List.of("train-0", "train-1", "train-2", "train-99", "train-98");
        double partialRecall = AnnBenchDatasetLoader.recallAtK(groundTruth, 10, partialResults, 5);
        assertThat(partialRecall).isEqualTo(3.0 / 5.0);

        java.util.List<String> gt100Results =
                java.util.List.of(
                        "train-0", "train-1", "train-2", "train-3", "train-4", "train-5", "train-6",
                        "train-7", "train-8", "train-9");
        double recallVsGt100 = AnnBenchDatasetLoader.recallAtK(groundTruth, 100, gt100Results, 10);
        assertThat(recallVsGt100).isEqualTo(1.0);
    }

    @Test
    @DisplayName("End-to-end: synthetic dataset -> JVector index -> recall > 0")
    void testEndToEndWithJVector() throws Exception {
        Path dataDir = tempDir.resolve("synthetic-e2e");
        SyntheticAnnBenchGenerator.GenConfig cfg =
                new SyntheticAnnBenchGenerator.GenConfig(
                        "synthetic-e2e", 128, 5_000, 100, 50, 20, 123L);
        SyntheticAnnBenchGenerator.generate(dataDir, cfg);

        AnnBenchDatasetLoader.Dataset ds = AnnBenchDatasetLoader.load(dataDir);

        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        ds.dimension(),
                        32,
                        10,
                        50,
                        tempDir.resolve("jvector-idx").toString(),
                        100000,
                        100000,
                        500000);
        StubEmbeddingProvider stubProvider = new StubEmbeddingProvider(ds.dimension());
        JVectorVectorIndex index = new JVectorVectorIndex(config, stubProvider, false);

        String namespace = "annbench-smoke";
        try {
            for (int i = 0; i < ds.trainSize(); i++) {
                index.indexWithEmbedding(namespace, "train-" + i, ds.train().get(i));
            }

            double totalRecall = 0;
            int validQueries = 0;
            for (int q = 0; q < ds.queryCount(); q++) {
                java.util.List<VectorSearchResult> results =
                        index.search(namespace, ds.test().get(q), 10);
                if (results.isEmpty()) continue;
                java.util.List<String> resultKeys = new java.util.ArrayList<>();
                for (VectorSearchResult r : results) resultKeys.add(r.entryKey());
                totalRecall +=
                        AnnBenchDatasetLoader.recallAtK(ds.groundTruth()[q], 50, resultKeys, 10);
                validQueries++;
            }
            double avgRecall = validQueries > 0 ? totalRecall / validQueries : 0;
            System.out.printf(
                    "%n[smoke] Synthetic dataset Recall@10 (GT@50): %.4f (n=%d)%n",
                    avgRecall, validQueries);
            assertThat(avgRecall).isGreaterThan(0.0);
        } finally {
            index.close();
        }
    }

    @Test
    @DisplayName("Missing dataset directory produces helpful error")
    void testMissingDatasetError() {
        Path missing = tempDir.resolve("does-not-exist");
        assertThat(AnnBenchDatasetLoader.isAvailable(missing)).isFalse();
        try {
            AnnBenchDatasetLoader.load(missing);
            assertThat(false).as("Expected IOException").isTrue();
        } catch (java.io.IOException e) {
            assertThat(e.getMessage()).contains("not found");
            assertThat(e.getMessage()).contains("download_annbench.py");
        }
    }
}
