package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Validates recall computation methodology: standard ann-benchmarks recall@10 uses GT@10 (exact
 * top-10), not GT@100. This test compares both metrics.
 */
@DisplayName("Recall Methodology: GT@10 vs GT@100")
@EnabledIfSystemProperty(named = "annbench.data.dir", matches = ".+")
class RecallMethodologyTest {

    @Test
    @DisplayName("Compare recall@10 with GT@10 vs GT@100")
    void testRecallMethodology() throws Exception {
        String dataDir = System.getProperty("annbench.data.dir");
        String datasetName = System.getProperty("annbench.dataset", "sift-128-euclidean");
        int trainLimit = Integer.getInteger("annbench.train.limit", 10000);
        int queryLimit = Integer.getInteger("annbench.query.limit", 100);

        Path datasetPath = Path.of(dataDir).resolve(datasetName);
        AnnBenchDatasetLoader.Dataset ds = AnnBenchDatasetLoader.load(datasetPath);

        List<float[]> train =
                trainLimit > 0 && trainLimit < ds.trainSize()
                        ? new ArrayList<>(ds.train().subList(0, trainLimit))
                        : ds.train();
        List<float[]> queries =
                queryLimit > 0 && queryLimit < ds.queryCount()
                        ? new ArrayList<>(ds.test().subList(0, queryLimit))
                        : ds.test();
        int[][] truth =
                queryLimit > 0 && queryLimit < ds.groundTruth().length
                        ? java.util.Arrays.copyOfRange(ds.groundTruth(), 0, queryLimit)
                        : ds.groundTruth();

        int topK = 10;
        int dim = ds.dimension();

        java.nio.file.Path indexPath = java.nio.file.Files.createTempDirectory("recall-method");
        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        dim,
                        32,
                        topK,
                        100,
                        indexPath.toString(),
                        train.size(),
                        train.size(),
                        train.size() * 2);

        List<String> keys = new ArrayList<>(train.size());
        for (int i = 0; i < train.size(); i++) keys.add("train-" + i);

        JVectorDiskIndex index = new JVectorDiskIndex(config, indexPath);
        index.initialize(train, keys);

        try {
            double totalRecallGt10 = 0;
            double totalRecallGt100 = 0;
            int validQueries = 0;

            for (int q = 0; q < queries.size(); q++) {
                List<SearchCandidate> results = index.search(queries.get(q), topK);
                if (results.isEmpty()) continue;

                List<String> resultKeys = new ArrayList<>();
                for (SearchCandidate c : results) resultKeys.add(c.entryKey());

                double recallGt10 = AnnBenchDatasetLoader.recallAtK(truth[q], 10, resultKeys, topK);
                double recallGt100 =
                        AnnBenchDatasetLoader.recallAtK(truth[q], 100, resultKeys, topK);
                totalRecallGt10 += recallGt10;
                totalRecallGt100 += recallGt100;
                validQueries++;
            }

            double avgRecallGt10 = validQueries > 0 ? totalRecallGt10 / validQueries : 0;
            double avgRecallGt100 = validQueries > 0 ? totalRecallGt100 / validQueries : 0;

            System.out.println("\n" + "=".repeat(60));
            System.out.printf(
                    "Dataset: %s, N=%,d, Q=%d%n", datasetName, train.size(), queries.size());
            System.out.printf("Recall@10 (GT@10,  standard): %.4f%n", avgRecallGt10);
            System.out.printf("Recall@10 (GT@100, lenient):   %.4f%n", avgRecallGt100);
            System.out.printf("Delta: %.4f%n", avgRecallGt100 - avgRecallGt10);
            System.out.println("=".repeat(60));

            assertThat(avgRecallGt10).isGreaterThan(0.0);
        } finally {
            index.close();
        }
    }
}
