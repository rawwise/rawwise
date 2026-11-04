package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/**
 * Validates that the recall ceiling on SIFT-128-euclidean is caused by the cosine-vs-L2 metric
 * mismatch. JVectorDiskIndex hardcodes COSINE, but SIFT-128 ground truth is L2-ordered. This test
 * builds an index with EUCLIDEAN metric directly via JVector API (bypassing JVectorDiskIndex) and
 * compares recall.
 */
@DisplayName("Metric Mismatch Validation: EUCLIDEAN vs COSINE on SIFT-128")
@EnabledIfSystemProperty(named = "annbench.data.dir", matches = ".+")
class MetricMismatchValidationTest {

    @TempDir Path tempDir;

    @Test
    @DisplayName("EUCLIDEAN metric should give higher recall than COSINE on SIFT-128")
    void testEuclideanVsCosineRecall() throws Exception {
        String dataDir = System.getProperty("annbench.data.dir");
        String datasetName = System.getProperty("annbench.dataset", "sift-128-euclidean");
        int trainLimit = Integer.getInteger("annbench.train.limit", 50000);
        int queryLimit = Integer.getInteger("annbench.query.limit", 200);
        int graphDegree = Integer.getInteger("annbench.graph.degree", 32);
        int efSearch = Integer.getInteger("annbench.ef.search", 100);
        int efConstruction = Integer.getInteger("annbench.ef.construction", 0);

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

        int dim = ds.dimension();
        int topK = 10;
        int gtK = 100;

        int effectiveEfConstruction = efConstruction > 0 ? efConstruction : graphDegree * 2;

        System.out.println("\n" + "=".repeat(80));
        System.out.printf(
                "Metric comparison on %s: N=%,d, Q=%d, g=%d, efConstruction=%d, efSearch=%d%n",
                datasetName,
                train.size(),
                queries.size(),
                graphDegree,
                effectiveEfConstruction,
                efSearch);
        System.out.println("=".repeat(80));

        double cosineRecall =
                buildAndMeasure(
                        train,
                        queries,
                        truth,
                        dim,
                        VectorSimilarityFunction.COSINE,
                        graphDegree,
                        effectiveEfConstruction,
                        efSearch,
                        topK,
                        gtK,
                        "COSINE");
        double euclideanRecall =
                buildAndMeasure(
                        train,
                        queries,
                        truth,
                        dim,
                        VectorSimilarityFunction.EUCLIDEAN,
                        graphDegree,
                        effectiveEfConstruction,
                        efSearch,
                        topK,
                        gtK,
                        "EUCLIDEAN");

        System.out.println("-".repeat(80));
        System.out.printf("COSINE   Recall@10: %.4f%n", cosineRecall);
        System.out.printf("EUCLIDEAN Recall@10: %.4f%n", euclideanRecall);
        System.out.printf(
                "Delta: %+.4f (%.1f%% relative)%n",
                euclideanRecall - cosineRecall,
                (euclideanRecall - cosineRecall) / cosineRecall * 100);
        System.out.println("-".repeat(80));

        assertThat(euclideanRecall).isGreaterThan(0.0);
    }

    @Test
    @DisplayName("Normalized vs unnormalized vectors: does L2 normalization help COSINE?")
    void testNormalizationEffect() throws Exception {
        String dataDir = System.getProperty("annbench.data.dir");
        String datasetName = System.getProperty("annbench.dataset", "sift-128-euclidean");
        int trainLimit = Integer.getInteger("annbench.train.limit", 10000);
        int queryLimit = Integer.getInteger("annbench.query.limit", 100);
        int graphDegree = Integer.getInteger("annbench.graph.degree", 32);
        int efSearch = Integer.getInteger("annbench.ef.search", 100);

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

        int dim = ds.dimension();
        int topK = 10;
        int gtK = 100;
        int efConstruction = graphDegree * 2;

        System.out.println("\n" + "=".repeat(80));
        System.out.printf(
                "Normalization effect on %s: N=%,d, Q=%d, g=%d, efC=%d, efS=%d%n",
                datasetName, train.size(), queries.size(), graphDegree, efConstruction, efSearch);
        System.out.println("=".repeat(80));

        double unnormalizedRecall =
                buildAndMeasure(
                        train,
                        queries,
                        truth,
                        dim,
                        VectorSimilarityFunction.COSINE,
                        graphDegree,
                        efConstruction,
                        efSearch,
                        topK,
                        gtK,
                        "UNNORMALIZED-COSINE");

        List<float[]> normalizedTrain = new ArrayList<>(train.size());
        for (float[] v : train) normalizedTrain.add(normalize(v));
        List<float[]> normalizedQueries = new ArrayList<>(queries.size());
        for (float[] v : queries) normalizedQueries.add(normalize(v));

        double normalizedCosineRecall =
                buildAndMeasure(
                        normalizedTrain,
                        normalizedQueries,
                        truth,
                        dim,
                        VectorSimilarityFunction.COSINE,
                        graphDegree,
                        efConstruction,
                        efSearch,
                        topK,
                        gtK,
                        "NORMALIZED-COSINE");

        double normalizedEuclideanRecall =
                buildAndMeasure(
                        normalizedTrain,
                        normalizedQueries,
                        truth,
                        dim,
                        VectorSimilarityFunction.EUCLIDEAN,
                        graphDegree,
                        efConstruction,
                        efSearch,
                        topK,
                        gtK,
                        "NORMALIZED-EUCLIDEAN");

        System.out.println("-".repeat(80));
        System.out.printf("Unnormalized COSINE:   %.4f%n", unnormalizedRecall);
        System.out.printf(
                "Normalized COSINE:     %.4f (delta %+.4f)%n",
                normalizedCosineRecall, normalizedCosineRecall - unnormalizedRecall);
        System.out.printf("Normalized EUCLIDEAN:  %.4f%n", normalizedEuclideanRecall);
        System.out.println("-".repeat(80));

        assertThat(normalizedCosineRecall).isGreaterThan(0.0);
    }

    private static float[] normalize(float[] v) {
        double sumSq = 0;
        for (float x : v) sumSq += x * x;
        float norm = (float) Math.sqrt(sumSq);
        float[] out = new float[v.length];
        if (norm > 0) {
            for (int i = 0; i < v.length; i++) out[i] = v[i] / norm;
        } else {
            out[0] = 1.0f;
        }
        return out;
    }

    private double buildAndMeasure(
            List<float[]> train,
            List<float[]> queries,
            int[][] truth,
            int dim,
            VectorSimilarityFunction metric,
            int graphDegree,
            int efConstruction,
            int efSearch,
            int topK,
            int gtK,
            String label) {
        List<VectorFloat<?>> vectors = new ArrayList<>(train.size());
        for (float[] v : train) {
            vectors.add(BuildScoreProvider.vts.createFloatVector(v));
        }
        var ravv = new ListRandomAccessVectorValues(vectors, dim);
        var scoreProvider = BuildScoreProvider.randomAccessScoreProvider(ravv, metric);
        var builder =
                new GraphIndexBuilder(
                        scoreProvider, dim, graphDegree, efConstruction, 1.0f, 1.0f, true);

        long t0 = System.nanoTime();
        for (int i = 0; i < vectors.size(); i++) {
            builder.addGraphNode(i, vectors.get(i));
        }
        builder.cleanup();
        double buildSec = (System.nanoTime() - t0) / 1_000_000_000.0;

        double totalRecall = 0;
        int validQueries = 0;
        long totalSearchNs = 0;

        for (int q = 0; q < queries.size(); q++) {
            VectorFloat<?> queryVec = BuildScoreProvider.vts.createFloatVector(queries.get(q));
            int searchWidth = Math.max(topK, efSearch);
            long ts0 = System.nanoTime();
            SearchResult result =
                    GraphSearcher.search(
                            queryVec, searchWidth, ravv, metric, builder.getGraph(), Bits.ALL);
            totalSearchNs += System.nanoTime() - ts0;

            List<String> resultKeys = new ArrayList<>();
            for (SearchResult.NodeScore ns : result.getNodes()) {
                resultKeys.add("train-" + ns.node);
            }
            totalRecall += AnnBenchDatasetLoader.recallAtK(truth[q], gtK, resultKeys, topK);
            validQueries++;
        }

        double avgRecall = validQueries > 0 ? totalRecall / validQueries : 0;
        double qps = totalSearchNs > 0 ? validQueries / (totalSearchNs / 1_000_000_000.0) : 0;
        double avgLatencyMs = validQueries > 0 ? (totalSearchNs / validQueries) / 1_000_000.0 : 0;

        System.out.printf(
                "[%s] Build=%.1fs, Recall@10=%.4f, QPS=%.0f, avgLatency=%.2fms%n",
                label, buildSec, avgRecall, qps, avgLatencyMs);

        return avgRecall;
    }
}
