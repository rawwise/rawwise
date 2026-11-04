package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/**
 * RawWise JVector benchmark against ann-benchmarks standard datasets.
 *
 * <p>This test provides industry-standard ANN (Approximate Nearest Neighbor) recall and latency
 * metrics comparable to published benchmarks. It replaces the synthetic-vector measurements in
 * {@link JVectorDiskIndexBenchmarkTest} with real-world datasets.
 *
 * <h2>Standard Datasets</h2>
 *
 * <ul>
 *   <li><b>SIFT-128-euclidean</b>: 1M train / 10K query / D=128 / L2 - the canonical ANN benchmark,
 *       used by FAISS, HNSW, ScaNN, etc.
 *   <li><b>GLoVe-25-angular</b>: 1.18M train / 10K query / D=25 / cosine - word embedding
 *       similarity, low-dimension stress test.
 *   <li><b>Fashion-MNIST-784-cosine</b>: 60K train / 10K query / D=784 / cosine - image similarity,
 *       high-dimension.
 *   <li><b>MNIST-784-euclidean</b>: 60K train / 10K query / D=784 / L2.
 * </ul>
 *
 * <h2>Metrics</h2>
 *
 * <ul>
 *   <li><b>Recall@10</b>: fraction of ground-truth top-10 found in approximate top-10. Standard
 *       ann-benchmarks primary metric.
 *   <li><b>Recall@100 vs GT@100</b>: looser recall, useful for ranking quality.
 *   <li><b>p50 / p95 / p99 latency</b>: query latency distribution.
 *   <li><b>QPS</b>: queries per second (single-thread).
 *   <li><b>Build time</b>: index construction time.
 *   <li><b>Index size on disk</b>: bytes consumed by the graph index.
 * </ul>
 *
 * <h2>Running</h2>
 *
 * <pre>
 * # 1. Download datasets (one-time, ~1.2GB total)
 * python3 bin/benchmark/download_annbench.py --dataset sift-128-euclidean \
 *   --out-dir benchmark/annbench/data
 *
 * # 2. Run benchmark
 * mvn test -pl rawwise-core -Dtest=AnnBenchStandardBenchmarkTest \
 *   -DfailIfNoTests=false \
 *   -Dannbench.data.dir=benchmark/annbench/data \
 *   -Dannbench.dataset=sift-128-euclidean
 * </pre>
 *
 * <p>System properties:
 *
 * <ul>
 *   <li><code>annbench.data.dir</code> - dataset root dir (required to enable test)
 *   <li><code>annbench.dataset</code> - which dataset to run (default: sift-128-euclidean)
 *   <li><code>annbench.train.limit</code> - cap training set size (default: 0 = no limit)
 *   <li><code>annbench.query.limit</code> - cap query count (default: 1000)
 *   <li><code>annbench.graph.degree</code> - JVector graphDegree (default: 32; try 48 or 64 for
 *       higher recall)
 *   <li><code>annbench.ef.search</code> - JVector efSearch (default: 50; higher = better recall,
 *       slower)
 * </ul>
 *
 * <p>This test is disabled by default. It only runs when <code>-Dannbench.data.dir=...</code> is
 * set and the dataset is present.
 *
 * <p><b>Bug fix</b>: JVector's OnDiskGraphIndex had a version mismatch bug where {@code
 * JVectorDiskIndex.persistToDisk()} appended an 8-byte CRC32 footer after JVector's own footer,
 * causing {@code OnDiskGraphIndex.load()} to read the CRC32 bytes as the footer magic ("Unsupported
 * version" error). Fixed by {@link CRC32StrippedReaderSupplier} which transparently strips the
 * trailing CRC32 bytes when loading. {@link JVectorDiskIndexBenchmarkTest} and this test now pass
 * correctly.
 */
@DisplayName("ann-benchmarks Standard ANN Benchmark")
@EnabledIfSystemProperty(named = "annbench.data.dir", matches = ".+")
class AnnBenchStandardBenchmarkTest {

    private static final int TOP_K = 10;
    private static final int TOP_K_100 = 100;
    private static final int GT_K = 100;

    @TempDir Path tempDir;

    private static AnnBenchDatasetLoader.Dataset dataset;
    private static String datasetName;
    private static int graphDegree;
    private static int efSearch;
    private static int trainLimit;
    private static int queryLimit;

    @BeforeAll
    static void loadDataset() throws Exception {
        String dataDir = System.getProperty("annbench.data.dir");
        datasetName = System.getProperty("annbench.dataset", "sift-128-euclidean");
        trainLimit = Integer.getInteger("annbench.train.limit", 0);
        queryLimit = Integer.getInteger("annbench.query.limit", 1000);
        graphDegree = Integer.getInteger("annbench.graph.degree", 32);
        efSearch = Integer.getInteger("annbench.ef.search", 50);

        Path datasetPath = Path.of(dataDir).resolve(datasetName);
        if (!AnnBenchDatasetLoader.isAvailable(datasetPath)) {
            throw new IllegalStateException(
                    "Dataset "
                            + datasetName
                            + " not found at "
                            + datasetPath
                            + ". Run: python3 bin/benchmark/download_annbench.py --dataset "
                            + datasetName
                            + " --out-dir "
                            + dataDir);
        }
        dataset = AnnBenchDatasetLoader.load(datasetPath);
        System.out.println("\n" + "=".repeat(90));
        System.out.printf("ann-benchmarks: %s%n", datasetName);
        System.out.printf(
                "  dim=%d  metric=%s  train=%d  query=%d  truthK=%d%n",
                dataset.dimension(),
                dataset.meta().metric(),
                dataset.trainSize(),
                dataset.queryCount(),
                dataset.truthK());
        System.out.printf(
                "  config: graphDegree=%d  efSearch=%d  trainLimit=%d  queryLimit=%d%n",
                graphDegree, efSearch, trainLimit, queryLimit);
        System.out.println("=".repeat(90));
    }

    @AfterAll
    static void cleanup() {
        dataset = null;
    }

    @Test
    @DisplayName("Recall@10 + latency vs ground truth (SIFT-128 / GLoVe-25 / MNIST)")
    void benchmarkRecallAndLatency() throws Exception {
        List<float[]> train = subSampleTrain(dataset.train(), trainLimit);
        List<float[]> queries = subSampleQueries(dataset.test(), queryLimit);
        int[][] truth = subSampleTruth(dataset.groundTruth(), queryLimit);

        int dim = dataset.dimension();
        Path indexPath = tempDir.resolve("annbench-" + datasetName);
        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        dim,
                        graphDegree,
                        TOP_K,
                        efSearch,
                        indexPath.toString(),
                        train.size(),
                        train.size(),
                        train.size() * 2);

        List<String> keys = new ArrayList<>(train.size());
        for (int i = 0; i < train.size(); i++) {
            keys.add("train-" + i);
        }

        JVectorDiskIndex index = new JVectorDiskIndex(config, indexPath);
        long buildStart = System.nanoTime();
        index.initialize(train, keys);
        long buildEnd = System.nanoTime();
        double buildSec = (buildEnd - buildStart) / 1_000_000_000.0;

        long indexSizeBytes = computeDirSize(indexPath);
        double indexSizeMb = indexSizeBytes / 1_000_000.0;

        BenchmarkStats stats10 = measureRecallAndLatency(index, queries, truth, TOP_K);
        BenchmarkStats stats100 = measureRecallAndLatency(index, queries, truth, TOP_K_100);

        index.close();

        System.out.println();
        System.out.println("-".repeat(90));
        System.out.printf(
                "Result: %s (graphDegree=%d, efSearch=%d)%n", datasetName, graphDegree, efSearch);
        System.out.printf("  Train size:      %,d vectors%n", train.size());
        System.out.printf("  Query count:     %,d%n", queries.size());
        System.out.printf("  Build time:      %.2f s%n", buildSec);
        System.out.printf("  Index size:      %.1f MB%n", indexSizeMb);
        System.out.printf("  --- Recall@10 --- (GT@%d)%n", GT_K);
        System.out.printf("    mean:          %.4f%n", stats10.meanRecall);
        System.out.printf("    p50:           %.4f%n", stats10.p50Recall);
        System.out.printf("    p99:           %.4f%n", stats10.p99Recall);
        System.out.printf("    zero-hit rate: %.2f%%%n", stats10.zeroHitRate * 100);
        System.out.printf("    latency p50:   %.2f ms%n", stats10.p50LatencyMs);
        System.out.printf("    latency p95:   %.2f ms%n", stats10.p95LatencyMs);
        System.out.printf("    latency p99:   %.2f ms%n", stats10.p99LatencyMs);
        System.out.printf("    QPS (1 thread):%.0f%n", stats10.qps);
        System.out.printf("  --- Recall@100 --- (GT@%d)%n", GT_K);
        System.out.printf("    mean:          %.4f%n", stats100.meanRecall);
        System.out.printf("    latency p99:   %.2f ms%n", stats100.p99LatencyMs);
        System.out.println("-".repeat(90));

        assertThat(stats10.meanRecall).isGreaterThan(0.0);
        assertThat(stats10.qps).isGreaterThan(0.0);

        if (train.size() >= 10_000) {
            assertThat(stats10.meanRecall)
                    .as(
                            "Recall@10 for %s with graphDegree=%d efSearch=%d should be > 0.5",
                            datasetName, graphDegree, efSearch)
                    .isGreaterThan(0.5);
        }
    }

    @Test
    @DisplayName("Parameter sweep: graphDegree effect on Recall@10")
    void benchmarkGraphDegreeSweep() throws Exception {
        if (trainLimit > 0 && trainLimit < 50_000) {
            System.out.println(
                    "\n[skip] graphDegree sweep requires trainLimit >= 50000 or 0 (unlimited)");
            return;
        }
        List<float[]> train = subSampleTrain(dataset.train(), trainLimit);
        List<float[]> queries = subSampleQueries(dataset.test(), Math.min(queryLimit, 200));
        int[][] truth = subSampleTruth(dataset.groundTruth(), Math.min(queryLimit, 200));
        int dim = dataset.dimension();
        List<String> keys = new ArrayList<>(train.size());
        for (int i = 0; i < train.size(); i++) keys.add("train-" + i);

        int[] degrees = {32, 48, 64};
        System.out.println("\n" + "=".repeat(70));
        System.out.printf(
                "graphDegree sweep: %s, N=%,d, Q=%d%n", datasetName, train.size(), queries.size());
        System.out.printf(
                "%-12s %10s %12s %12s %12s%n", "degree", "build(s)", "Recall@10", "p99(ms)", "QPS");
        System.out.println("-".repeat(70));

        for (int degree : degrees) {
            Path indexPath = tempDir.resolve("sweep-g" + degree);
            JVectorConfig config =
                    new JVectorConfig(
                            "disk",
                            dim,
                            degree,
                            TOP_K,
                            efSearch,
                            indexPath.toString(),
                            train.size(),
                            train.size(),
                            train.size() * 2);
            JVectorDiskIndex index = new JVectorDiskIndex(config, indexPath);
            long t0 = System.nanoTime();
            index.initialize(train, keys);
            double buildSec = (System.nanoTime() - t0) / 1_000_000_000.0;
            BenchmarkStats stats = measureRecallAndLatency(index, queries, truth, TOP_K);
            index.close();
            System.out.printf(
                    "%-12d %10.2f %12.4f %12.2f %12.0f%n",
                    degree, buildSec, stats.meanRecall, stats.p99LatencyMs, stats.qps);
        }
        System.out.println("=".repeat(70));
    }

    private BenchmarkStats measureRecallAndLatency(
            JVectorDiskIndex index, List<float[]> queries, int[][] truth, int topK) {
        List<Double> recalls = new ArrayList<>(queries.size());
        List<Long> latencies = new ArrayList<>(queries.size());
        int zeroHits = 0;

        long totalStart = System.nanoTime();
        for (int i = 0; i < queries.size(); i++) {
            long t0 = System.nanoTime();
            List<SearchCandidate> results = index.search(queries.get(i), topK);
            long t1 = System.nanoTime();
            latencies.add((t1 - t0) / 1_000_000L);

            List<String> resultKeys = new ArrayList<>(results.size());
            for (SearchCandidate c : results) {
                resultKeys.add(c.entryKey());
            }
            double recall = AnnBenchDatasetLoader.recallAtK(truth[i], GT_K, resultKeys, topK);
            recalls.add(recall);
            if (recall == 0.0) zeroHits++;
        }
        long totalNs = System.nanoTime() - totalStart;
        double totalSec = totalNs / 1_000_000_000.0;

        recalls.sort(Comparator.naturalOrder());
        latencies.sort(Comparator.naturalOrder());
        int n = recalls.size();

        double meanRecall = recalls.stream().mapToDouble(d -> d).average().orElse(0);
        double p50Recall = n > 0 ? recalls.get(n / 2) : 0;
        double p99Recall = n > 0 ? recalls.get((int) (n * 0.99)) : 0;
        double p50Latency = n > 0 ? latencies.get(n / 2) : 0;
        double p95Latency = n > 0 ? latencies.get((int) (n * 0.95)) : 0;
        double p99Latency = n > 0 ? latencies.get((int) (n * 0.99)) : 0;
        double qps = totalSec > 0 ? n / totalSec : 0;
        double zeroHitRate = (double) zeroHits / n;

        return new BenchmarkStats(
                meanRecall,
                p50Recall,
                p99Recall,
                p50Latency,
                p95Latency,
                p99Latency,
                qps,
                zeroHitRate);
    }

    private List<float[]> subSampleTrain(List<float[]> train, int limit) {
        if (limit <= 0 || limit >= train.size()) return train;
        return new ArrayList<>(train.subList(0, limit));
    }

    private List<float[]> subSampleQueries(List<float[]> queries, int limit) {
        if (limit <= 0 || limit >= queries.size()) return queries;
        return new ArrayList<>(queries.subList(0, limit));
    }

    private int[][] subSampleTruth(int[][] truth, int limit) {
        if (limit <= 0 || limit >= truth.length) return truth;
        int[][] out = new int[limit][];
        System.arraycopy(truth, 0, out, 0, limit);
        return out;
    }

    private long computeDirSize(Path dir) {
        try {
            return java.nio.file.Files.walk(dir)
                    .filter(java.nio.file.Files::isRegularFile)
                    .mapToLong(p -> p.toFile().length())
                    .sum();
        } catch (Exception e) {
            return -1;
        }
    }

    private static final class BenchmarkStats {
        final double meanRecall;
        final double p50Recall;
        final double p99Recall;
        final double p50LatencyMs;
        final double p95LatencyMs;
        final double p99LatencyMs;
        final double qps;
        final double zeroHitRate;

        BenchmarkStats(
                double meanRecall,
                double p50Recall,
                double p99Recall,
                double p50LatencyMs,
                double p95LatencyMs,
                double p99LatencyMs,
                double qps,
                double zeroHitRate) {
            this.meanRecall = meanRecall;
            this.p50Recall = p50Recall;
            this.p99Recall = p99Recall;
            this.p50LatencyMs = p50LatencyMs;
            this.p95LatencyMs = p95LatencyMs;
            this.p99LatencyMs = p99LatencyMs;
            this.qps = qps;
            this.zeroHitRate = zeroHitRate;
        }
    }
}
