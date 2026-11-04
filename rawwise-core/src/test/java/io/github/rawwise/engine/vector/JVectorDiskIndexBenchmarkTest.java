package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("JVectorDiskIndex Performance Benchmark")
class JVectorDiskIndexBenchmarkTest {

    private static final int DIM = 128;
    private static final int GRAPH_DEGREE = 32;
    private static final int SEARCH_TOP_K = 10;
    private static final int QUERY_COUNT = 50;

    @TempDir Path tempDir;

    private StubEmbeddingProvider embedder;
    private final List<BenchmarkResult> results = new ArrayList<>();

    @BeforeEach
    void setUp() {
        embedder = new StubEmbeddingProvider(DIM);
    }

    @AfterEach
    void tearDown() {
        System.out.println("\n" + "=".repeat(90));
        System.out.printf(
                "%-50s %10s %10s %10s %10s %10s %10s\n",
                "Test", "Scale", "Build(s)", "Inserts/s", "Search(ms)", "P99(ms)", "Recall");
        System.out.println("-".repeat(90));
        for (BenchmarkResult r : results) {
            System.out.printf(
                    "%-50s %10d %10.1f %10.0f %10.2f %10.2f %10.2f\n",
                    r.name,
                    r.scale,
                    r.buildSec,
                    r.insertsPerSec,
                    r.searchAvgMs,
                    r.searchP99Ms,
                    r.recall);
        }
        System.out.println("=".repeat(90));
    }

    // ==================== JVectorDiskIndex directly test ====================

    @Test
    @DisplayName("B1: JVectorDiskIndex direct - 10K vectors")
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void testJVectorDiskIndex10K() {
        benchmarkJVectorDiskIndexDirect(10_000, "B1: JVectorDiskIndex-10K");
    }

    @Test
    @DisplayName("B2: JVectorDiskIndex direct - 50K vectors")
    @Timeout(value = 600, unit = TimeUnit.SECONDS)
    void testJVectorDiskIndex50K() {
        benchmarkJVectorDiskIndexDirect(50_000, "B2: JVectorDiskIndex-50K");
    }

    @Test
    @DisplayName("B3: JVectorDiskIndex direct - 100K vectors")
    @Timeout(value = 900, unit = TimeUnit.SECONDS)
    void testJVectorDiskIndex100K() {
        benchmarkJVectorDiskIndexDirect(100_000, "B3: JVectorDiskIndex-100K");
    }

    // ==================== JVectorVectorIndex Full-mode test ====================

    @Test
    @DisplayName("B4: JVectorVectorIndex Full-mode - 10K vectors")
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void testJVectorVectorIndex10K() {
        benchmarkJVectorVectorIndex(10_000, "B4: JVectorVectorIndex-10K");
    }

    @Test
    @DisplayName("B5: JVectorVectorIndex Full-mode - 50K vectors")
    @Timeout(value = 600, unit = TimeUnit.SECONDS)
    void testJVectorVectorIndex50K() {
        benchmarkJVectorVectorIndex(50_000, "B5: JVectorVectorIndex-50K");
    }

    @Test
    @DisplayName("B6: JVectorVectorIndex Full-mode - 100K vectors")
    @Timeout(value = 900, unit = TimeUnit.SECONDS)
    void testJVectorVectorIndex100K() {
        benchmarkJVectorVectorIndex(100_000, "B6: JVectorVectorIndex-100K");
    }

    // ==================== ====================

    @Test
    @DisplayName("B7: Incremental insert after 10K build")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testIncrementalInsertPerformance() {
        int initialCount = 10_000;
        int insertCount = 1_000;

        Path indexPath = tempDir.resolve("incr-test");
        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        DIM,
                        GRAPH_DEGREE,
                        10,
                        50,
                        indexPath.toString(),
                        100000,
                        100000,
                        500000);

        List<float[]> initialVectors = generateVectors(initialCount);
        List<String> initialKeys = generateKeys(0, initialCount);
        long buildStart = System.nanoTime();
        JVectorDiskIndex index = new JVectorDiskIndex(config, indexPath);
        index.initialize(initialVectors, initialKeys);
        long buildEnd = System.nanoTime();

        List<float[]> insertVectors = generateVectors(insertCount);
        long insertStart = System.nanoTime();
        for (int i = 0; i < insertCount; i++) {
            index.insert("insert-" + i, insertVectors.get(i));
        }
        long insertEnd = System.nanoTime();

        long diskSize = computeDirSize(indexPath);
        double insertsPerSec = insertCount / ((insertEnd - insertStart) / 1_000_000_000.0);

        System.out.printf("\n--- Incremental Insert ---\n");
        System.out.printf("  Initial build: %.1fs\n", (buildEnd - buildStart) / 1_000_000_000.0);
        System.out.printf("  Insert %d: %.1f inserts/s\n", insertCount, insertsPerSec);
        System.out.printf("  Disk size: %.1f MB\n", diskSize / 1_000_000.0);

        index.close();
        assertThat(insertsPerSec).isGreaterThan(0);
    }

    // ==================== persistedrecoverytest ====================

    @Test
    @DisplayName("B8: Persist -> close -> new instance auto-reloads from disk")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testDiskRecoveryAfterClose() {
        int count = 10_000;
        Path indexPath = tempDir.resolve("recovery-test");
        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        DIM,
                        GRAPH_DEGREE,
                        10,
                        50,
                        indexPath.toString(),
                        100000,
                        100000,
                        500000);

        float[] query = embedder.embed("query-doc-0");

        JVectorDiskIndex index = new JVectorDiskIndex(config, indexPath);
        index.initialize(generateVectors(count), generateKeys(0, count));
        List<SearchCandidate> resultsBefore = index.search(query, SEARCH_TOP_K);
        int countBefore = index.vectorCount();
        index.close();

        JVectorDiskIndex reloaded = new JVectorDiskIndex(config, indexPath);
        try {
            assertThat(reloaded.isInitialized()).isTrue();
            assertThat(reloaded.vectorCount()).isEqualTo(countBefore);
            List<SearchCandidate> resultsAfter = reloaded.search(query, SEARCH_TOP_K);

            assertThat(resultsAfter).isNotEmpty();
            assertThat(resultsAfter.size()).isEqualTo(resultsBefore.size());
            assertThat(resultsAfter.get(0).entryKey()).isEqualTo(resultsBefore.get(0).entryKey());

            long diskSize = computeDirSize(indexPath);
            System.out.printf("\n--- Disk Recovery ---\n");
            System.out.printf("  Disk size: %.1f MB\n", diskSize / 1_000_000.0);
            System.out.printf(
                    "  Vectors before close: %d, after reload: %d\n",
                    countBefore, reloaded.vectorCount());
            System.out.printf(
                    "  Top result before: %s, after: %s (score: %.4f → %.4f)\n",
                    resultsBefore.get(0).entryKey(),
                    resultsAfter.get(0).entryKey(),
                    resultsBefore.get(0).approximateDistance(),
                    resultsAfter.get(0).approximateDistance());
        } finally {
            reloaded.close();
        }
    }

    @Test
    @DisplayName("B9: Search cache — first call loads disk, subsequent calls hit cache")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testSearchCachePerformance() {
        int count = 10_000;
        Path indexPath = tempDir.resolve("cache-test");
        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        DIM,
                        GRAPH_DEGREE,
                        10,
                        50,
                        indexPath.toString(),
                        100000,
                        100000,
                        500000);

        float[] query = embedder.embed("query-doc-0");

        JVectorDiskIndex index = new JVectorDiskIndex(config, indexPath);
        index.initialize(generateVectors(count), generateKeys(0, count));

        long t0 = System.nanoTime();
        index.search(query, SEARCH_TOP_K);
        long firstCallMs = (System.nanoTime() - t0) / 1_000_000L;

        long t1 = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            index.search(query, SEARCH_TOP_K);
        }
        long avgCachedMs = (System.nanoTime() - t1) / 1_000_000L / 100;

        System.out.printf("\n--- Search Cache ---\n");
        System.out.printf("  First call (disk load): %dms\n", firstCallMs);
        System.out.printf("  Avg cached call: %dms\n", avgCachedMs);
        System.out.printf("  Speedup: %.1fx\n", (double) firstCallMs / avgCachedMs);

        index.close();
        assertThat(avgCachedMs).isLessThan(firstCallMs * 2);
    }

    // ==================== internal ====================

    private void benchmarkJVectorDiskIndexDirect(int count, String name) {
        Path indexPath = tempDir.resolve("jdvi-" + count);
        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        DIM,
                        GRAPH_DEGREE,
                        10,
                        50,
                        indexPath.toString(),
                        100000,
                        100000,
                        500000);

        List<float[]> vectors = generateVectors(count);
        List<String> keys = generateKeys(0, count);
        List<float[]> queries = generateQueries(vectors);

        JVectorDiskIndex index = new JVectorDiskIndex(config, indexPath);

        long buildStart = System.nanoTime();
        index.initialize(vectors, keys);
        long buildEnd = System.nanoTime();

        long diskSize = computeDirSize(indexPath);
        RecallStats stats = measureRecallAndLatency(index, queries, vectors);

        long buildSecNanos = buildEnd - buildStart;
        results.add(
                new BenchmarkResult(
                        name,
                        count,
                        buildSecNanos / 1_000_000_000.0,
                        0,
                        stats.searchAvgMs,
                        stats.searchP99Ms,
                        stats.recall));

        System.out.printf("  Disk size: %.1f MB\n", diskSize / 1_000_000.0);
        System.out.printf(
                "  Search calls: %d, Avg: %.2fms, P50: %.2fms, P99: %.2fms\n",
                stats.searchCount, stats.searchAvgMs, stats.searchP50Ms, stats.searchP99Ms);
        System.out.printf("  Hits@10: %.2f, R-Precision: %.2f\n", stats.hitsAt10, stats.rPrecision);

        index.close();
        assertThat(stats.recall).isGreaterThan(0.0);
    }

    private void benchmarkJVectorVectorIndex(int count, String name) {
        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        DIM,
                        GRAPH_DEGREE,
                        10,
                        50,
                        tempDir.resolve("lmdb-" + count).toString(),
                        count,
                        count,
                        count * 2);
        JVectorVectorIndex index = new JVectorVectorIndex(config, embedder, false);

        List<float[]> vectors = generateVectors(count);
        List<float[]> queries = generateQueries(vectors);

        long buildStart = System.nanoTime();
        for (int i = 0; i < count; i++) {
            index.indexWithEmbedding("bench-ns-" + count, "key-" + i, vectors.get(i));
        }
        long buildEnd = System.nanoTime();

        long diskSize = computeDirSize(tempDir.resolve("lmdb-" + count));

        List<Long> latencies = new ArrayList<>();
        int validQueries = 0;
        for (int qi = 0; qi < queries.size(); qi++) {
            long t0 = System.nanoTime();
            List<VectorSearchResult> results =
                    index.search("bench-ns-" + count, queries.get(qi), SEARCH_TOP_K);
            long t1 = System.nanoTime();
            if (!results.isEmpty()) {
                latencies.add((t1 - t0) / 1_000_000L);
                validQueries++;
            }
        }

        latencies.sort(Long::compare);
        int n = latencies.size();
        double avgMs = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        double p50Ms = n > 0 ? latencies.get(n / 2) : 0;
        double p99Ms = n > 0 ? latencies.get((int) (n * 0.99)) : 0;

        double recallAvg = computeRecallForLmdb(index, queries, vectors, "bench-ns-" + count);

        results.add(
                new BenchmarkResult(
                        name,
                        count,
                        (buildEnd - buildStart) / 1_000_000_000.0,
                        0,
                        avgMs,
                        p99Ms,
                        recallAvg));

        System.out.printf("  Disk size: %.1f MB\n", diskSize / 1_000_000.0);
        System.out.printf(
                "  Search calls: %d, Avg: %.2fms, P50: %.2fms, P99: %.2fms\n",
                validQueries, avgMs, p50Ms, p99Ms);
        System.out.printf("  Recall@10 (vs brute-force): %.2f\n", recallAvg);

        index.close();
        assertThat(recallAvg).isGreaterThan(0.0);
    }

    private double computeRecallForLmdb(
            JVectorVectorIndex index,
            List<float[]> queries,
            List<float[]> allVectors,
            String namespace) {
        int topK = SEARCH_TOP_K;
        int recallSum = 0;
        int total = 0;

        for (float[] query : queries) {
            List<VectorSearchResult> results = index.search(namespace, query, topK);
            if (results.isEmpty()) continue;

            List<GroundTruthEntry> gt = computeGroundTruth(query, allVectors);

            int matchCount = 0;
            for (VectorSearchResult r : results) {
                for (int j = 0; j < topK; j++) {
                    if (gt.get(j).key.equals(r.entryKey())) {
                        matchCount++;
                        break;
                    }
                }
            }
            recallSum += matchCount;
            total++;
        }

        return total > 0 ? (double) recallSum / (total * topK) : 0;
    }

    private RecallStats measureRecallAndLatency(
            JVectorDiskIndex index, List<float[]> queries, List<float[]> gtSubset) {
        List<Long> latencies = new ArrayList<>();
        double totalRecall = 0;
        int searchCount = 0;
        double totalHitsAt10 = 0;
        double totalRPrecision = 0;

        for (float[] query : queries) {
            long t0 = System.nanoTime();
            List<SearchCandidate> candidates = index.search(query, SEARCH_TOP_K);
            long t1 = System.nanoTime();

            if (candidates.isEmpty()) continue;

            latencies.add((t1 - t0) / 1_000_000L);
            searchCount++;

            List<GroundTruthEntry> gt = computeGroundTruth(query, gtSubset);

            int hits = 0;
            for (SearchCandidate c : candidates) {
                for (int j = 0; j < SEARCH_TOP_K; j++) {
                    if (gt.get(j).key.equals(c.entryKey())) {
                        hits++;
                        break;
                    }
                }
            }
            totalRecall += (double) hits / SEARCH_TOP_K;
            totalHitsAt10 += hits;

            int rPrecisionHits = 0;
            for (int k = 0; k < SEARCH_TOP_K; k++) {
                for (int j = 0; j < SEARCH_TOP_K; j++) {
                    if (gt.get(j).key.equals(candidates.get(k).entryKey())) {
                        rPrecisionHits++;
                        break;
                    }
                }
            }
            totalRPrecision += (double) rPrecisionHits / SEARCH_TOP_K;
        }

        latencies.sort(Long::compare);
        int n = latencies.size();
        return new RecallStats(
                searchCount,
                latencies.stream().mapToLong(Long::longValue).average().orElse(0),
                n > 0 ? latencies.get(n / 2) : 0,
                n > 0 ? latencies.get((int) (n * 0.99)) : 0,
                0,
                searchCount > 0 ? totalRecall / searchCount : 0,
                searchCount > 0 ? totalHitsAt10 / searchCount : 0,
                searchCount > 0 ? totalRPrecision / searchCount : 0);
    }

    private List<GroundTruthEntry> computeGroundTruth(float[] query, List<float[]> vectors) {
        List<GroundTruthEntry> entries = new ArrayList<>(vectors.size());
        for (int i = 0; i < vectors.size(); i++) {
            float sim = cosineSimilarity(query, vectors.get(i));
            entries.add(new GroundTruthEntry("key-" + i, sim));
        }
        entries.sort(Comparator.comparingDouble(e -> -e.score));
        return entries;
    }

    private List<float[]> generateVectors(int count) {
        List<float[]> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            vectors.add(embedder.embed("doc-" + i));
        }
        return vectors;
    }

    private List<String> generateKeys(int start, int count) {
        return IntStream.range(start, start + count).mapToObj(i -> "key-" + i).toList();
    }

    private List<float[]> generateQueries(List<float[]> fromVectors) {
        List<float[]> queries = new ArrayList<>(QUERY_COUNT);
        for (int i = 0; i < QUERY_COUNT; i++) {
            float[] base = fromVectors.get((i * 113) % fromVectors.size());
            float[] perturbed = new float[DIM];
            for (int d = 0; d < DIM; d++) {
                perturbed[d] = base[d] + (float) ((Math.random() - 0.5) * 0.02);
            }
            normalize(perturbed);
            queries.add(perturbed);
        }
        return queries;
    }

    private static void normalize(float[] v) {
        double sumSq = 0;
        for (float x : v) sumSq += x * x;
        float norm = (float) Math.sqrt(sumSq);
        if (norm > 0) {
            for (int i = 0; i < v.length; i++) v[i] /= norm;
        }
    }

    private long computeDirSize(Path dir) {
        try {
            return Files.walk(dir)
                    .filter(Files::isRegularFile)
                    .mapToLong(p -> p.toFile().length())
                    .sum();
        } catch (Exception e) {
            return -1;
        }
    }

    private static float cosineSimilarity(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0 || normB == 0) return 0;
        return dot / (float) (Math.sqrt(normA) * Math.sqrt(normB));
    }

    // ==================== data ====================

    record BenchmarkResult(
            String name,
            int scale,
            double buildSec,
            double insertsPerSec,
            double searchAvgMs,
            double searchP99Ms,
            double recall) {}

    record GroundTruthEntry(String key, float score) {}

    static class RecallStats {
        final int searchCount;
        final double searchAvgMs;
        final double searchP50Ms;
        final double searchP99Ms;
        final double insertsPerSec;
        final double recall;
        final double hitsAt10;
        final double rPrecision;

        RecallStats(
                int searchCount,
                double searchAvgMs,
                double searchP50Ms,
                double searchP99Ms,
                double insertsPerSec,
                double recall,
                double hitsAt10,
                double rPrecision) {
            this.searchCount = searchCount;
            this.searchAvgMs = searchAvgMs;
            this.searchP50Ms = searchP50Ms;
            this.searchP99Ms = searchP99Ms;
            this.insertsPerSec = insertsPerSec;
            this.recall = recall;
            this.hitsAt10 = hitsAt10;
            this.rPrecision = rPrecision;
        }
    }
}
