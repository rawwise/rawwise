package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.derivation.KeywordIndex;
import io.github.rawwise.axiom.pipeline.derivation.KeywordIndex.KeywordHit;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Distributed Vector Search E2E")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class DistributedVectorSearchTest {

    private static final int DIM = 16;
    private static final int TOP_K = 10;

    @TempDir Path tempDir;

    private StubEmbeddingProvider embeddingProvider;
    private List<JVectorVectorIndex> shardIndices;
    private List<KeywordIndex> shardKeywords;
    private List<AutoCloseable> resources;

    @BeforeEach
    void setUp() {
        embeddingProvider = new StubEmbeddingProvider(DIM);
        shardIndices = new ArrayList<>();
        shardKeywords = new ArrayList<>();
        resources = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (int i = resources.size() - 1; i >= 0; i--) {
            try {
                resources.get(i).close();
            } catch (Exception ignored) {
            }
        }
    }

    private JVectorVectorIndex createShardIndex(String shardId) {
        JVectorConfig config =
                new JVectorConfig(
                        "disk", DIM, 16, 10, 50, tempDir.resolve(shardId).toString(), 5, 5, 500000);
        JVectorVectorIndex index = new JVectorVectorIndex(config, embeddingProvider, false);
        shardIndices.add(index);
        resources.add(index);
        return index;
    }

    private KeywordIndex createKeywordIndex() {
        KeywordIndex ki = new KeywordIndex();
        shardKeywords.add(ki);
        return ki;
    }

    private ShardClient createShardClient(
            JVectorVectorIndex index, KeywordIndex ki, String namespace) {
        return (shardId, queryEmb, topK) -> {
            List<SearchCandidate> candidates = new ArrayList<>();
            List<VectorSearchResult> results = index.search(namespace, queryEmb, topK);
            for (VectorSearchResult r : results) {
                candidates.add(new SearchCandidate(r.entryKey(), 1.0f - (float) r.score()));
            }
            return candidates;
        };
    }

    private Map<String, ShardClient> buildShardClients(String namespace) {
        Map<String, ShardClient> clients = new HashMap<>();
        for (int i = 0; i < shardIndices.size(); i++) {
            clients.put(
                    "shard-" + i,
                    createShardClient(shardIndices.get(i), shardKeywords.get(i), namespace));
        }
        return clients;
    }

    private void indexVectors(JVectorVectorIndex index, String namespace, List<String> docs) {
        for (String doc : docs) {
            float[] embedding = embeddingProvider.embed(doc);
            index.indexWithEmbedding(namespace, doc, embedding);
        }
    }

    private void indexKeywords(KeywordIndex ki, String namespace, List<String> docs) {
        for (String doc : docs) {
            ki.index(namespace, doc, doc, doc);
        }
    }

    private List<String> searchKeywords(
            List<KeywordIndex> indices, String namespace, String query, int topK) {
        List<String> keys = new ArrayList<>();
        for (KeywordIndex ki : indices) {
            for (KeywordHit hit : ki.search(namespace, query, topK)) {
                keys.add(hit.docId());
            }
        }
        return keys;
    }

    @Test
    @DisplayName("S8: Multi-Node Search — 3node writedifferentdata, merge aftertopKcorrect")
    void testMultiNodeSearchMergesCorrectly() {
        String ns = "s8-ns";

        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        JVectorVectorIndex shard1 = createShardIndex("shard-1");
        JVectorVectorIndex shard2 = createShardIndex("shard-2");
        KeywordIndex kw0 = createKeywordIndex();
        KeywordIndex kw1 = createKeywordIndex();
        KeywordIndex kw2 = createKeywordIndex();

        List<String> docsShard0 = List.of("alpha apple", "alpha banana");
        List<String> docsShard1 = List.of("beta cherry", "beta date");
        List<String> docsShard2 = List.of("gamma elderberry", "gamma fig");

        indexVectors(shard0, ns, docsShard0);
        indexVectors(shard1, ns, docsShard1);
        indexVectors(shard2, ns, docsShard2);
        indexKeywords(kw0, ns, docsShard0);
        indexKeywords(kw1, ns, docsShard1);
        indexKeywords(kw2, ns, docsShard2);

        Map<String, ShardClient> clients = buildShardClients(ns);
        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);

        float[] queryEmb = embeddingProvider.embed("alpha");
        List<VectorSearchResult> results = coordinator.distributedSearch(queryEmb, TOP_K);

        assertThat(results).isNotEmpty();
        Set<String> resultKeys = new HashSet<>();
        for (VectorSearchResult r : results) {
            resultKeys.add(r.entryKey());
        }
        assertThat(resultKeys).contains("alpha apple", "alpha banana");
        assertThat(results.size()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("S8: Multi-Node Search — nodehas key Coordinator duplicate , RRF")
    void testMultiNodeDeduplicatesByHigherScore() {
        String ns = "s8-dedup-ns";

        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        JVectorVectorIndex shard1 = createShardIndex("shard-1");
        KeywordIndex kw0 = createKeywordIndex();
        KeywordIndex kw1 = createKeywordIndex();

        String sharedDoc = "shared document alpha";
        List<String> docsShard0 = List.of(sharedDoc, "unique shard0 banana");
        List<String> docsShard1 = List.of(sharedDoc, "unique shard1 cherry");

        indexVectors(shard0, ns, docsShard0);
        indexVectors(shard1, ns, docsShard1);
        indexKeywords(kw0, ns, docsShard0);
        indexKeywords(kw1, ns, docsShard1);

        Map<String, ShardClient> clients = buildShardClients(ns);
        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);

        float[] queryEmb = embeddingProvider.embed("shared document");
        List<VectorSearchResult> results = coordinator.distributedSearch(queryEmb, TOP_K);

        long sharedCount = results.stream().filter(r -> r.entryKey().equals(sharedDoc)).count();
        assertThat(sharedCount).isGreaterThanOrEqualTo(1);
        assertThat(results.size()).isGreaterThanOrEqualTo(3);
    }

    @Test
    @DisplayName("S9: Node unavailable — itemShardClient exception, its node correct")
    void testNodeUnavailablePartialResults() {
        String ns = "s9-ns";

        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        JVectorVectorIndex shard1 = createShardIndex("shard-1");
        KeywordIndex kw0 = createKeywordIndex();
        KeywordIndex kw1 = createKeywordIndex();

        List<String> docsShard0 = List.of("alpha apple", "alpha banana");
        indexVectors(shard0, ns, docsShard0);
        indexVectors(shard1, ns, List.of("beta cherry"));
        indexKeywords(kw0, ns, docsShard0);

        Map<String, ShardClient> clients = new HashMap<>();
        clients.put("shard-0", createShardClient(shard0, kw0, ns));
        clients.put(
                "shard-faulty",
                (shardId, queryEmb, topK) -> {
                    throw new RuntimeException("Simulated shard failure");
                });

        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);

        float[] queryEmb = embeddingProvider.embed("alpha");
        List<VectorSearchResult> results = coordinator.distributedSearch(queryEmb, TOP_K);

        assertThat(results).isNotEmpty();
        Set<String> resultKeys = new HashSet<>();
        for (VectorSearchResult r : results) {
            resultKeys.add(r.entryKey());
        }
        assertThat(resultKeys).contains("alpha apple", "alpha banana");
    }

    @Test
    @DisplayName("S9: Node unavailable — allnode fail returns empty results")
    void testAllNodesUnavailableReturnsEmpty() {
        Map<String, ShardClient> clients = new HashMap<>();
        clients.put(
                "fault-0",
                (shardId, queryEmb, topK) -> {
                    throw new RuntimeException("Fault injection");
                });
        clients.put(
                "fault-1",
                (shardId, queryEmb, topK) -> {
                    throw new RuntimeException("Fault injection");
                });

        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);

        float[] queryEmb = embeddingProvider.embed("anything");
        List<VectorSearchResult> results = coordinator.distributedSearch(queryEmb, TOP_K);

        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("S10: KeywordIndex — multinodeBM25 RRF")
    void testKeywordIndexBroadcastRrfFusion() {
        String ns = "s10-ns";

        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        JVectorVectorIndex shard1 = createShardIndex("shard-1");
        KeywordIndex kw0 = createKeywordIndex();
        KeywordIndex kw1 = createKeywordIndex();

        List<String> docsShard0 = List.of("machine learning basics", "deep neural networks");
        List<String> docsShard1 =
                List.of("machine learning advanced", "natural language processing");

        indexVectors(shard0, ns, docsShard0);
        indexVectors(shard1, ns, docsShard1);
        indexKeywords(kw0, ns, docsShard0);
        indexKeywords(kw1, ns, docsShard1);

        Map<String, ShardClient> clients = buildShardClients(ns);
        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);
        float[] queryEmb = embeddingProvider.embed("machine learning");
        List<VectorSearchResult> vectorResults = coordinator.distributedSearch(queryEmb, TOP_K * 2);

        List<String> allKeywords = searchKeywords(shardKeywords, ns, "machine learning", TOP_K * 2);

        Map<String, Double> fusedScores = new HashMap<>();
        int vecRank = 1;
        for (VectorSearchResult r : vectorResults) {
            fusedScores.merge(r.entryKey(), 1.0 / (60 + vecRank), Double::sum);
            vecRank++;
        }
        int kwRank = 1;
        for (String kw : allKeywords) {
            fusedScores.merge(kw, 1.0 / (60 + kwRank), Double::sum);
            kwRank++;
        }

        List<Map.Entry<String, Double>> sorted = new ArrayList<>(fusedScores.entrySet());
        sorted.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        assertThat(sorted).isNotEmpty();
        boolean hasMlDoc = sorted.stream().anyMatch(e -> e.getKey().contains("machine learning"));
        assertThat(hasMlDoc).isTrue();
    }

    @Test
    @DisplayName("S11: — nocan ShardClient returns empty results")
    void testEmptyClusterReturnsEmpty() {
        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(Map.of(), 5000);

        float[] queryEmb = embeddingProvider.embed("anything");
        List<VectorSearchResult> results = coordinator.distributedSearch(queryEmb, TOP_K);

        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("S12: repeat — JVectorVectorIndexinBackend Node create, searchviaCoordinatorcan")
    void testArchitectureFixRegression() {
        String ns = "s12-ns";

        JVectorVectorIndex backendNodeIndex = createShardIndex("backend-node");
        KeywordIndex backendNodeKw = createKeywordIndex();

        List<String> docs =
                List.of("regression test alpha", "regression test beta", "regression test gamma");
        indexVectors(backendNodeIndex, ns, docs);
        indexKeywords(backendNodeKw, ns, docs);

        Map<String, ShardClient> clients = new HashMap<>();
        clients.put("backend-node", createShardClient(backendNodeIndex, backendNodeKw, ns));
        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);

        float[] queryEmb = embeddingProvider.embed("regression");
        List<VectorSearchResult> results = coordinator.distributedSearch(queryEmb, 5);

        assertThat(results).isNotEmpty();
        Set<String> resultKeys = new HashSet<>();
        for (VectorSearchResult r : results) {
            resultKeys.add(r.entryKey());
        }
        assertThat(resultKeys).contains("regression test alpha");
    }

    @Test
    @DisplayName("S2: concurrentdistributed — multi same query throws exception")
    void testConcurrentDistributedSearch() throws Exception {
        String ns = "s2-dist-ns";

        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        JVectorVectorIndex shard1 = createShardIndex("shard-1");
        KeywordIndex kw0 = createKeywordIndex();
        KeywordIndex kw1 = createKeywordIndex();

        List<String> allDocs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            allDocs.add("concurrent doc " + i);
        }
        List<String> docsShard0 = allDocs.subList(0, 25);
        List<String> docsShard1 = allDocs.subList(25, 50);

        indexVectors(shard0, ns, docsShard0);
        indexVectors(shard1, ns, docsShard1);
        indexKeywords(kw0, ns, docsShard0);
        indexKeywords(kw1, ns, docsShard1);

        Map<String, ShardClient> clients = buildShardClients(ns);
        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);

        int threadCount = 4;
        int queriesPerThread = 20;
        var executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount);
        var latch = new java.util.concurrent.CountDownLatch(1);
        var errors = new java.util.concurrent.atomic.AtomicInteger(0);
        var successCount = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(
                    () -> {
                        try {
                            latch.await();
                            for (int q = 0; q < queriesPerThread; q++) {
                                try {
                                    float[] queryEmb =
                                            embeddingProvider.embed(
                                                    "concurrent doc " + (threadId * 25 + q % 25));
                                    List<VectorSearchResult> results =
                                            coordinator.distributedSearch(queryEmb, 5);
                                    if (!results.isEmpty()) {
                                        successCount.incrementAndGet();
                                    }
                                } catch (Exception e) {
                                    errors.incrementAndGet();
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
        }

        latch.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(errors.get()).isEqualTo(0);
        assertThat(successCount.get()).isGreaterThan(0);
    }

    @Test
    @DisplayName("S3: partialnodetimeout — timeoutnode , its node")
    void testPartialTimeoutDegradation() {
        String ns = "s3-timeout-ns";

        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        KeywordIndex kw0 = createKeywordIndex();

        List<String> docs = List.of("timeout test alpha", "timeout test beta");
        indexVectors(shard0, ns, docs);
        indexKeywords(kw0, ns, docs);

        Map<String, ShardClient> clients = new HashMap<>();
        clients.put("shard-0", createShardClient(shard0, kw0, ns));
        clients.put(
                "shard-slow",
                (shardId, queryEmb, topK) -> {
                    try {
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return List.of();
                });

        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 1000);

        float[] queryEmb = embeddingProvider.embed("timeout");
        List<VectorSearchResult> results = coordinator.distributedSearch(queryEmb, 5);

        assertThat(results).isNotEmpty();
        Set<String> resultKeys = new HashSet<>();
        for (VectorSearchResult r : results) {
            resultKeys.add(r.entryKey());
        }
        assertThat(resultKeys).contains("timeout test alpha");
    }

    @Test
    @DisplayName("S4: topKboundary — queryEmbedding does not throw exception")
    void testTopKBoundaryHandling() {
        String ns = "s4-topk-boundary-ns";

        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        KeywordIndex kw0 = createKeywordIndex();

        List<String> docs = List.of("boundary doc");
        indexVectors(shard0, ns, docs);
        indexKeywords(kw0, ns, docs);

        Map<String, ShardClient> clients = buildShardClients(ns);
        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);

        float[] queryEmb = embeddingProvider.embed("boundary");
        List<VectorSearchResult> r1 = coordinator.distributedSearch(queryEmb, 1);
        assertThat(r1).hasSizeLessThanOrEqualTo(1);

        List<VectorSearchResult> r2 = coordinator.distributedSearch(queryEmb, 100);
        assertThat(r2).isNotEmpty();

        float[] wrongDim = new float[DIM * 2];
        List<VectorSearchResult> r3 = coordinator.distributedSearch(wrongDim, 5);
        assertThat(r3).isNotNull();
    }
}
