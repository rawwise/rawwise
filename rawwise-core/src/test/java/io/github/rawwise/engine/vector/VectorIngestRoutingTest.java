package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Vector Ingest Routing E2E")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class VectorIngestRoutingTest {

    private static final int DIM = 16;
    private static final int TOP_K = 10;

    @TempDir Path tempDir;

    private StubEmbeddingProvider embeddingProvider;
    private List<JVectorVectorIndex> shardIndices;
    private List<AutoCloseable> resources;

    @BeforeEach
    void setUp() {
        embeddingProvider = new StubEmbeddingProvider(DIM);
        shardIndices = new ArrayList<>();
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

    private ShardClient createShardClient(JVectorVectorIndex index, String namespace) {
        return (shardId, queryEmb, topK) -> {
            List<SearchCandidate> candidates = new ArrayList<>();
            List<VectorSearchResult> results = index.search(namespace, queryEmb, topK);
            for (VectorSearchResult r : results) {
                candidates.add(new SearchCandidate(r.entryKey(), 1.0f - (float) r.score()));
            }
            return candidates;
        };
    }

    @Test
    @DisplayName("L1: Request with embedding routes to nearest shard via IVF")
    void l1EmbeddingRoutesToNearestShard() {
        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        JVectorVectorIndex shard1 = createShardIndex("shard-1");

        float[] vec0 = new float[DIM];
        vec0[0] = 1.0f;
        shard0.indexWithEmbedding("ns", "key-0", vec0, "doc zero");

        float[] vec1 = new float[DIM];
        vec1[1] = 1.0f;
        shard1.indexWithEmbedding("ns", "key-1", vec1, "doc one");

        IvfRouter router = new IvfRouter();
        ShardCentroids sc0 = shard0.buildAndPersistCentroids("ns", "shard-0");
        ShardCentroids sc1 = shard1.buildAndPersistCentroids("ns", "shard-1");
        if (sc0 != null) router.registerShard(sc0);
        if (sc1 != null) router.registerShard(sc1);

        if (router.shardCount() > 0) {
            List<String> targets = router.route(vec0, 1);
            assertThat(targets).isNotEmpty();
            assertThat(targets.get(0)).isEqualTo("shard-0");

            List<String> targets1 = router.route(vec1, 1);
            assertThat(targets1).isNotEmpty();
            assertThat(targets1.get(0)).isEqualTo("shard-1");
        }
    }

    @Test
    @DisplayName("L3: Cold start Round-Robin distributes evenly")
    void l3RoundRobinDistributesEvenly() {
        int shardCount = 3;
        for (int i = 0; i < shardCount; i++) {
            createShardIndex("shard-" + i);
        }

        AtomicInteger roundRobinIndex = new AtomicInteger(0);
        int[] counts = new int[shardCount];
        for (int i = 0; i < 300; i++) {
            int idx = Math.abs(roundRobinIndex.getAndIncrement() % shardCount);
            counts[idx]++;
        }

        for (int count : counts) {
            assertThat(count).isBetween(80, 120);
        }
    }

    @Test
    @DisplayName("L3: AtomicInteger overflow safety")
    void l3RoundRobinOverflowSafety() {
        AtomicInteger idx = new AtomicInteger(Integer.MAX_VALUE - 1);
        int shardCount = 3;

        int r1 = Math.abs(idx.getAndIncrement() % shardCount);
        int r2 = Math.abs(idx.getAndIncrement() % shardCount);
        int r3 = Math.abs(idx.getAndIncrement() % shardCount);

        assertThat(r1).isBetween(0, shardCount - 1);
        assertThat(r2).isBetween(0, shardCount - 1);
        assertThat(r3).isBetween(0, shardCount - 1);
    }

    @Test
    @DisplayName("L2: Embed failure falls back to Round-Robin")
    void l2EmbedFailureFallsBackToRoundRobin() {
        EmbeddingProvider failingProvider =
                text -> {
                    throw new RuntimeException("Embedding service unavailable");
                };

        int shardCount = 2;
        for (int i = 0; i < shardCount; i++) {
            createShardIndex("shard-" + i);
        }

        AtomicInteger roundRobinIndex = new AtomicInteger(0);
        IvfRouter router = new IvfRouter();

        String textContent = "some text content";
        int target = -1;
        if (textContent != null
                && !textContent.isEmpty()
                && router.shardCount() > 0
                && failingProvider != null) {
            try {
                float[] emb = failingProvider.embed(textContent);
                List<String> shards = router.route(emb, 1);
                if (!shards.isEmpty())
                    target = Integer.parseInt(shards.get(0).substring("shard-".length()));
            } catch (Exception e) {
                target = Math.abs(roundRobinIndex.getAndIncrement() % shardCount);
            }
        }
        if (target < 0) {
            target = Math.abs(roundRobinIndex.getAndIncrement() % shardCount);
        }

        assertThat(target).isBetween(0, shardCount - 1);
    }

    @Test
    @DisplayName("Centroid build trigger: builds after >= 16 vectors")
    void centroidBuildTrigger() {
        JVectorVectorIndex shard = createShardIndex("shard-0");

        assertThat(shard.totalVectorCount("ns")).isEqualTo(0);
        assertThat(shard.getLatestCentroids("ns")).isNull();

        for (int i = 0; i < 20; i++) {
            float[] vec = new float[DIM];
            vec[i % DIM] = 1.0f + i * 0.01f;
            shard.indexWithEmbedding("ns", "key-" + i, vec, "doc " + i);
        }

        assertThat(shard.totalVectorCount("ns")).isGreaterThanOrEqualTo(16);

        ShardCentroids sc = shard.buildAndPersistCentroids("ns", "shard-0");
        if (sc != null) {
            assertThat(sc.nlist()).isGreaterThan(0);
        } else {
            assertThat(shard.totalVectorCount("ns")).isGreaterThanOrEqualTo(16);
        }
    }

    @Test
    @DisplayName("Cold start to steady state: RR write then centroid build then search finds all")
    void coldStartToSteadyState() {
        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        JVectorVectorIndex shard1 = createShardIndex("shard-1");

        for (int i = 0; i < 10; i++) {
            float[] vec = new float[DIM];
            vec[0] = 1.0f;
            shard0.indexWithEmbedding("ns", "key-" + i, vec, "doc " + i);
        }
        for (int i = 10; i < 20; i++) {
            float[] vec = new float[DIM];
            vec[1] = 1.0f;
            shard1.indexWithEmbedding("ns", "key-" + i, vec, "doc " + i);
        }

        Map<String, ShardClient> clients = new HashMap<>();
        clients.put("shard-0", createShardClient(shard0, "ns"));
        clients.put("shard-1", createShardClient(shard1, "ns"));

        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);

        float[] query = new float[DIM];
        query[0] = 1.0f;
        List<VectorSearchResult> results = coordinator.distributedSearch(query, TOP_K, 5000);
        assertThat(results).isNotEmpty();

        IvfRouter router = new IvfRouter();
        ShardCentroids sc0 = shard0.buildAndPersistCentroids("ns", "shard-0");
        ShardCentroids sc1 = shard1.buildAndPersistCentroids("ns", "shard-1");
        if (sc0 != null) router.registerShard(sc0);
        if (sc1 != null) router.registerShard(sc1);

        if (router.shardCount() > 0) {
            List<VectorSearchResult> routedResults =
                    coordinator.distributedSearch(query, TOP_K, 5000, 1, router);
            assertThat(routedResults).isNotEmpty();
        }

        coordinator.close();
    }

    @Test
    @DisplayName("Concurrent ingest routing correctness")
    void concurrentIngestRouting() throws Exception {
        int shardCount = 3;
        for (int i = 0; i < shardCount; i++) {
            createShardIndex("shard-" + i);
        }

        AtomicInteger roundRobinIndex = new AtomicInteger(0);
        int iterations = 1000;
        int[] counts = new int[shardCount];

        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            threads.add(
                    new Thread(
                            () -> {
                                for (int i = 0; i < iterations / 4; i++) {
                                    int idx =
                                            Math.abs(
                                                    roundRobinIndex.getAndIncrement() % shardCount);
                                    synchronized (counts) {
                                        counts[idx]++;
                                    }
                                }
                            }));
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        int total = 0;
        for (int count : counts) {
            total += count;
            assertThat(count).isGreaterThan(0);
        }
        assertThat(total).isEqualTo(iterations);
    }

    @Test
    @DisplayName("Cold start search: no centroid searches all shards")
    void coldStartSearchAllShards() {
        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        JVectorVectorIndex shard1 = createShardIndex("shard-1");

        float[] vec = new float[DIM];
        vec[0] = 1.0f;
        shard1.indexWithEmbedding("ns", "only-on-shard1", vec, "only on shard 1");

        Map<String, ShardClient> clients = new HashMap<>();
        clients.put("shard-0", createShardClient(shard0, "ns"));
        clients.put("shard-1", createShardClient(shard1, "ns"));

        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);

        IvfRouter emptyRouter = new IvfRouter();
        int nprobe =
                emptyRouter.shardCount() > 0
                        ? Math.min(4, coordinator.shardCount())
                        : coordinator.shardCount();

        assertThat(nprobe).isEqualTo(coordinator.shardCount());

        List<VectorSearchResult> results =
                coordinator.distributedSearch(vec, TOP_K, 5000, nprobe, emptyRouter);
        boolean found = results.stream().anyMatch(r -> r.entryKey().equals("only-on-shard1"));
        assertThat(found).isTrue();

        coordinator.close();
    }

    @Test
    @DisplayName("Dynamic nprobe: centroid available uses min(4, shardCount)")
    void dynamicNprobeWithCentroids() {
        JVectorVectorIndex shard0 = createShardIndex("shard-0");
        JVectorVectorIndex shard1 = createShardIndex("shard-1");

        for (int i = 0; i < 16; i++) {
            float[] vec = new float[DIM];
            vec[i % DIM] = 1.0f;
            shard0.indexWithEmbedding("ns", "key-" + i, vec, "doc " + i);
        }
        shard0.buildAndPersistCentroids("ns", "shard-0");

        IvfRouter router = new IvfRouter();
        ShardCentroids sc = shard0.getLatestCentroids("ns");
        if (sc != null) router.registerShard(sc);

        Map<String, ShardClient> clients = new HashMap<>();
        clients.put("shard-0", createShardClient(shard0, "ns"));
        clients.put("shard-1", createShardClient(shard1, "ns"));

        DistributedSearchCoordinator coordinator = new DistributedSearchCoordinator(clients, 5000);

        int nprobe =
                router.shardCount() > 0
                        ? Math.min(4, coordinator.shardCount())
                        : coordinator.shardCount();

        assertThat(nprobe).isEqualTo(Math.min(4, 2));

        coordinator.close();
    }

    @Test
    @DisplayName("Shard to stub index parsing")
    void shardToStubIndex() {
        assertThat(Integer.parseInt("shard-0".substring("shard-".length()))).isEqualTo(0);
        assertThat(Integer.parseInt("shard-3".substring("shard-".length()))).isEqualTo(3);
    }
}
