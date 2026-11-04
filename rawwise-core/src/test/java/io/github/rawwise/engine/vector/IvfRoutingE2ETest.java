package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("IVF Routing E2E - Shardroutingfull chaintest")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class IvfRoutingE2ETest {

    private static final int DIM = 16;

    @TempDir Path tempDir;

    private StubEmbeddingProvider embeddingProvider;
    private IvfRouter router;
    private List<JVectorVectorIndex> shardIndices;

    @BeforeEach
    void setUp() {
        embeddingProvider = new StubEmbeddingProvider(DIM);
        router = new IvfRouter();
        shardIndices = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (JVectorVectorIndex idx : shardIndices) {
            try {
                idx.close();
            } catch (Exception e) {
                /* ignore */
            }
        }
    }

    private JVectorVectorIndex createShardIndex(String shardId) {
        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        DIM,
                        16,
                        10,
                        50,
                        tempDir.resolve("shard-" + shardId).toString(),
                        5,
                        5,
                        500000);
        JVectorVectorIndex idx = new JVectorVectorIndex(config, embeddingProvider, false);
        shardIndices.add(idx);
        return idx;
    }

    private float[][] generateCentroids(int nlist, long seed) {
        Random rng = new Random(seed);
        float[][] centroids = new float[nlist][DIM];
        for (int i = 0; i < nlist; i++) {
            for (int d = 0; d < DIM; d++) {
                centroids[i][d] = rng.nextFloat();
            }
        }
        return centroids;
    }

    @Test
    @DisplayName(
            "S5: Shard Routing - write to multiple shards, route query to nearest shard, verify search results")
    void testShardRoutingE2E() {
        float[][] centroids1 = generateCentroids(4, 1L);
        float[][] centroids2 = generateCentroids(4, 2L);
        float[][] centroids3 = generateCentroids(4, 3L);

        router.registerShard(
                new ShardCentroids("shard-1", 4, centroids1, 1000, 1, System.currentTimeMillis()));
        router.registerShard(
                new ShardCentroids("shard-2", 4, centroids2, 2000, 1, System.currentTimeMillis()));
        router.registerShard(
                new ShardCentroids("shard-3", 4, centroids3, 3000, 1, System.currentTimeMillis()));

        assertThat(router.shardCount()).isEqualTo(3);

        JVectorVectorIndex shard1 = createShardIndex("1");
        JVectorVectorIndex shard2 = createShardIndex("2");
        JVectorVectorIndex shard3 = createShardIndex("3");

        for (int i = 0; i < 20; i++) {
            float[] embedding = embeddingProvider.embed("shard1 doc " + i);
            shard1.indexWithEmbedding("ns", "s1-key-" + i, embedding);
        }
        for (int i = 0; i < 20; i++) {
            float[] embedding = embeddingProvider.embed("shard2 doc " + i);
            shard2.indexWithEmbedding("ns", "s2-key-" + i, embedding);
        }
        for (int i = 0; i < 20; i++) {
            float[] embedding = embeddingProvider.embed("shard3 doc " + i);
            shard3.indexWithEmbedding("ns", "s3-key-" + i, embedding);
        }

        float[] query = centroids1[0];
        List<String> targetShards = router.route(query, 10, 2);
        assertThat(targetShards).hasSize(2);
        assertThat(targetShards).contains("shard-1");

        List<VectorSearchResult> allResults = new ArrayList<>();
        for (String shardId : targetShards) {
            JVectorVectorIndex shardIdx =
                    switch (shardId) {
                        case "shard-1" -> shard1;
                        case "shard-2" -> shard2;
                        case "shard-3" -> shard3;
                        default -> throw new IllegalArgumentException("Unknown shard: " + shardId);
                    };
            allResults.addAll(shardIdx.search("ns", query, 10));
        }
        assertThat(allResults).isNotEmpty();
    }

    @Test
    @DisplayName("S6: Shard Merge - merge two shards, verify routing and search on merged shard")
    void testShardMergeE2E() {
        float[][] centroids1 = generateCentroids(4, 10L);
        float[][] centroids2 = generateCentroids(4, 20L);

        router.registerShard(
                new ShardCentroids("shard-a", 4, centroids1, 500, 1, System.currentTimeMillis()));
        router.registerShard(
                new ShardCentroids("shard-b", 4, centroids2, 800, 1, System.currentTimeMillis()));
        assertThat(router.shardCount()).isEqualTo(2);

        JVectorVectorIndex shardA = createShardIndex("a");
        JVectorVectorIndex shardB = createShardIndex("b");

        for (int i = 0; i < 20; i++) {
            shardA.indexWithEmbedding(
                    "ns", "a-key-" + i, embeddingProvider.embed("shardA doc " + i));
            shardB.indexWithEmbedding(
                    "ns", "b-key-" + i, embeddingProvider.embed("shardB doc " + i));
        }

        List<String> removed = router.mergeShards(List.of("shard-a", "shard-b"), "shard-merged");
        assertThat(removed).containsExactly("shard-a", "shard-b");
        assertThat(router.shardCount()).isEqualTo(1);

        ShardCentroids merged = router.getShardCentroids("shard-merged");
        assertThat(merged).isNotNull();
        assertThat(merged.vectorCount()).isEqualTo(1300);

        JVectorVectorIndex mergedShard = createShardIndex("merged");
        for (int i = 0; i < 20; i++) {
            mergedShard.indexWithEmbedding(
                    "ns", "a-key-" + i, embeddingProvider.embed("shardA doc " + i));
            mergedShard.indexWithEmbedding(
                    "ns", "b-key-" + i, embeddingProvider.embed("shardB doc " + i));
        }

        float[] query = embeddingProvider.embed("shardA doc 0");
        List<String> targetShards = router.route(query, 10);
        assertThat(targetShards).containsExactly("shard-merged");

        List<VectorSearchResult> results = mergedShard.search("ns", query, 10);
        assertThat(results).isNotEmpty();
    }

    @Test
    @DisplayName("S7: Shard Split - split shard, verify both new shards routable and searchable")
    void testShardSplitE2E() {
        float[][] centroids = generateCentroids(4, 5L);
        router.registerShard(
                new ShardCentroids("shard-big", 4, centroids, 2000, 1, System.currentTimeMillis()));
        assertThat(router.shardCount()).isEqualTo(1);

        JVectorVectorIndex bigShard = createShardIndex("big");
        for (int i = 0; i < 40; i++) {
            bigShard.indexWithEmbedding(
                    "ns", "big-key-" + i, embeddingProvider.embed("big shard doc " + i));
        }

        List<String> newShardIds = router.splitShard("shard-big", "shard-left", "shard-right");
        assertThat(newShardIds).containsExactly("shard-left", "shard-right");
        assertThat(router.shardCount()).isEqualTo(2);
        assertThat(router.getShardCentroids("shard-big")).isNull();
        assertThat(router.getShardCentroids("shard-left")).isNotNull();
        assertThat(router.getShardCentroids("shard-right")).isNotNull();

        JVectorVectorIndex leftShard = createShardIndex("left");
        JVectorVectorIndex rightShard = createShardIndex("right");
        for (int i = 0; i < 20; i++) {
            leftShard.indexWithEmbedding(
                    "ns", "big-key-" + i, embeddingProvider.embed("big shard doc " + i));
            rightShard.indexWithEmbedding(
                    "ns",
                    "big-key-" + (i + 20),
                    embeddingProvider.embed("big shard doc " + (i + 20)));
        }

        float[] query = embeddingProvider.embed("big shard doc 0");
        List<String> targetShards = router.route(query, 10);
        assertThat(targetShards).isNotEmpty();
        assertThat(targetShards).isSubsetOf("shard-left", "shard-right");

        List<VectorSearchResult> results = new ArrayList<>();
        for (String shardId : targetShards) {
            JVectorVectorIndex shardIdx = shardId.equals("shard-left") ? leftShard : rightShard;
            results.addAll(shardIdx.search("ns", query, 10));
        }
        assertThat(results).isNotEmpty();
    }

    @Test
    @DisplayName("Single shard: router short-circuits to the only shard")
    void testSingleShardRoutingE2E() {
        float[][] centroids = generateCentroids(4, 42L);
        router.registerShard(
                new ShardCentroids("only-shard", 4, centroids, 100, 1, System.currentTimeMillis()));
        assertThat(router.shardCount()).isEqualTo(1);

        JVectorVectorIndex onlyShard = createShardIndex("only");
        for (int i = 0; i < 20; i++) {
            onlyShard.indexWithEmbedding(
                    "ns", "key-" + i, embeddingProvider.embed("single shard doc " + i));
        }

        float[] query = embeddingProvider.embed("single shard doc 0");
        List<String> targetShards = router.route(query, 10);
        assertThat(targetShards).containsExactly("only-shard");

        List<VectorSearchResult> results = onlyShard.search("ns", query, 10);
        assertThat(results).isNotEmpty();
    }
}
