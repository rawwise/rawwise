package io.github.rawwise.axiom.pipeline.graph.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.entity.EntityType;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("GraphCentroidRouter E2E - centroid routing + bloom broadcast")
class GraphCentroidRouterE2ETest {

    private Metastore metastore;
    private EntityCentroidStore centroidStore;
    private GraphCentroidRouter router;

    private static final int SHARD_COUNT = 4;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        centroidStore = new MetastoreEntityCentroidStore(metastore);
        router = new GraphCentroidRouter(centroidStore, NameIndexResolver.empty(), SHARD_COUNT);

        long now = System.currentTimeMillis();
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId("e1")
                        .entityType(EntityType.PERSON)
                        .sampleId("s1")
                        .shardId(new ShardId(0))
                        .lastUpdatedEpochMs(now)
                        .build());
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId("e2")
                        .entityType(EntityType.PERSON)
                        .sampleId("s1")
                        .shardId(new ShardId(0))
                        .lastUpdatedEpochMs(now)
                        .build());
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId("e3")
                        .entityType(EntityType.ORG)
                        .sampleId("s1")
                        .shardId(new ShardId(2))
                        .lastUpdatedEpochMs(now)
                        .build());
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId("e4")
                        .entityType(EntityType.ORG)
                        .sampleId("s2")
                        .shardId(new ShardId(2))
                        .lastUpdatedEpochMs(now)
                        .build());
    }

    @AfterEach
    void tearDown() throws Exception {
        metastore.close();
    }

    @Test
    @DisplayName(
            "before refresh: routeBroadcast returns all shards for unknown entity (cold start)")
    void coldStartBroadcastAllShards() {
        Set<ShardId> shards = router.routeBroadcast("unknown-entity");
        assertThat(shards).hasSize(SHARD_COUNT);
    }

    @Test
    @DisplayName("after refresh: routeByEntityId(known) returns the centroid's stored shard")
    void routeKnownEntityById() {
        router.refreshFromMetastore();
        assertThat(router.routeByEntityId("e1")).isEqualTo(new ShardId(0));
        assertThat(router.routeByEntityId("e3")).isEqualTo(new ShardId(2));
    }

    @Test
    @DisplayName("after refresh: routeByEntityId(unknown) returns hash-based shard")
    void routeUnknownEntityFallsBackToHash() {
        router.refreshFromMetastore();
        ShardId shard = router.routeByEntityId("never-seen-id");
        assertThat(shard).isNotNull();
        assertThat(shard.value()).isBetween(0, SHARD_COUNT - 1);
    }

    @Test
    @DisplayName(
            "after refresh: routeBroadcast for known entity returns containing shard via bloom mightContain")
    void broadcastAfterRefreshContainsKnown() {
        router.refreshFromMetastore();
        Set<ShardId> shards = router.routeBroadcast("e1");
        assertThat(shards).contains(new ShardId(0));
    }

    @Test
    @DisplayName(
            "after refresh: routeBroadcast for unknown entity returns empty (initialized + bloom miss)")
    void broadcastAfterRefreshEmptyForUnknown() {
        router.refreshFromMetastore();
        Set<ShardId> shards = router.routeBroadcast("totally-unknown-12345");
        // Bloom filter has small false-positive prob (0.001), but the API contract is:
        // after initialized, broadcast may return some shards due to bloom FP, but typically empty.
        // We assert that we do not return ALL shards (which is the cold-start behavior).
        assertThat(shards.size()).isLessThan(SHARD_COUNT);
    }

    @Test
    @DisplayName("refresh updates lastRefreshMs timestamp")
    void refreshUpdatesTimestamp() {
        long before = router.lastRefreshMs();
        router.refreshFromMetastore();
        long after = router.lastRefreshMs();
        assertThat(after).isGreaterThanOrEqualTo(before);
        assertThat(after).isGreaterThan(0L);
    }
}
