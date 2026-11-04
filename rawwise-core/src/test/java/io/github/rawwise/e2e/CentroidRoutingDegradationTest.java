package io.github.rawwise.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.axiom.pipeline.entity.EntityType;
import io.github.rawwise.axiom.pipeline.graph.distributed.EntityCentroid;
import io.github.rawwise.axiom.pipeline.graph.distributed.EntityCentroidStore;
import io.github.rawwise.axiom.pipeline.graph.distributed.GraphCentroidRouter;
import io.github.rawwise.axiom.pipeline.graph.distributed.NameIndexResolver;
import io.github.rawwise.axiom.pipeline.graph.distributed.ShardId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("CentroidRoutingDegradation - distributedcentroid routingdegradationend-to-end test")
class CentroidRoutingDegradationTest {

    private TestEntityCentroidStore centroidStore;
    private GraphCentroidRouter router;

    @BeforeEach
    void setUp() {
        centroidStore = new TestEntityCentroidStore();
        router = new GraphCentroidRouter(centroidStore, NameIndexResolver.empty(), 4, true);
    }

    private EntityCentroid centroid(String entityId, EntityType type, int shard) {
        return EntityCentroid.builder()
                .entityId(entityId)
                .entityType(type)
                .sampleId("sample-" + entityId)
                .shardId(new ShardId(shard))
                .lastUpdatedEpochMs(System.currentTimeMillis())
                .build();
    }

    @Nested
    @DisplayName("S1-HappyPath - centroidfrom EntityCentroidStore load")
    class HappyPath {

        @Test
        @DisplayName(
                "refreshFromMetastore loadcentroidafterrouting entityId returns correctly shard")
        void refreshLoadsCentroidsAndRoutingWorks() {
            centroidStore.put("entity-A", centroid("entity-A", EntityType.PERSON, 0));
            centroidStore.put("entity-B", centroid("entity-B", EntityType.ORG, 1));

            router.refreshFromMetastore();

            assertThat(router.isInitialized()).isTrue();
            assertThat(router.cachedCentroidCount()).isEqualTo(2);

            ShardId shardA = router.routeByEntityId("entity-A");
            assertThat(shardA).isEqualTo(new ShardId(0));

            ShardId shardB = router.routeByEntityId("entity-B");
            assertThat(shardB).isEqualTo(new ShardId(1));
        }

        @Test
        @DisplayName("unknown entityId fallbackto hash calculate shard")
        void unknownEntityIdFallsBackToHashRouting() {
            centroidStore.put("entity-A", centroid("entity-A", EntityType.PERSON, 2));
            router.refreshFromMetastore();

            ShardId shard = router.routeByEntityId("entity-unknown");
            assertThat(shard).isNotNull();
            assertThat(shard.value()).isBetween(0, 3);
        }
    }

    @Nested
    @DisplayName("S3-FaultRecovery - Metastore degrade to cached centroid when unavailable")
    class FaultRecovery {

        @Test
        @DisplayName("Metastore scanAll exception refresh cachecentroid in")
        void metastoreUnavailableRetainsCachedCentroids() {
            centroidStore.put("entity-A", centroid("entity-A", EntityType.PERSON, 0));
            centroidStore.put("entity-B", centroid("entity-B", EntityType.ORG, 1));
            centroidStore.put("entity-C", centroid("entity-C", EntityType.LOCATION, 2));
            router.refreshFromMetastore();
            assertThat(router.cachedCentroidCount()).isEqualTo(3);

            centroidStore.setScanAllThrows(true);

            router.refreshFromMetastore();

            assertThat(router.cachedCentroidCount()).isEqualTo(3);
            assertThat(router.isInitialized()).isTrue();

            ShardId shardA = router.routeByEntityId("entity-A");
            assertThat(shardA).isEqualTo(new ShardId(0));
            ShardId shardB = router.routeByEntityId("entity-B");
            assertThat(shardB).isEqualTo(new ShardId(1));
            ShardId shardC = router.routeByEntityId("entity-C");
            assertThat(shardC).isEqualTo(new ShardId(2));
        }

        @Test
        @DisplayName(
                "Metastore scanAll returns empty list alreadyhascachecentroid(cancandegradationscenario)")
        void emptyScanAllRetainsCacheWhenInitialized() {
            centroidStore.put("entity-A", centroid("entity-A", EntityType.PERSON, 0));
            router.refreshFromMetastore();
            assertThat(router.cachedCentroidCount()).isEqualTo(1);

            centroidStore.clear();

            router.refreshFromMetastore();

            assertThat(router.cachedCentroidCount()).isEqualTo(1);
            ShardId shard = router.routeByEntityId("entity-A");
            assertThat(shard).isEqualTo(new ShardId(0));
        }

        @Test
        @DisplayName("fallbackOnError=false Metastore exception non-degradation")
        void fallbackDisabledThrowsOnMetastoreError() {
            GraphCentroidRouter strictRouter =
                    new GraphCentroidRouter(centroidStore, NameIndexResolver.empty(), 4, false);
            centroidStore.put("entity-A", centroid("entity-A", EntityType.PERSON, 0));
            strictRouter.refreshFromMetastore();

            centroidStore.setScanAllThrows(true);

            assertThatThrownBy(strictRouter::refreshFromMetastore)
                    .isInstanceOf(RuntimeException.class);
        }
    }

    static class TestEntityCentroidStore implements EntityCentroidStore {
        private final Map<String, EntityCentroid> store = new ConcurrentHashMap<>();
        private volatile boolean scanAllThrows = false;
        private volatile long versionMs = System.currentTimeMillis();

        void put(String entityId, EntityCentroid centroid) {
            store.put(entityId, centroid);
            versionMs = System.currentTimeMillis();
        }

        void clear() {
            store.clear();
        }

        void setScanAllThrows(boolean throwsOnScan) {
            this.scanAllThrows = throwsOnScan;
        }

        @Override
        public Optional<EntityCentroid> lookup(String entityId) {
            return Optional.ofNullable(store.get(entityId));
        }

        @Override
        public List<EntityCentroid> lookupByType(EntityType type) {
            return store.values().stream().filter(c -> c.entityType() == type).toList();
        }

        @Override
        public void upsert(EntityCentroid centroid) {
            store.put(centroid.entityId(), centroid);
            versionMs = System.currentTimeMillis();
        }

        @Override
        public void delete(String entityId) {
            store.remove(entityId);
        }

        @Override
        public List<EntityCentroid> scanAll() {
            if (scanAllThrows) {
                throw new RuntimeException("Metastore unavailable: connection refused");
            }
            return List.copyOf(store.values());
        }

        @Override
        public long lastVersionMs() {
            return versionMs;
        }
    }
}
