package io.github.rawwise.axiom.pipeline.graph.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.entity.Entity;
import io.github.rawwise.axiom.pipeline.entity.EntityType;
import io.github.rawwise.axiom.pipeline.entity.EntityView;
import io.github.rawwise.axiom.pipeline.entity.ExtractionStatus;
import io.github.rawwise.axiom.pipeline.entity.Relation;
import io.github.rawwise.axiom.pipeline.graph.EdgeFilter;
import io.github.rawwise.axiom.pipeline.graph.GraphIndexer;
import io.github.rawwise.axiom.pipeline.graph.GraphQuery;
import io.github.rawwise.axiom.pipeline.graph.LocalGraphQueryEngine;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("GraphShardCoordinator E2E - 2-shard local traverse")
class GraphShardCoordinatorE2ETest {

    private Metastore metastore;
    private KnowledgeStore shard0Store;
    private KnowledgeStore shard1Store;
    private GraphCentroidRouter router;
    private GraphShardCoordinator coordinator;

    private Entity a;
    private Entity b;
    private Entity c;
    private Entity d;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        EntityCentroidStore centroidStore = new MetastoreEntityCentroidStore(metastore);

        shard0Store = new InMemoryKnowledgeStore();
        shard1Store = new InMemoryKnowledgeStore();

        a = Entity.deterministic(EntityType.PERSON, "A", "s1", "A", 0.9);
        b = Entity.deterministic(EntityType.EVENT, "B", "s1", "B", 0.9);
        c = Entity.deterministic(EntityType.PERSON, "C", "s2", "C", 0.9);
        d = Entity.deterministic(EntityType.EVENT, "D", "s2", "D", 0.9);

        // Shard 0: A -> B
        Relation rAB = new Relation(a.id(), "attended", b.id(), null, 0.9);
        GraphIndexer idx0 = new GraphIndexer(shard0Store);
        idx0.indexEntities(
                new EntityView(
                        "dia-0",
                        "s1",
                        List.of(a, b),
                        List.of(rAB),
                        "test",
                        Instant.now(),
                        ExtractionStatus.SUCCESS,
                        null));

        // Shard 1: C -> D
        Relation rCD = new Relation(c.id(), "attended", d.id(), null, 0.9);
        GraphIndexer idx1 = new GraphIndexer(shard1Store);
        idx1.indexEntities(
                new EntityView(
                        "dia-1",
                        "s2",
                        List.of(c, d),
                        List.of(rCD),
                        "test",
                        Instant.now(),
                        ExtractionStatus.SUCCESS,
                        null));

        long now = System.currentTimeMillis();
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId(a.id())
                        .entityType(EntityType.PERSON)
                        .sampleId("s1")
                        .shardId(new ShardId(0))
                        .lastUpdatedEpochMs(now)
                        .build());
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId(b.id())
                        .entityType(EntityType.EVENT)
                        .sampleId("s1")
                        .shardId(new ShardId(0))
                        .lastUpdatedEpochMs(now)
                        .build());
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId(c.id())
                        .entityType(EntityType.PERSON)
                        .sampleId("s2")
                        .shardId(new ShardId(1))
                        .lastUpdatedEpochMs(now)
                        .build());
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId(d.id())
                        .entityType(EntityType.EVENT)
                        .sampleId("s2")
                        .shardId(new ShardId(1))
                        .lastUpdatedEpochMs(now)
                        .build());

        router = new GraphCentroidRouter(centroidStore, NameIndexResolver.empty(), 2);
        router.refreshFromMetastore();

        Map<ShardId, GraphQuery> engines = new HashMap<>();
        engines.put(
                new ShardId(0),
                new LocalGraphQueryEngine(
                        shard0Store, new KnowledgeDataAccessor(metastore, shard0Store)));
        engines.put(
                new ShardId(1),
                new LocalGraphQueryEngine(
                        shard1Store, new KnowledgeDataAccessor(metastore, shard1Store)));
        ShardedGraphClient client = new InProcessShardedGraphClient(engines);

        coordinator = new GraphShardCoordinator(router, client);
    }

    @AfterEach
    void tearDown() throws Exception {
        coordinator.close();
        shard0Store.close();
        shard1Store.close();
        metastore.close();
    }

    @Test
    @DisplayName("traverse {A} maxHops=2 reaches B on the same shard (not C/D on other shard)")
    void traverseSingleShardReachesNeighbor() {
        Map<String, Double> scores = coordinator.traverse(Set.of(a.id()), 2, EdgeFilter.allowAll());
        assertThat(scores).containsKey(a.id());
        assertThat(scores).containsKey(b.id());
        assertThat(scores).doesNotContainKey(c.id());
        assertThat(scores).doesNotContainKey(d.id());
    }

    @Test
    @DisplayName("empty seeds -> empty result")
    void emptySeedsReturnEmpty() {
        Map<String, Double> scores = coordinator.traverse(Set.of(), 2, EdgeFilter.allowAll());
        assertThat(scores).isEmpty();
    }

    @Test
    @DisplayName("maxHops=0 -> empty result")
    void zeroHopsReturnsEmpty() {
        Map<String, Double> scores = coordinator.traverse(Set.of(a.id()), 0, EdgeFilter.allowAll());
        assertThat(scores).isEmpty();
    }

    @Test
    @DisplayName("multi-shard seeds: traverse merges scores across shards without crashing")
    void multiShardSeedsMerged() {
        Map<String, Double> scores =
                coordinator.traverse(Set.of(a.id(), c.id()), 2, EdgeFilter.allowAll());
        assertThat(scores).containsKeys(a.id(), c.id());
    }
}
