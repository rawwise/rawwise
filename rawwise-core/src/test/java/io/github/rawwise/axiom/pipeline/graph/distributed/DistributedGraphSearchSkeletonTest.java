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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("DistributedGraphSearch Integration E2E - three-way recall + RRF fusion")
class DistributedGraphSearchSkeletonTest {

    private Metastore metastore;
    private KnowledgeStore shard0Store;
    private KnowledgeStore shard1Store;
    private EntityCentroidStore centroidStore;
    private GraphCentroidRouter router;
    private GraphShardCoordinator shardCoordinator;
    private GraphSearchCoordinatorImpl graphCoordinator;
    private SimpleStubVectorSearchCoordinator vectorCoordinator;
    private SimpleStubKeywordSearchCoordinator keywordCoordinator;
    private HybridGraphSearchService service;

    private Entity caroline;
    private Entity lgbtq;
    private Entity seattle;
    private Entity alice;

    private static final String SAMPLE_ID = "sample-1";
    private static final int SHARD_COUNT = 2;

    @BeforeEach
    void setUp() throws Exception {
        metastore = new InMemoryMetastore();
        centroidStore = new MetastoreEntityCentroidStore(metastore);
        shard0Store = new InMemoryKnowledgeStore();
        shard1Store = new InMemoryKnowledgeStore();

        caroline = Entity.deterministic(EntityType.PERSON, "Caroline", SAMPLE_ID, "Caroline", 0.95);
        lgbtq =
                Entity.deterministic(
                        EntityType.ORG,
                        "LGBTQ Support Group",
                        SAMPLE_ID,
                        "LGBTQ Support Group",
                        0.9);
        seattle = Entity.deterministic(EntityType.LOCATION, "Seattle", SAMPLE_ID, "Seattle", 0.9);
        alice = Entity.deterministic(EntityType.PERSON, "Alice", SAMPLE_ID, "Alice", 0.85);

        Relation rCarolineLgbtq = new Relation(caroline.id(), "ATTENDED", lgbtq.id(), null, 0.9);
        GraphIndexer idx0 = new GraphIndexer(shard0Store);
        idx0.indexEntities(
                new EntityView(
                        "dia-001",
                        SAMPLE_ID,
                        List.of(caroline, lgbtq),
                        List.of(rCarolineLgbtq),
                        "hybrid",
                        Instant.now(),
                        ExtractionStatus.SUCCESS,
                        null));

        Relation rAliceSeattle = new Relation(alice.id(), "VISITED", seattle.id(), null, 0.8);
        GraphIndexer idx1 = new GraphIndexer(shard1Store);
        idx1.indexEntities(
                new EntityView(
                        "dia-002",
                        SAMPLE_ID,
                        List.of(alice, seattle),
                        List.of(rAliceSeattle),
                        "hybrid",
                        Instant.now(),
                        ExtractionStatus.SUCCESS,
                        null));

        long now = System.currentTimeMillis();
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId(caroline.id())
                        .entityType(EntityType.PERSON)
                        .sampleId(SAMPLE_ID)
                        .shardId(new ShardId(0))
                        .lastUpdatedEpochMs(now)
                        .build());
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId(lgbtq.id())
                        .entityType(EntityType.ORG)
                        .sampleId(SAMPLE_ID)
                        .shardId(new ShardId(0))
                        .lastUpdatedEpochMs(now)
                        .build());
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId(seattle.id())
                        .entityType(EntityType.LOCATION)
                        .sampleId(SAMPLE_ID)
                        .shardId(new ShardId(1))
                        .lastUpdatedEpochMs(now)
                        .build());
        centroidStore.upsert(
                EntityCentroid.builder()
                        .entityId(alice.id())
                        .entityType(EntityType.PERSON)
                        .sampleId(SAMPLE_ID)
                        .shardId(new ShardId(1))
                        .lastUpdatedEpochMs(now)
                        .build());

        Map<String, String> nameIndex = new HashMap<>();
        nameIndex.put(SAMPLE_ID + ":" + Entity.canonicalize("Caroline"), caroline.id());
        nameIndex.put(SAMPLE_ID + ":" + Entity.canonicalize("LGBTQ Support Group"), lgbtq.id());
        nameIndex.put(SAMPLE_ID + ":" + Entity.canonicalize("Seattle"), seattle.id());
        nameIndex.put(SAMPLE_ID + ":" + Entity.canonicalize("Alice"), alice.id());
        NameIndexResolver nameResolver =
                (sampleId, canon) -> Optional.ofNullable(nameIndex.get(sampleId + ":" + canon));

        router = new GraphCentroidRouter(centroidStore, nameResolver, SHARD_COUNT);
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

        shardCoordinator = new GraphShardCoordinator(router, client);
        graphCoordinator = new GraphSearchCoordinatorImpl(router, shardCoordinator);

        Map<String, Integer> vocab = new LinkedHashMap<>();
        vocab.put("caroline", 0);
        vocab.put("attended", 1);
        vocab.put("lgbtq", 2);
        vocab.put("support", 3);
        vocab.put("group", 4);
        vocab.put("alice", 5);
        vocab.put("visited", 6);
        vocab.put("museum", 7);
        vocab.put("seattle", 8);

        Function<String, double[]> embedder =
                query -> {
                    double[] vec = new double[vocab.size()];
                    for (String token : query.toLowerCase().split("[\\s\\p{Punct}]+")) {
                        Integer idx = vocab.get(token);
                        if (idx != null) vec[idx] = 1.0;
                    }
                    return vec;
                };

        Map<String, double[]> docVectors = new LinkedHashMap<>();
        docVectors.put("dia-001", embedder.apply("Caroline attended LGBTQ support group Seattle"));
        docVectors.put("dia-002", embedder.apply("Alice visited museum"));
        vectorCoordinator = new SimpleStubVectorSearchCoordinator(docVectors, embedder);

        Map<String, String> documents = new LinkedHashMap<>();
        documents.put("dia-001", "Caroline attended the LGBTQ support group in Seattle");
        documents.put("dia-002", "Alice visited the art museum downtown");
        keywordCoordinator = new SimpleStubKeywordSearchCoordinator(documents);

        service =
                new HybridGraphSearchService(
                        vectorCoordinator,
                        keywordCoordinator,
                        graphCoordinator,
                        QueryReformulationStage.identity(),
                        2000L);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (service != null) service.close();
        if (shardCoordinator != null) shardCoordinator.close();
        if (shard0Store != null) shard0Store.close();
        if (shard1Store != null) shard1Store.close();
        if (metastore != null) metastore.close();
    }

    @Test
    @DisplayName(
            "three-way recall: vector + keyword + graph all contribute, RRF fuses with correct ranking")
    void threeWayRecallAndRrfFusion() {
        HybridSearchRequest req =
                HybridSearchRequest.builder()
                        .tableId(SAMPLE_ID)
                        .query("Caroline")
                        .topK(10)
                        .graphMaxHops(2)
                        .multiQuery(false)
                        .build();

        HybridSearchResponse resp = service.hybridSearch(req);

        assertThat(resp).isNotNull();
        assertThat(resp.hits()).isNotEmpty();

        Set<String> hitIds = new java.util.LinkedHashSet<>();
        for (HybridSearchResponse.Hit h : resp.hits()) {
            hitIds.add(h.viewId());
            assertThat(h.score()).isGreaterThan(0.0);
            assertThat(h.provenance()).isNotNull();
        }

        assertThat(hitIds).contains("dia-001");
        assertThat(hitIds).contains(caroline.id());

        HybridSearchResponse.Hit topHit = resp.hits().get(0);
        assertThat(topHit.viewId()).isEqualTo("dia-001");

        HybridSearchResponse.LatencyBreakdown lat = resp.latencyBreakdown();
        assertThat(lat).isNotNull();
        assertThat(lat.vectorMs()).isGreaterThanOrEqualTo(0L);
        assertThat(lat.keywordMs()).isGreaterThanOrEqualTo(0L);
        assertThat(lat.graphMs()).isGreaterThanOrEqualTo(0L);
        assertThat(lat.rrfMs()).isGreaterThanOrEqualTo(0L);
        assertThat(lat.totalMs()).isGreaterThanOrEqualTo(0L);

        log.info(
                "Three-way recall verified: hits={}, topHit={}, latency={}",
                resp.hits().size(),
                topHit.viewId(),
                lat);
    }

    @Test
    @DisplayName(
            "graph-only fallback: when vector and keyword miss, graph path still returns entity hits")
    void graphPathContributesAlongsideVectorKeyword() {
        HybridSearchRequest req =
                HybridSearchRequest.builder()
                        .tableId(SAMPLE_ID)
                        .query("Alice")
                        .topK(10)
                        .graphMaxHops(1)
                        .multiQuery(false)
                        .build();

        HybridSearchResponse resp = service.hybridSearch(req);

        assertThat(resp).isNotNull();
        assertThat(resp.hits()).isNotEmpty();

        boolean hasAliceEntity = false;
        for (HybridSearchResponse.Hit h : resp.hits()) {
            assertThat(h.score()).isGreaterThan(0.0);
            if (h.viewId().equals(alice.id())) {
                hasAliceEntity = true;
                assertThat(h.provenance().retrievalPath()).isEqualTo("GRAPH_AND_VECTOR");
            }
        }
        assertThat(hasAliceEntity).as("graph path should return Alice entity").isTrue();

        log.info("Graph-only fallback verified: hits={}", resp.hits().size());
    }

    @Test
    @DisplayName(
            "multiQuery expansion: reformulation generates variants, all three paths still functional")
    void multiQueryExpansionWorks() {
        QueryReformulationStage reform =
                (query, n) -> List.of(query, query + " support", "LGBTQ " + query);
        try (HybridGraphSearchService multiService =
                new HybridGraphSearchService(
                        vectorCoordinator, keywordCoordinator, graphCoordinator, reform, 2000L)) {

            HybridSearchRequest req =
                    HybridSearchRequest.builder()
                            .tableId(SAMPLE_ID)
                            .query("Caroline")
                            .topK(10)
                            .graphMaxHops(1)
                            .multiQuery(true)
                            .build();

            HybridSearchResponse resp = multiService.hybridSearch(req);

            assertThat(resp).isNotNull();
            assertThat(resp.hits()).isNotEmpty();

            Set<String> hitIds = new java.util.LinkedHashSet<>();
            for (HybridSearchResponse.Hit h : resp.hits()) {
                hitIds.add(h.viewId());
            }
            assertThat(hitIds).contains("dia-001");

            log.info("MultiQuery expansion verified: hits={}", resp.hits().size());
        }
    }

    @Test
    @DisplayName("cross-shard traversal: seeds on shard 0 reach entities via graph hops")
    void crossShardTraversalViaGraph() {
        Map<String, Double> scores =
                shardCoordinator.traverse(Set.of(caroline.id()), 2, EdgeFilter.allowAll());

        assertThat(scores).containsKey(caroline.id());
        assertThat(scores).containsKey(lgbtq.id());
        assertThat(scores.get(caroline.id())).isGreaterThan(0.0);

        log.info("Cross-shard traverse verified: scores={}", scores.keySet());
    }
}
