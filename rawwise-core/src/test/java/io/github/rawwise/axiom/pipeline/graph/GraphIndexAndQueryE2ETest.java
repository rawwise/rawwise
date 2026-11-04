package io.github.rawwise.axiom.pipeline.graph;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.entity.Entity;
import io.github.rawwise.axiom.pipeline.entity.EntityType;
import io.github.rawwise.axiom.pipeline.entity.EntityView;
import io.github.rawwise.axiom.pipeline.entity.ExtractionStatus;
import io.github.rawwise.axiom.pipeline.entity.Relation;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("GraphIndexer + LocalGraphQueryEngine E2E")
class GraphIndexAndQueryE2ETest {

    private KnowledgeStore knowledgeStore;
    private Metastore metastore;
    private GraphIndexer indexer;
    private LocalGraphQueryEngine engine;

    private Entity entityA;
    private Entity entityB;
    private Entity entityC;

    @BeforeEach
    void setUp() {
        knowledgeStore = new InMemoryKnowledgeStore();
        metastore = new InMemoryMetastore();
        indexer = new GraphIndexer(knowledgeStore);
        KnowledgeDataAccessor accessor = new KnowledgeDataAccessor(metastore, knowledgeStore);
        engine = new LocalGraphQueryEngine(knowledgeStore, accessor);

        entityA = Entity.deterministic(EntityType.PERSON, "Alice", "s1", "Alice", 0.9);
        entityB = Entity.deterministic(EntityType.EVENT, "Wedding", "s1", "Wedding", 0.85);
        entityC = Entity.deterministic(EntityType.LOCATION, "Boston", "s1", "Boston", 0.8);

        Relation rAB = new Relation(entityA.id(), "attended", entityB.id(), null, 0.9);
        Relation rBC = new Relation(entityB.id(), "located_in", entityC.id(), null, 0.8);

        EntityView view =
                new EntityView(
                        "dia-1",
                        "s1",
                        List.of(entityA, entityB, entityC),
                        List.of(rAB, rBC),
                        "test",
                        Instant.now(),
                        ExtractionStatus.SUCCESS,
                        null);

        indexer.indexEntities(view);
    }

    @AfterEach
    void tearDown() throws Exception {
        knowledgeStore.close();
        metastore.close();
    }

    @Test
    @DisplayName("findEntity returns indexed PERSON entity by name")
    void findEntityByName() {
        Optional<Entity> found = engine.findEntity("s1", "Alice");
        assertThat(found).isPresent();
        assertThat(found.get().id()).isEqualTo(entityA.id());
        assertThat(found.get().type()).isEqualTo(EntityType.PERSON);
    }

    @Test
    @DisplayName("findEntity with non-existent name returns empty")
    void findEntityMissing() {
        Optional<Entity> found = engine.findEntity("s1", "Nobody");
        assertThat(found).isEmpty();
    }

    @Test
    @DisplayName(
            "traverse from {A} with maxHops=2 returns scores monotonically non-increasing per hop")
    void traverseMonotonicallyDecreasing() {
        Map<String, Double> scores =
                engine.traverse(Set.of(entityA.id()), 2, EdgeFilter.allowAll());
        assertThat(scores).containsKey(entityA.id());
        assertThat(scores).containsKey(entityB.id());
        assertThat(scores).containsKey(entityC.id());

        double scoreA = scores.get(entityA.id());
        double scoreB = scores.get(entityB.id());
        double scoreC = scores.get(entityC.id());
        assertThat(scoreA).isGreaterThanOrEqualTo(scoreB);
        assertThat(scoreB).isGreaterThanOrEqualTo(scoreC);
        assertThat(scoreC).isGreaterThan(0.0);
    }

    @Test
    @DisplayName("traverse with empty seeds returns empty map")
    void traverseEmptySeeds() {
        Map<String, Double> scores = engine.traverse(Set.of(), 2, EdgeFilter.allowAll());
        assertThat(scores).isEmpty();
    }

    @Test
    @DisplayName("lookupDiaIds returns the dia reference for an indexed entity")
    void lookupDiaIdsReturnsRef() {
        Set<DiaRef> refs = engine.lookupDiaIds(entityA.id());
        assertThat(refs).isNotEmpty();
        boolean hasDia1 = refs.stream().anyMatch(r -> "dia-1".equals(r.diaId()));
        assertThat(hasDia1).isTrue();
    }
}
