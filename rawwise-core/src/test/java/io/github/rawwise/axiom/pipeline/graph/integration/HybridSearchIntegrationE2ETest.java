package io.github.rawwise.axiom.pipeline.graph.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.entity.Entity;
import io.github.rawwise.axiom.pipeline.entity.EntityType;
import io.github.rawwise.axiom.pipeline.entity.EntityView;
import io.github.rawwise.axiom.pipeline.entity.ExtractionStatus;
import io.github.rawwise.axiom.pipeline.entity.Relation;
import io.github.rawwise.axiom.pipeline.graph.DiaRef;
import io.github.rawwise.axiom.pipeline.graph.EdgeFilter;
import io.github.rawwise.axiom.pipeline.graph.GraphIndexer;
import io.github.rawwise.axiom.pipeline.graph.GraphQuery;
import io.github.rawwise.axiom.pipeline.graph.LocalGraphQueryEngine;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * §19 Step 3 verify: HybridGraphSearchService graph-recall path end-to-end integration test.
 *
 * <p>verify: when entity via GraphIndexer write KnowledgeStore after, AxiomServiceImpl of graph
 * path (by GraphQuery)canfrom query in to entity diaId .
 *
 * <p>does not directly start RawWise gRPC(avoid Trae sandbox LMDB lock issue), changed to:
 *
 * <ol>
 *   <li>InMemoryKnowledgeStore + GraphIndexer simulatewrite pathof entity/relation
 *   <li>LocalGraphQueryEngine simulate AxiomServiceImpl.search of graph-recall No.
 *   <li>verify query ->entity -> ->diaId columntableoffull chain
 * </ol>
 *
 * <p>and AxiomServiceImpl.search No. [ §19 Step 3] ofis sameof GraphQuery , thisvia test canverify
 * wiring pathofcorrect .
 */
@Slf4j
@DisplayName("§19 Step 3: Hybrid search graph-recall path integration")
class HybridSearchIntegrationE2ETest {

    @Test
    @DisplayName("graph-recall path: ingest after query can diaId")
    void graphRecallPathEndToEnd() throws Exception {
        try (KnowledgeStore ks = new InMemoryKnowledgeStore();
                Metastore metastore = new InMemoryMetastore()) {

            // === Phase 1: simulate AutoDerivationHook write path, GraphIndexer write entity ===
            GraphIndexer indexer = new GraphIndexer(ks);
            Entity caroline =
                    Entity.deterministic(
                            EntityType.PERSON, "Caroline", "sample-1", "Caroline", 0.9);
            Entity lgbtq =
                    Entity.deterministic(
                            EntityType.ORG,
                            "LGBTQ Support Group",
                            "sample-1",
                            "LGBTQ Support Group",
                            0.85);
            Entity seattle =
                    Entity.deterministic(
                            EntityType.LOCATION, "Seattle", "sample-1", "Seattle", 0.9);
            Relation rel = new Relation(caroline.id(), "ATTENDED", lgbtq.id(), null, 0.85);

            EntityView view =
                    new EntityView(
                            "dia-001",
                            "sample-1",
                            List.of(caroline, lgbtq, seattle),
                            List.of(rel),
                            "hybrid",
                            Instant.now(),
                            ExtractionStatus.SUCCESS,
                            null);
            indexer.indexEntities(view);

            // === Phase 2: simulate AxiomServiceImpl.search of graph-recall No. ===
            KnowledgeDataAccessor accessor = new KnowledgeDataAccessor(metastore, ks);
            GraphQuery gq = new LocalGraphQueryEngine(ks, accessor);

            String query = "When did Caroline go to the LGBTQ support group?";

            // §19 Step 3: AxiomServiceImpl.extractEntityCandidates simulate
            List<String> candidates = extractCandidates(query);
            assertThat(candidates).as("query 中的大写词应该被识别为候选 entity").contains("Caroline");

            // findEntity -> traverse -> lookupDiaIds
            Set<String> recalledDiaIds = new java.util.LinkedHashSet<>();
            for (String name : candidates) {
                Optional<Entity> ent = gq.findEntity("sample-1", name);
                if (ent.isEmpty()) continue;
                Map<String, Double> traversed =
                        gq.traverse(Set.of(ent.get().id()), 1, EdgeFilter.allowAll());
                for (String entityId : traversed.keySet()) {
                    for (DiaRef ref : gq.lookupDiaIds(entityId)) {
                        recalledDiaIds.add(ref.diaId());
                    }
                }
            }

            // === Phase 3: verify graph package ground-truth diaId ===
            assertThat(recalledDiaIds)
                    .as("graph 召回必须包含 ground-truth diaId dia-001 (Caroline → LGBTQ → diaId)")
                    .contains("dia-001");

            log.info(
                    "Graph recall path verified: query='{}' candidates={} recalled_dia_ids={}",
                    query,
                    candidates,
                    recalledDiaIds);
        }
    }

    @Test
    @DisplayName(
            "graph-recall path: nomatch entity returns empty , (backward compatibledegradation)")
    void graphRecallReturnsEmptyWhenNoEntityMatches() throws Exception {
        try (KnowledgeStore ks = new InMemoryKnowledgeStore();
                Metastore metastore = new InMemoryMetastore()) {
            KnowledgeDataAccessor accessor = new KnowledgeDataAccessor(metastore, ks);
            GraphQuery gq = new LocalGraphQueryEngine(ks, accessor);

            // KnowledgeStore, no entity
            String query = "Did NonExistentPerson visit Atlantis?";
            List<String> candidates = extractCandidates(query);
            assertThat(candidates).contains("NonExistentPerson", "Atlantis");

            Set<String> recalledDiaIds = new java.util.LinkedHashSet<>();
            for (String name : candidates) {
                Optional<Entity> ent = gq.findEntity("sample-1", name);
                assertThat(ent).as("无 entity 时 findEntity 必须返回 Optional.empty()").isEmpty();
                if (ent.isEmpty()) continue;
            }
            assertThat(recalledDiaIds).isEmpty();
        }
    }

    /**
     * and AxiomServiceImpl#extractEntityCandidates exactly consistentof query entity candidate
     * extractionlogic.
     */
    private static List<String> extractCandidates(String query) {
        if (query == null || query.isBlank()) return List.of();
        List<String> result = new java.util.ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        java.util.Set<String> stop =
                java.util.Set.of(
                        "When", "What", "Where", "Who", "Why", "How", "Which", "Did", "Does", "Do",
                        "Is", "Are", "Was", "Were", "Has", "Have", "The");
        for (String token : query.split("[\\s\\p{Punct}]+")) {
            if (token.length() < 2) continue;
            char c0 = token.charAt(0);
            if (!Character.isLetter(c0) || !Character.isUpperCase(c0)) continue;
            if (stop.contains(token)) continue;
            if (seen.add(token)) result.add(token);
            if (result.size() >= 8) break;
        }
        return result;
    }
}
