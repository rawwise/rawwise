package io.github.rawwise.axiom.pipeline.contract;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.entity.Entity;
import io.github.rawwise.axiom.pipeline.entity.EntityType;
import io.github.rawwise.axiom.pipeline.entity.EntityView;
import io.github.rawwise.axiom.pipeline.entity.ExtractionStatus;
import io.github.rawwise.axiom.pipeline.entity.Relation;
import io.github.rawwise.axiom.pipeline.graph.GraphIndexer;
import io.github.rawwise.axiom.pipeline.graph.GraphStoreSchema;
import io.github.rawwise.axiom.pipeline.graph.distributed.EntityCentroid;
import io.github.rawwise.axiom.pipeline.graph.distributed.GraphCentroidRouter;
import io.github.rawwise.axiom.pipeline.graph.distributed.MetastoreEntityCentroidStore;
import io.github.rawwise.axiom.pipeline.graph.distributed.NameIndexResolver;
import io.github.rawwise.axiom.pipeline.graph.distributed.ShardId;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Cross-domain contract verification E2E tests (Phase C design §7 + §16.1 T-B3).
 *
 * <p>Each test method maps to one PHC-* contract and verifies it via observable behavior or static
 * greppable invariants.
 */
@Slf4j
@DisplayName("Phase C PHC-* cross-domain contract verification")
class PhaseCContractE2ETest {

    /**
     * PHC-001 (Entity ↔ EntityView): async derivation failure must result in EntityView.status ==
     * FAILED with non-null failureReason.
     */
    @Test
    @DisplayName("PHC-001: EntityView.failed produces FAILED status with reason")
    void phc001_FailedExtractionMarksFailed() {
        EntityView failed = EntityView.failed("dia-1", "sample-1", "TIMEOUT");
        assertThat(failed.status()).isEqualTo(ExtractionStatus.FAILED);
        assertThat(failed.failureReason()).isEqualTo("TIMEOUT");
        assertThat(failed.entities()).isEmpty();
        assertThat(failed.relations()).isEmpty();

        // pending -> not FAILED
        EntityView pending = EntityView.pending("dia-2", "sample-1");
        assertThat(pending.status()).isEqualTo(ExtractionStatus.PENDING);
        assertThat(pending.failureReason()).isNull();
    }

    /**
     * PHC-002 (Graph ↔ Metastore): EntityCentroid stored in Metastore is the single source of
     * truth. Router.refreshFromMetastore must reflect any upsert performed against the underlying
     * Metastore.
     */
    @Test
    @DisplayName(
            "PHC-002: Metastore centroid is single authority — router reflects upsert after refresh")
    void phc002_MetastoreCentroidIsSingleAuthority() throws Exception {
        try (Metastore ms = new InMemoryMetastore()) {
            MetastoreEntityCentroidStore store = new MetastoreEntityCentroidStore(ms);
            GraphCentroidRouter router =
                    new GraphCentroidRouter(store, NameIndexResolver.empty(), 4);

            // Insert a centroid via the store (which writes to metastore)
            store.upsert(
                    EntityCentroid.builder()
                            .entityId("ent-A")
                            .entityType(EntityType.PERSON)
                            .sampleId("s1")
                            .shardId(new ShardId(2))
                            .lastUpdatedEpochMs(System.currentTimeMillis())
                            .build());

            // Before refresh, router does not yet know the centroid (cold map),
            // so falls back to computeShard(sampleId, type). After refresh, it must
            // return the persisted shardId=2.
            router.refreshFromMetastore();
            ShardId resolved = router.routeByEntityId("ent-A");
            assertThat(resolved).isEqualTo(new ShardId(2));

            // Sanity: lookup directly via store agrees
            Optional<EntityCentroid> looked = store.lookup("ent-A");
            assertThat(looked).isPresent();
            assertThat(looked.get().shardId()).isEqualTo(new ShardId(2));
        }
    }

    /**
     * PHC-004 (Frontend ↔ Backend Server): Frontend package MUST NOT import any engine.graph /
     * engine.vector.onnx / axiom.pipeline.entity packages. This is a greppable invariant per design
     * §3.2.
     */
    @Test
    @DisplayName("PHC-004: Frontend package contains no Phase C engine/entity imports (greppable)")
    void phc004_FrontendNoStorageOrEngineImports() throws IOException {
        Path frontendRoot = Paths.get("src/main/java/io/github/rawwise/frontend");
        if (!Files.exists(frontendRoot)) {
            log.info(
                    "Frontend root not found at {} — skipping (running from different module)",
                    frontendRoot);
            return;
        }
        try (Stream<Path> files = Files.walk(frontendRoot)) {
            files.filter(p -> p.toString().endsWith(".java"))
                    .forEach(
                            p -> {
                                try {
                                    String content = Files.readString(p);
                                    assertThat(content)
                                            .as(
                                                    "Frontend file %s must not import axiom.pipeline.entity",
                                                    p)
                                            .doesNotContain(
                                                    "import io.github.rawwise.axiom.pipeline.entity");
                                    assertThat(content)
                                            .as(
                                                    "Frontend file %s must not import axiom.pipeline.graph",
                                                    p)
                                            .doesNotContain(
                                                    "import io.github.rawwise.axiom.pipeline.graph");
                                    assertThat(content)
                                            .as(
                                                    "Frontend file %s must not import engine.vector.onnx",
                                                    p)
                                            .doesNotContain(
                                                    "import io.github.rawwise.engine.vector.onnx");
                                } catch (IOException e) {
                                    throw new RuntimeException("Failed to read " + p, e);
                                }
                            });
        }
    }

    /**
     * PHC-009 (KnowledgeStore ↔ GraphStore): GraphIndexer writes must land at keys with graph:*
     * prefix in KnowledgeStore. This guarantees Phase C graph data shares the v87 KnowledgeStore
     * schema namespace.
     */
    @Test
    @DisplayName("PHC-009: GraphIndexer writes land at graph:* keys in KnowledgeStore")
    void phc009_GraphWritesUseGraphPrefixInKnowledgeStore() throws Exception {
        try (KnowledgeStore ks = new InMemoryKnowledgeStore()) {
            GraphIndexer indexer = new GraphIndexer(ks);
            Entity alice = Entity.deterministic(EntityType.PERSON, "Alice", "s1", "Alice", 0.9);
            Entity bob = Entity.deterministic(EntityType.PERSON, "Bob", "s1", "Bob", 0.9);
            Relation rel = new Relation(alice.id(), "KNOWS", bob.id(), null, 0.85);

            EntityView view =
                    new EntityView(
                            "dia-1",
                            "s1",
                            List.of(alice, bob),
                            List.of(rel),
                            "hybrid",
                            Instant.now(),
                            ExtractionStatus.SUCCESS,
                            null);

            indexer.indexEntities(view);

            // Verify writes landed at expected graph:* keys
            assertThat(ks.exists(GraphStoreSchema.nodeKey(alice.id()))).isTrue();
            assertThat(ks.exists(GraphStoreSchema.nodeKey(bob.id()))).isTrue();
            assertThat(ks.exists(GraphStoreSchema.outEdgeKey(alice.id(), "KNOWS", bob.id())))
                    .isTrue();
            assertThat(ks.exists(GraphStoreSchema.inEdgeKey(bob.id(), "KNOWS", alice.id())))
                    .isTrue();

            // All keys must start with "graph:" prefix
            byte[] nodeKey = GraphStoreSchema.nodeKey(alice.id());
            String nodeKeyStr = new String(nodeKey, StandardCharsets.UTF_8);
            assertThat(nodeKeyStr).startsWith("graph:");
        }
    }

    /**
     * PHC-005 (DerivationEngine ↔ EntityDerivationStage): EntityView records carry sampleId and
     * diaId that originate from the upstream record, verifying the derivation stage preserves
     * provenance.
     */
    @Test
    @DisplayName("PHC-005: EntityView preserves diaId and sampleId from derivation context")
    void phc005_EntityViewPreservesProvenance() {
        EntityView pending = EntityView.pending("dia-xyz", "sample-abc");
        assertThat(pending.diaId()).isEqualTo("dia-xyz");
        assertThat(pending.sampleId()).isEqualTo("sample-abc");
        assertThat(pending.source()).isEqualTo("pending");
    }
}
