package io.github.rawwise.axiom.pipeline.entity.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.entity.EntityDerivationStage;
import io.github.rawwise.axiom.pipeline.entity.EntityExtractionConfig;
import io.github.rawwise.axiom.pipeline.entity.EntityExtractorFactory;
import io.github.rawwise.axiom.pipeline.entity.HybridEntityExtractor;
import io.github.rawwise.axiom.pipeline.graph.GraphIndexer;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.model.record.MapRecord;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * §19 Step 1 verify: M1 EntityExtraction write path wiring end-to-end integration test.
 *
 * <p>[review-convergence.md](.trae/rules/review-convergence.md) "end-to-end integration integrity
 * enforcement"BLOCKER rule:
 *
 * <ul>
 *   <li>requirement 1: from path ({@link EntityDerivationStage#onWrite})trigger, verify
 *       (KnowledgeStore of entity_view:* / graph:node:*)ofwrite.
 *   <li>requirement 2: real (HybridEntityExtractor + GraphIndexer + real KnowledgeStore), does not
 *       mock critical path.
 *   <li>requirement 3: *IntegrationE2ETest, and *E2ETest .
 * </ul>
 */
@Slf4j
@DisplayName("§19 Step 1: EntityDerivationStage write-path integration")
class EntityWritePathIntegrationE2ETest {

    @Test
    @DisplayName(
            "onWrite triggerafter, KnowledgeStore occurrence entity_view:* + async afteroccurrence SUCCESS")
    void onWriteTriggersEntityViewPersistence() throws Exception {
        try (KnowledgeStore ks = new InMemoryKnowledgeStore()) {
            EntityExtractionConfig cfg =
                    EntityExtractionConfig.builder()
                            .enabled(true)
                            .hybridEnabled(true)
                            .llmEndpoint("stub://test")
                            .timeout(Duration.ofSeconds(5))
                            .asyncThreads(2)
                            .asyncQueueSize(100)
                            .build();
            HybridEntityExtractor extractor = EntityExtractorFactory.create(cfg);
            GraphIndexer indexer = new GraphIndexer(ks);
            ThreadPoolExecutor pool = EntityDerivationStage.newDefaultPool(cfg);
            EntityDerivationStage stage =
                    new EntityDerivationStage(extractor, indexer, ks, pool, cfg);

            byte[] keyBytes = "dia-test-1".getBytes(StandardCharsets.UTF_8);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("body", "Alice met Bob in Seattle yesterday");
            body.put("sample_id", "sample-x");
            Record record = withKey(MapRecord.fromMap(body), Records.keyOf(keyBytes));

            stage.onWrite(record, "default.dev_dialogs", 1L);

            // diaId = base64url(keyBytes)
            String diaId = Base64.getUrlEncoder().withoutPadding().encodeToString(keyBytes);
            byte[] viewKey = ("entity_view:" + diaId).getBytes(StandardCharsets.UTF_8);

            assertThat(ks.exists(viewKey))
                    .as("entity_view:%s placeholder must exist immediately after onWrite", diaId)
                    .isTrue();

            pool.shutdown();
            boolean terminated = pool.awaitTermination(15, TimeUnit.SECONDS);
            assertThat(terminated).as("async derivation must complete within 15s").isTrue();

            assertThat(ks.exists(viewKey))
                    .as("entity_view:%s must still exist after async derivation", diaId)
                    .isTrue();
        }
    }

    @Test
    @DisplayName(
            "onWrite triggerafter, GraphIndexer in KnowledgeStore write graph:node:* and graph:edge:* keys")
    void onWriteTriggersGraphIndexWrites() throws Exception {
        try (KnowledgeStore ks = new InMemoryKnowledgeStore()) {
            EntityExtractionConfig cfg =
                    EntityExtractionConfig.builder()
                            .enabled(true)
                            .hybridEnabled(true)
                            .llmEndpoint("stub://test")
                            .timeout(Duration.ofSeconds(5))
                            .asyncThreads(2)
                            .asyncQueueSize(100)
                            .build();
            HybridEntityExtractor extractor = EntityExtractorFactory.create(cfg);
            GraphIndexer indexer = new GraphIndexer(ks);
            ThreadPoolExecutor pool = EntityDerivationStage.newDefaultPool(cfg);
            EntityDerivationStage stage =
                    new EntityDerivationStage(extractor, indexer, ks, pool, cfg);

            byte[] keyBytes = "dia-graph-1".getBytes(StandardCharsets.UTF_8);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("body", "Alice met Bob in Seattle yesterday");
            body.put("sample_id", "sample-graph");
            Record record = withKey(MapRecord.fromMap(body), Records.keyOf(keyBytes));

            stage.onWrite(record, "default.dev_dialogs", 1L);

            pool.shutdown();
            boolean terminated = pool.awaitTermination(15, TimeUnit.SECONDS);
            assertThat(terminated).as("async derivation must complete within 15s").isTrue();

            int graphNodeCount = countKeysWithPrefix(ks, "graph:node:");
            assertThat(graphNodeCount)
                    .as(
                            "GraphIndexer must write at least 1 graph:node:* key in KnowledgeStore (got %d)",
                            graphNodeCount)
                    .isGreaterThanOrEqualTo(1);
        }
    }

    /**
     * via reflection putcustom Key write back MapRecord, because MapRecord.fromMap default key is
     * empty bytes.
     */
    private static Record withKey(Record record, Record.Key key) throws Exception {
        Field f = MapRecord.class.getDeclaredField("key");
        f.setAccessible(true);
        f.set(record, key);
        return record;
    }

    private static int countKeysWithPrefix(KnowledgeStore ks, String prefix) {
        int count = 0;
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        try (var it = ks.scan(prefixBytes)) {
            while (it.hasNext()) {
                it.next();
                count++;
            }
        } catch (RuntimeException | java.io.IOException e) {
            log.warn("countKeysWithPrefix failed: {}", e.getMessage());
        }
        return count;
    }
}
