package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("RerankerTraining Integration E2E - feedback -> train -> model -> AB switch")
class RerankerTrainingSkeletonTest {

    private KnowledgeStore knowledgeStore;
    private Metastore metastore;
    private RetrievalFeedbackCollector collector;
    private RerankerTrainingScheduler scheduler;
    private RerankerAbSwitcher switcher;
    private AtomicInteger cacheInvalidations;

    @BeforeEach
    void setUp() {
        knowledgeStore = new InMemoryKnowledgeStore();
        metastore = new InMemoryMetastore();
        collector = new RetrievalFeedbackCollector(knowledgeStore, "integration_test_table");
        cacheInvalidations = new AtomicInteger();

        RerankerTrainingConfig cfg =
                RerankerTrainingConfig.builder()
                        .enabled(true)
                        .minSamples(3)
                        .minInterval(Duration.ZERO)
                        .modelOutputPath("/tmp/integration/reranker")
                        .currentModelVersion("v0")
                        .abTrafficPercent(0.50)
                        .build();
        scheduler =
                new RerankerTrainingScheduler(
                        collector, cfg, metastore, cacheInvalidations::incrementAndGet);
        scheduler.setKnowledgeStore(knowledgeStore);
        switcher = new RerankerAbSwitcher(0.50);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (scheduler != null) scheduler.stop();
        knowledgeStore.close();
        metastore.close();
    }

    private FeedbackRecord positive(String query, List<String> retrieved, String expected) {
        return FeedbackRecord.builder()
                .recordId(UUID.randomUUID().toString())
                .query(query)
                .retrievedHitIds(retrieved)
                .userExpectedHitId(expected)
                .feedbackType(FeedbackType.POSITIVE)
                .timestamp(Instant.now())
                .metadata(Map.of())
                .build();
    }

    private FeedbackRecord negative(String query, List<String> retrieved, String expected) {
        return FeedbackRecord.builder()
                .recordId(UUID.randomUUID().toString())
                .query(query)
                .retrievedHitIds(retrieved)
                .userExpectedHitId(expected)
                .feedbackType(FeedbackType.NEGATIVE)
                .timestamp(Instant.now())
                .metadata(Map.of())
                .build();
    }

    private FeedbackRecord correction(
            String query, List<String> retrieved, String expected, String correctionText) {
        return FeedbackRecord.builder()
                .recordId(UUID.randomUUID().toString())
                .query(query)
                .retrievedHitIds(retrieved)
                .userExpectedHitId(expected)
                .feedbackType(FeedbackType.CORRECTION)
                .correctionText(correctionText)
                .timestamp(Instant.now())
                .metadata(Map.of())
                .build();
    }

    @Test
    @DisplayName(
            "full chain: collect 6 feedback -> trigger training -> model has weights -> AB switch routes traffic")
    void fullChainFeedbackTrainModelSwitch() throws Exception {
        collector.recordFeedback(positive("query1", List.of("hit_a", "hit_b"), "hit_a"));
        collector.recordFeedback(positive("query2", List.of("hit_a", "hit_c"), "hit_a"));
        collector.recordFeedback(negative("query3", List.of("hit_b", "hit_d"), "hit_a"));
        collector.recordFeedback(negative("query4", List.of("hit_c", "hit_e"), "hit_a"));
        collector.recordFeedback(
                correction("query5", List.of("hit_b", "hit_f"), "hit_a", "should be hit_a"));
        collector.recordFeedback(positive("query6", List.of("hit_a", "hit_g"), "hit_a"));
        collector.recount();

        assertThat(collector.count()).isEqualTo(6L);

        RerankerTrainingTrigger trigger = scheduler.triggerOnDemand();

        assertThat(trigger.version()).isNotEqualTo("v0");
        assertThat(trigger.feedbackCount()).isEqualTo(6L);

        assertThat(metastore.exists(b(RerankerTrainingScheduler.KEY_LAST_TRAIN_AT_MS))).isTrue();
        assertThat(
                        metastore.exists(
                                b(RerankerTrainingScheduler.KEY_META_PREFIX + trigger.version())))
                .isTrue();
        assertThat(metastore.exists(b(RerankerTrainingScheduler.KEY_LATEST_VERSION))).isTrue();
        assertThat(cacheInvalidations.get()).isEqualTo(1);

        RerankerModelMetadata model = scheduler.latestModel().orElseThrow();
        assertThat(model.version()).isEqualTo(trigger.version());
        assertThat(model.trainingSampleCount()).isEqualTo(6);
        assertThat(model.weights()).isNotEmpty();
        assertThat(model.weights()).containsKey("hit_a");
        assertThat(model.weights().get("hit_a")).isGreaterThan(0.0);
        assertThat(model.weights()).containsKey("hit_b");
        assertThat(model.weights().get("hit_b")).isLessThan(0.0);
        assertThat(model.validationMetrics()).containsKey("precision_at_k");
        assertThat(model.validationMetrics().get("precision_at_k")).isGreaterThan(0.0);

        RerankerModelMetadata loaded = scheduler.loadModelMetadata(trigger.version()).orElseThrow();
        assertThat(loaded.version()).isEqualTo(model.version());
        assertThat(loaded.weights()).isEqualTo(model.weights());

        RerankerModelMetadata baseline =
                RerankerModelMetadata.empty("v0", "/tmp/baseline", Instant.now());
        switcher.setCurrentModel(baseline);
        switcher.setCandidateModel(model);

        int candidateCount = 0;
        int currentCount = 0;
        for (int i = 0; i < 1000; i++) {
            String reqId = "req-" + i;
            RerankerModelMetadata assigned = switcher.allocateTraffic(reqId);
            assertThat(assigned).isNotNull();
            if (assigned.version().equals(model.version())) {
                candidateCount++;
            } else {
                currentCount++;
            }
        }
        assertThat(candidateCount).isBetween(400, 600);
        assertThat(currentCount).isBetween(400, 600);

        switcher.promoteCandidate();
        assertThat(switcher.currentModel().orElseThrow().version()).isEqualTo(model.version());
        assertThat(switcher.candidateModel()).isEmpty();
        assertThat(switcher.getAbTrafficPercent()).isEqualTo(0.0);

        for (int i = 0; i < 100; i++) {
            RerankerModelMetadata assigned = switcher.allocateTraffic("req-" + i);
            assertThat(assigned.version()).isEqualTo(model.version());
        }
    }

    @Test
    @DisplayName(
            "training weights: positive boosts expected, negative penalizes wrong hits, correction boosts strongest")
    void trainingWeightsDirectionality() {
        collector.recordFeedback(positive("q", List.of("good", "bad"), "good"));
        collector.recordFeedback(negative("q", List.of("bad", "wrong"), "good"));
        collector.recordFeedback(correction("q", List.of("wrong", "bad"), "good", "fix"));
        collector.recount();

        RerankerTrainingTrigger trigger = scheduler.triggerOnDemand();
        RerankerModelMetadata model = scheduler.latestModel().orElseThrow();

        double goodWeight = model.weights().get("good");
        double badWeight = model.weights().get("bad");
        double wrongWeight = model.weights().get("wrong");

        assertThat(goodWeight).isGreaterThan(0.0);
        assertThat(badWeight).isLessThan(0.0);
        assertThat(wrongWeight).isLessThan(0.0);

        assertThat(goodWeight).isGreaterThan(badWeight);
        assertThat(goodWeight).isGreaterThan(wrongWeight);

        assertThat(model.validationMetrics().get("precision_at_k")).isEqualTo(0.3333);
        assertThat(model.validationMetrics().get("negative_count")).isEqualTo(1.0);
        assertThat(model.validationMetrics().get("correction_count")).isEqualTo(1.0);
    }

    @Test
    @DisplayName("insufficient feedback -> no training, version stays v0, no metadata written")
    void insufficientFeedbackNoTraining() throws Exception {
        collector.recordFeedback(positive("q", List.of("h"), "h"));
        collector.recount();

        RerankerTrainingTrigger trigger = scheduler.triggerOnDemand();

        assertThat(trigger.version()).isEqualTo("v0");
        assertThat(scheduler.latestModel()).isEmpty();
        assertThat(metastore.exists(b(RerankerTrainingScheduler.KEY_LAST_TRAIN_AT_MS))).isFalse();
        assertThat(cacheInvalidations.get()).isEqualTo(0);
    }

    @Test
    @DisplayName("rollback clears candidate and routes all traffic to current model")
    void rollbackRestoresCurrentModel() {
        collector.recordFeedback(positive("q1", List.of("h1"), "h1"));
        collector.recordFeedback(positive("q2", List.of("h1"), "h1"));
        collector.recordFeedback(positive("q3", List.of("h1"), "h1"));
        collector.recordFeedback(positive("q4", List.of("h1"), "h1"));
        collector.recordFeedback(positive("q5", List.of("h1"), "h1"));
        collector.recount();

        RerankerTrainingTrigger trigger = scheduler.triggerOnDemand();
        RerankerModelMetadata trained = scheduler.latestModel().orElseThrow();

        RerankerModelMetadata baseline =
                RerankerModelMetadata.empty("v0", "/tmp/baseline", Instant.now());
        switcher.setCurrentModel(baseline);
        switcher.setCandidateModel(trained);
        switcher.setAbTrafficPercent(0.50);

        assertThat(switcher.candidateModel()).isPresent();

        switcher.rollback();

        assertThat(switcher.candidateModel()).isEmpty();
        assertThat(switcher.getAbTrafficPercent()).isEqualTo(0.0);
        for (int i = 0; i < 100; i++) {
            RerankerModelMetadata assigned = switcher.allocateTraffic("req-" + i);
            assertThat(assigned.version()).isEqualTo("v0");
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
