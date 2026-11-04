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
@DisplayName(
        "RerankerTrainingScheduler E2E - triggerOnDemand persists metadata + invalidates cache")
class RerankerTrainingSchedulerE2ETest {

    private KnowledgeStore knowledgeStore;
    private Metastore metastore;
    private RetrievalFeedbackCollector collector;
    private AtomicInteger cacheInvalidations;
    private RerankerTrainingScheduler scheduler;

    @BeforeEach
    void setUp() {
        knowledgeStore = new InMemoryKnowledgeStore();
        metastore = new InMemoryMetastore();
        collector = new RetrievalFeedbackCollector(knowledgeStore, "test_table");
        cacheInvalidations = new AtomicInteger();

        RerankerTrainingConfig cfg =
                RerankerTrainingConfig.builder()
                        .enabled(true)
                        .minSamples(3)
                        .minInterval(Duration.ZERO)
                        .modelOutputPath("/tmp/test/reranker")
                        .currentModelVersion("v0")
                        .abTrafficPercent(0.10)
                        .build();
        scheduler =
                new RerankerTrainingScheduler(
                        collector, cfg, metastore, cacheInvalidations::incrementAndGet);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (scheduler != null) scheduler.stop();
        knowledgeStore.close();
        metastore.close();
    }

    private FeedbackRecord recordOf(FeedbackType type) {
        return FeedbackRecord.builder()
                .recordId(UUID.randomUUID().toString())
                .query("q")
                .retrievedHitIds(List.of("h1"))
                .userExpectedHitId("h1")
                .feedbackType(type)
                .timestamp(Instant.now())
                .metadata(Map.of())
                .build();
    }

    @Test
    @DisplayName(
            "triggerOnDemand with no feedback -> no training (no version bump, no metadata key)")
    void noFeedbackNoTraining() throws Exception {
        RerankerTrainingTrigger trigger = scheduler.triggerOnDemand();
        assertThat(trigger.version()).isEqualTo("v0");
        // No reranker:meta:v1 key in metastore
        assertThat(metastore.exists(key(RerankerTrainingScheduler.KEY_META_PREFIX + "v1")))
                .isFalse();
        assertThat(cacheInvalidations.get()).isEqualTo(0);
    }

    @Test
    @DisplayName(
            "3 feedback + triggerOnDemand -> training fires, metadata + last_train_at_ms written, cache invalidator called")
    void enoughFeedbackTriggersTraining() throws Exception {
        collector.recordFeedback(recordOf(FeedbackType.NEGATIVE));
        collector.recordFeedback(recordOf(FeedbackType.POSITIVE));
        collector.recordFeedback(recordOf(FeedbackType.CORRECTION));
        collector.recount();

        RerankerTrainingTrigger trigger = scheduler.triggerOnDemand();
        assertThat(trigger.version()).isNotEqualTo("v0");
        assertThat(trigger.feedbackCount()).isGreaterThanOrEqualTo(3L);

        assertThat(metastore.exists(key(RerankerTrainingScheduler.KEY_LAST_TRAIN_AT_MS))).isTrue();
        assertThat(
                        metastore.exists(
                                key(RerankerTrainingScheduler.KEY_META_PREFIX + trigger.version())))
                .isTrue();
        assertThat(cacheInvalidations.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("subsequent triggerOnDemand with minInterval=ZERO bumps version again")
    void secondTriggerBumpsVersion() throws Exception {
        for (int i = 0; i < 3; i++) {
            collector.recordFeedback(recordOf(FeedbackType.POSITIVE));
        }
        collector.recount();

        RerankerTrainingTrigger first = scheduler.triggerOnDemand();
        RerankerTrainingTrigger second = scheduler.triggerOnDemand();
        assertThat(second.version()).isNotEqualTo(first.version());
        assertThat(cacheInvalidations.get()).isEqualTo(2);
    }

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
