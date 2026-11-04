package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("RetrievalFeedbackCollector E2E - record/scan/count")
class RetrievalFeedbackCollectorE2ETest {

    private KnowledgeStore knowledgeStore;
    private RetrievalFeedbackCollector collector;

    @BeforeEach
    void setUp() {
        knowledgeStore = new InMemoryKnowledgeStore();
        collector = new RetrievalFeedbackCollector(knowledgeStore, "test_table");
    }

    @AfterEach
    void tearDown() throws Exception {
        knowledgeStore.close();
    }

    private FeedbackRecord build(FeedbackType type) {
        return FeedbackRecord.builder()
                .recordId(UUID.randomUUID().toString())
                .query("q")
                .retrievedHitIds(List.of("h1", "h2"))
                .userExpectedHitId("h2")
                .feedbackType(type)
                .correctionText(null)
                .timestamp(Instant.now())
                .metadata(Map.of())
                .build();
    }

    @Test
    @DisplayName("recordFeedback for 3 entries -> recount==3, scanRecent returns 3")
    void recordAndScanThree() {
        collector.recordFeedback(build(FeedbackType.NEGATIVE));
        collector.recordFeedback(build(FeedbackType.POSITIVE));
        collector.recordFeedback(build(FeedbackType.CORRECTION));

        assertThat(collector.recount()).isEqualTo(3L);
        List<FeedbackRecord> scanned = collector.scanRecent(10, null);
        assertThat(scanned).hasSize(3);
    }

    @Test
    @DisplayName("count() returns 0 on empty store and triggers recount lazily")
    void countOnEmpty() {
        assertThat(collector.count()).isEqualTo(0L);
    }

    @Test
    @DisplayName("scanRecent with limit caps the result size")
    void scanRecentRespectsLimit() {
        for (int i = 0; i < 5; i++) {
            collector.recordFeedback(build(FeedbackType.POSITIVE));
        }
        List<FeedbackRecord> scanned = collector.scanRecent(3, null);
        assertThat(scanned).hasSize(3);
    }

    @Test
    @DisplayName("concurrent recordFeedback: final recount equals total writes")
    void concurrentRecordFinalRecountConsistent() throws Exception {
        int threadCount = 50;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            pool.submit(
                    () -> {
                        try {
                            start.await();
                            collector.recordFeedback(build(FeedbackType.POSITIVE));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
        }
        start.countDown();
        assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(collector.recount()).isEqualTo(50L);
    }
}
