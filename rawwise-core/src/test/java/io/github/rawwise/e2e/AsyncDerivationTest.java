package io.github.rawwise.e2e;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.rawwise.axiom.pipeline.derivation.AsyncDerivationQueue;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("AsyncDerivation - async derivation queue end-to-end test")
class AsyncDerivationTest {

    private KnowledgeStore knowledgeStore;
    private AsyncDerivationQueue queue;

    @BeforeEach
    void setUp() {
        knowledgeStore = new InMemoryKnowledgeStore();
    }

    @AfterEach
    void tearDown() {
        if (queue != null) {
            try {
                queue.close();
            } catch (Exception e) {
                /* tearDown best-effort */
            }
        }
        if (knowledgeStore != null) {
            try {
                knowledgeStore.close();
            } catch (Exception e) {
                /* tearDown best-effort */
            }
        }
    }

    @Nested
    @DisplayName("S1-HappyPath - async derivation and watermark update")
    class HappyPath {

        @Test
        @DisplayName(
                "submit immediately non-blocking, derivationcompletes asynchronouslyafter watermark update")
        void submitReturnsImmediatelyDerivationCompletesAsync() throws Exception {
            queue = new AsyncDerivationQueue(knowledgeStore);
            String tableId = "default.test_table";
            String recordKey = "rec:001";
            CountDownLatch latch = new CountDownLatch(1);

            long start = System.nanoTime();
            queue.submit(
                    () -> {
                        try {
                            Thread.sleep(100);
                            queue.updateWatermark(tableId, recordKey);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            latch.countDown();
                        }
                    });
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertThat(elapsedMs).isLessThan(80);
            assertThat(queue.getWatermark(tableId)).isNull();

            assertThat(latch.await(5, SECONDS)).isTrue();

            await().atMost(3, SECONDS)
                    .untilAsserted(
                            () -> assertThat(queue.getWatermark(tableId)).isEqualTo(recordKey));

            byte[] wmKey = "derivation:watermark:".concat(tableId).getBytes(StandardCharsets.UTF_8);
            assertThat(knowledgeStore.get(wmKey)).isPresent();
        }

        @Test
        @DisplayName("multi after commit watermark aftercompleteofrecord")
        void multipleTasksWatermarkReflectsLastCompleted() throws Exception {
            queue = new AsyncDerivationQueue(knowledgeStore);
            String tableId = "default.multi_table";
            CountDownLatch latch = new CountDownLatch(3);

            for (int i = 0; i < 3; i++) {
                String key = "rec:" + i;
                queue.submit(
                        () -> {
                            try {
                                Thread.sleep(50);
                                queue.updateWatermark(tableId, key);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                latch.countDown();
                            }
                        });
            }

            assertThat(latch.await(5, SECONDS)).isTrue();

            await().atMost(5, SECONDS)
                    .untilAsserted(() -> assertThat(queue.getWatermark(tableId)).isNotNull());
            assertThat(queue.getWatermark(tableId)).startsWith("rec:");
        }
    }

    @Nested
    @DisplayName("S3-FaultRecovery - derivation exceptiondoes not block watermark consistency")
    class FaultRecovery {

        @Test
        @DisplayName("derivation throws exception watermark is notupdate")
        void derivationExceptionDoesNotUpdateWatermark() throws Exception {
            queue = new AsyncDerivationQueue(knowledgeStore);
            String tableId = "default.fault_table";
            CountDownLatch latch = new CountDownLatch(1);

            queue.submit(
                    () -> {
                        try {
                            throw new RuntimeException("derivation chain crashed");
                        } finally {
                            latch.countDown();
                        }
                    });

            assertThat(latch.await(5, SECONDS)).isTrue();

            // Negative assertion: latch guarantees task completed without calling updateWatermark
            assertThat(queue.getWatermark(tableId)).isNull();

            byte[] wmKey = "derivation:watermark:".concat(tableId).getBytes(StandardCharsets.UTF_8);
            assertThat(knowledgeStore.get(wmKey)).isEmpty();
        }

        @Test
        @DisplayName("exception completeaftersubsequent can execute update watermark")
        void subsequentTaskSucceedsAfterException() throws Exception {
            queue = new AsyncDerivationQueue(knowledgeStore);
            String tableId = "default.recovery_table";
            CountDownLatch latch1 = new CountDownLatch(1);
            CountDownLatch latch2 = new CountDownLatch(1);

            queue.submit(
                    () -> {
                        try {
                            throw new RuntimeException("first task crashes");
                        } finally {
                            latch1.countDown();
                        }
                    });
            assertThat(latch1.await(5, SECONDS)).isTrue();
            Thread.sleep(100);

            queue.submit(
                    () -> {
                        queue.updateWatermark(tableId, "rec:recovered");
                        latch2.countDown();
                    });
            assertThat(latch2.await(5, SECONDS)).isTrue();

            await().atMost(3, SECONDS)
                    .untilAsserted(
                            () ->
                                    assertThat(queue.getWatermark(tableId))
                                            .isEqualTo("rec:recovered"));
        }
    }

    @Nested
    @DisplayName("S4-Boundary - column CallerRunsPolicy execute")
    class Boundary {

        @Test
        @DisplayName("column permits incall execute(nodata)")
        void queueFullTriggersCallerRunsPolicy() throws Exception {
            queue = new AsyncDerivationQueue(1, knowledgeStore);
            String tableId = "default.boundary_table";
            AtomicBoolean inlineExecuted = new AtomicBoolean(false);
            AtomicInteger completedCount = new AtomicInteger(0);

            CountDownLatch asyncLatch = new CountDownLatch(1);
            queue.submit(
                    () -> {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            asyncLatch.countDown();
                        }
                    });

            Thread.sleep(100);

            String callerThreadName = Thread.currentThread().getName();
            queue.submit(
                    () -> {
                        if (Thread.currentThread().getName().equals(callerThreadName)) {
                            inlineExecuted.set(true);
                        }
                        queue.updateWatermark(tableId, "rec:inline");
                        completedCount.incrementAndGet();
                    });

            assertThat(inlineExecuted.get()).isTrue();
            assertThat(completedCount.get()).isEqualTo(1);
            assertThat(queue.getWatermark(tableId)).isEqualTo("rec:inline");

            assertThat(asyncLatch.await(5, SECONDS)).isTrue();
        }

        @Test
        @DisplayName("close after submit incall execute")
        void closedQueueRunsInline() {
            queue = new AsyncDerivationQueue(knowledgeStore);
            queue.close();

            AtomicBoolean executed = new AtomicBoolean(false);
            queue.submit(() -> executed.set(true));

            assertThat(executed.get()).isTrue();
            assertThat(queue.isClosed()).isTrue();
        }

        @Test
        @DisplayName("watermark query null")
        void emptyWatermarkReturnsNull() {
            queue = new AsyncDerivationQueue(knowledgeStore);
            assertThat(queue.getWatermark("nonexistent_table")).isNull();
        }
    }
}
