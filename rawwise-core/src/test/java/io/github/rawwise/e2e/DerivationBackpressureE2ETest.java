package io.github.rawwise.e2e;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.rawwise.axiom.pipeline.derivation.AsyncDerivationQueue;
import io.github.rawwise.axiom.pipeline.derivation.DerivationLoadMonitor;
import io.github.rawwise.axiom.pipeline.derivation.DerivationTask;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DerivationBackpressure - derivation backpressure degradationend-to-end test")
class DerivationBackpressureE2ETest {

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

    private DerivationTask createTask(
            String tableId,
            String recordKey,
            AtomicInteger fullCounter,
            AtomicInteger lightweightCounter,
            CountDownLatch latch,
            boolean updateWatermark) {
        return new DerivationTask() {
            @Override
            public void runFull() {
                try {
                    Thread.sleep(100);
                    fullCounter.incrementAndGet();
                    if (updateWatermark) {
                        queue.updateWatermark(tableId, recordKey);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    if (latch != null) {
                        latch.countDown();
                    }
                }
            }

            @Override
            public void runLightweight() {
                lightweightCounter.incrementAndGet();
                if (latch != null) {
                    latch.countDown();
                }
            }

            @Override
            public String tableId() {
                return tableId;
            }

            @Override
            public String recordKey() {
                return recordKey;
            }
        };
    }

    @Nested
    @DisplayName("S1-LightweightDegradation - passwhenlightamountdegradationdoes not block ingest")
    class LightweightDegradation {

        @Test
        @DisplayName("systempasswhenall submit fastreturns,full notbeexecute")
        void overloadedSystemDegradesToLightweight() {
            DerivationLoadMonitor monitor = new DerivationLoadMonitor(-1.0, -1.0);
            queue = new AsyncDerivationQueue(2, knowledgeStore, monitor, 2);

            AtomicInteger fullCounter = new AtomicInteger(0);
            AtomicInteger lightweightCounter = new AtomicInteger(0);

            for (int i = 0; i < 5; i++) {
                DerivationTask task =
                        createTask(
                                "default.s1_table",
                                "rec:" + i,
                                fullCounter,
                                lightweightCounter,
                                null,
                                false);
                long start = System.nanoTime();
                queue.submit(task);
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                assertThat(elapsedMs).isLessThan(50);
            }

            assertThat(fullCounter.get()).isZero();
            assertThat(lightweightCounter.get()).isGreaterThan(0);
        }
    }

    @Nested
    @DisplayName("S2-PendingQueueFull - handlingqueue fullafterdegrades tolightamount")
    class PendingQueueFull {

        @Test
        @DisplayName(
                "handlingqueuefullaftermanyanymustdegrades tolightamountand submit does not block")
        void pendingQueueFullDegradesToLightweight() {
            DerivationLoadMonitor monitor = new DerivationLoadMonitor(-1.0, -1.0);
            queue = new AsyncDerivationQueue(2, knowledgeStore, monitor, 3);

            AtomicInteger fullCounter = new AtomicInteger(0);
            AtomicInteger lightweightCounter = new AtomicInteger(0);

            for (int i = 0; i < 10; i++) {
                DerivationTask task =
                        createTask(
                                "default.s2_table",
                                "rec:" + i,
                                fullCounter,
                                lightweightCounter,
                                null,
                                false);
                long start = System.nanoTime();
                queue.submit(task);
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                assertThat(elapsedMs).isLessThan(50);
            }

            assertThat(fullCounter.get()).isZero();
            assertThat(lightweightCounter.get()).isGreaterThan(0);
        }
    }

    @Nested
    @DisplayName("S3-NormalLoad - positiveoftennegativeuse full andupdate watermark")
    class NormalLoad {

        @Test
        @DisplayName("systemnotpasswhenallanymustgo full andupdate watermark")
        void normalLoadUsesFullPath() throws Exception {
            DerivationLoadMonitor monitor = new DerivationLoadMonitor(100.0, 100.0);
            queue = new AsyncDerivationQueue(10, knowledgeStore, monitor, 5);

            AtomicInteger fullCounter = new AtomicInteger(0);
            AtomicInteger lightweightCounter = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(3);
            String tableId = "default.s3_table";

            for (int i = 0; i < 3; i++) {
                DerivationTask task =
                        createTask(
                                tableId, "rec:" + i, fullCounter, lightweightCounter, latch, true);
                queue.submit(task);
            }

            assertThat(latch.await(5, SECONDS)).isTrue();

            await().atMost(3, SECONDS)
                    .untilAsserted(() -> assertThat(queue.getWatermark(tableId)).isNotNull());
            assertThat(queue.getWatermark(tableId)).startsWith("rec:");

            byte[] wmKey = "derivation:watermark:".concat(tableId).getBytes(StandardCharsets.UTF_8);
            assertThat(knowledgeStore.get(wmKey)).isPresent();

            assertThat(fullCounter.get()).isEqualTo(3);
            assertThat(lightweightCounter.get()).isZero();
        }
    }
}
