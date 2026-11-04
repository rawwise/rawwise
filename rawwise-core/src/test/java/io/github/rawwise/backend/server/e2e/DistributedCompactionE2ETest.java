package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.compaction.DistributedCompactionScheduler;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Phase 3.10-T3: distributed Compaction mutexlock E2E test.
 *
 * <p>validation Compaction lockcore semantics(§3.10):
 *
 * <ol>
 *   <li>S1: acquirelocksuccess
 *   <li>S2: lockbeother executor holdhaswhenacquirefailfail
 *   <li>S3: renewsuccess
 *   <li>S4: lockpassexpiredafterother executor takeover
 *   <li>S5: releaselockafterother executor canacquire
 *   <li>S6: andacquireonlyhas 1 success
 * </ol>
 */
@DisplayName("Phase 3.10-T3: Distributed Compaction Lock E2E")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DistributedCompactionE2ETest {

    private DistributedCompactionScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) scheduler.close();
    }

    @Test
    @Order(1)
    @DisplayName("S1: Acquire lock success")
    void testAcquireSuccess() {
        scheduler = new DistributedCompactionScheduler(60, 300, 60);
        assertThat(scheduler.tryAcquireCompactionLock(0, "exec-A")).isTrue();
        assertThat(scheduler.getLockState(0)).isPresent();
        assertThat(scheduler.getLockState(0).get().executorId()).isEqualTo("exec-A");
    }

    @Test
    @Order(2)
    @DisplayName("S2: Acquire fails when held by another executor")
    void testAcquireFailsWhenBusy() {
        scheduler = new DistributedCompactionScheduler(60, 300, 60);
        assertThat(scheduler.tryAcquireCompactionLock(0, "exec-A")).isTrue();
        assertThat(scheduler.tryAcquireCompactionLock(0, "exec-B")).isFalse();
    }

    @Test
    @Order(3)
    @DisplayName("S3: Renew lock success")
    void testRenewSuccess() {
        scheduler = new DistributedCompactionScheduler(60, 300, 60);
        scheduler.tryAcquireCompactionLock(0, "exec-A");
        assertThat(scheduler.renewLock(0, "exec-A")).isTrue();
    }

    @Test
    @Order(4)
    @DisplayName("S4: Expired lock takeover by another executor")
    void testExpiredLockTakeover() throws Exception {
        scheduler = new DistributedCompactionScheduler(60, 1, 60);
        scheduler.tryAcquireCompactionLock(0, "exec-A");
        Thread.sleep(1500);
        assertThat(scheduler.tryAcquireCompactionLock(0, "exec-B"))
                .as("exec-B should acquire expired lock")
                .isTrue();
    }

    @Test
    @Order(5)
    @DisplayName("S5: Release lock -> another executor can acquire")
    void testReleaseThenAcquire() {
        scheduler = new DistributedCompactionScheduler(60, 300, 60);
        scheduler.tryAcquireCompactionLock(0, "exec-A");
        scheduler.releaseLock(0, "exec-A");
        assertThat(scheduler.tryAcquireCompactionLock(0, "exec-B"))
                .as("exec-B should acquire after release")
                .isTrue();
    }

    @Test
    @Order(6)
    @DisplayName("S6: Concurrent acquire -> only 1 succeeds")
    void testConcurrentAcquire() throws Exception {
        scheduler = new DistributedCompactionScheduler(60, 300, 60);
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        for (int i = 0; i < threads; i++) {
            final String execId = "exec-" + i;
            pool.submit(
                    () -> {
                        try {
                            if (scheduler.tryAcquireCompactionLock(0, execId)) {
                                successCount.incrementAndGet();
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
        }
        latch.await(5, TimeUnit.SECONDS);
        pool.shutdown();
        assertThat(successCount.get()).as("only 1 thread should acquire the lock").isEqualTo(1);
    }

    @Test
    @Order(7)
    @DisplayName("S7: Renew by wrong executor fails")
    void testRenewByWrongExecutor() {
        scheduler = new DistributedCompactionScheduler(60, 300, 60);
        scheduler.tryAcquireCompactionLock(0, "exec-A");
        assertThat(scheduler.renewLock(0, "exec-B"))
                .as("renew by wrong executor should fail")
                .isFalse();
    }

    @Test
    @Order(8)
    @DisplayName("S8: Different shards can hold locks simultaneously")
    void testDifferentShardsConcurrent() {
        scheduler = new DistributedCompactionScheduler(60, 300, 60);
        assertThat(scheduler.tryAcquireCompactionLock(0, "exec-A")).isTrue();
        assertThat(scheduler.tryAcquireCompactionLock(1, "exec-B")).isTrue();
        assertThat(scheduler.getActiveLocks()).hasSize(2);
    }
}
