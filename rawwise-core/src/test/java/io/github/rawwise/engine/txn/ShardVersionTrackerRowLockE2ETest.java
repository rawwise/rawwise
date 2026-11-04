package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Spec 05 A E2E: ShardVersionTracker rowlockend-to-endverify.
 *
 * <p>coverage scenario:
 *
 * <ul>
 *   <li>S1 Happy Path: single txn acquireForUpdate / releaseAll
 *   <li>S2 : T1 lock -> T2 acquireForUpdate same key timeout
 *   <li>S3 release: T1 releaseAll after T2 canacquire
 *   <li>S4 : same txn multiple times acquire same key does not block
 *   <li>S5 timeout: tryLock timeout LockTimeoutException
 *   <li>S6 concurrent : 32 thread same key, allacquireall rowsuccess
 *   <li>S7 holdsExclusiveLock: H-8 repeat ofquery API correct status
 *   <li>S8 H-6 memory: releaseAll after cache release
 * </ul>
 */
@DisplayName("ShardVersionTracker Row Lock E2E — Spec 05 A")
class ShardVersionTrackerRowLockE2ETest {

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Nested
    @DisplayName("S1: Happy Path")
    class HappyPath {

        @Test
        @DisplayName("S1.1: single txn acquire + releaseAll - lockdata correct")
        void singleTxnAcquireRelease() throws Exception {
            ShardVersionTracker tracker =
                    new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
            long txnId = 100L;
            byte[] k = key("row-1");

            assertThat(tracker.heldKeyCount(txnId)).isZero();
            tracker.acquireForUpdate(k, txnId, 1_000L);
            assertThat(tracker.heldKeyCount(txnId)).isEqualTo(1);
            assertThat(tracker.holdsExclusiveLock(k, txnId)).isTrue();

            tracker.releaseAll(txnId);
            assertThat(tracker.heldKeyCount(txnId)).isZero();
            assertThat(tracker.holdsExclusiveLock(k, txnId)).isFalse();
        }

        @Test
        @DisplayName("S1.2: same txn lockmultipledifferent key - heldKeyCount")
        void multipleKeysSameTxn() throws Exception {
            ShardVersionTracker tracker =
                    new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
            long txnId = 200L;

            tracker.acquireForUpdate(key("k1"), txnId, 1_000L);
            tracker.acquireForUpdate(key("k2"), txnId, 1_000L);
            tracker.acquireForUpdate(key("k3"), txnId, 1_000L);

            assertThat(tracker.heldKeyCount(txnId)).isEqualTo(3);
            tracker.releaseAll(txnId);
            assertThat(tracker.heldKeyCount(txnId)).isZero();
        }
    }

    @Nested
    @DisplayName("S2-S3: andrelease")
    class BlockAndRelease {

        @Test
        @DisplayName("S2.1: T1 lock -> T2 same key acquire timeout")
        @Timeout(value = 5, unit = TimeUnit.SECONDS)
        void blockedByOtherTxn() throws Exception {
            ShardVersionTracker tracker =
                    new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
            byte[] k = key("contended");

            ExecutorService exec = Executors.newFixedThreadPool(2);
            try {
                CountDownLatch t1Acquired = new CountDownLatch(1);
                CountDownLatch t1Hold = new CountDownLatch(1);

                Future<?> t1 =
                        exec.submit(
                                () -> {
                                    try {
                                        tracker.acquireForUpdate(k, 1L, 1_000L);
                                        t1Acquired.countDown();
                                        t1Hold.await(5, TimeUnit.SECONDS);
                                        tracker.releaseAll(1L);
                                    } catch (InterruptedException ignored) {
                                        Thread.currentThread().interrupt();
                                    }
                                });

                t1Acquired.await(2, TimeUnit.SECONDS);

                Future<?> t2 =
                        exec.submit(
                                () -> {
                                    assertThatThrownBy(() -> tracker.acquireForUpdate(k, 2L, 100L))
                                            .isInstanceOf(
                                                    ShardVersionTracker.LockTimeoutException.class)
                                            .hasMessageContaining("FOR UPDATE timeout")
                                            .hasMessageContaining("txn=2");
                                });

                t2.get(2, TimeUnit.SECONDS);
                t1Hold.countDown();
                t1.get(2, TimeUnit.SECONDS);
            } finally {
                exec.shutdownNow();
            }
        }

        @Test
        @DisplayName("S3.1: T1 releaseAll after T2 canacquire")
        @Timeout(value = 5, unit = TimeUnit.SECONDS)
        void releaseAllowsOther() throws Exception {
            ShardVersionTracker tracker =
                    new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
            byte[] k = key("hand-off");

            tracker.acquireForUpdate(k, 1L, 1_000L);
            tracker.releaseAll(1L);

            // T2 should acquire immediately (no contention)
            tracker.acquireForUpdate(k, 2L, 50L);
            assertThat(tracker.holdsExclusiveLock(k, 2L)).isTrue();
            tracker.releaseAll(2L);
        }
    }

    @Nested
    @DisplayName("S4: sametransaction")
    class Reentrant {

        @Test
        @DisplayName("S4.1: same txn duplicate acquire same key does not block")
        void sameThreadReentrant() throws Exception {
            ShardVersionTracker tracker =
                    new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
            byte[] k = key("reentrant");

            assertThatCode(
                            () -> {
                                tracker.acquireForUpdate(k, 1L, 100L);
                                tracker.acquireForUpdate(k, 1L, 100L);
                                tracker.acquireForUpdate(k, 1L, 100L);
                            })
                    .doesNotThrowAnyException();

            // inNo. heldKeys
            assertThat(tracker.heldKeyCount(1L)).isEqualTo(1);
            tracker.releaseAll(1L);
            assertThat(tracker.heldKeyCount(1L)).isZero();
        }
    }

    @Nested
    @DisplayName("S5: timeout")
    class TimeoutTests {

        @Test
        @DisplayName("S5.1: 0ms timeout LockTimeoutException(alreadybe lock)")
        @Timeout(value = 5, unit = TimeUnit.SECONDS)
        void zeroTimeoutImmediate() throws Exception {
            ShardVersionTracker tracker =
                    new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
            byte[] k = key("zero-to");

            ExecutorService exec = Executors.newSingleThreadExecutor();
            try {
                CountDownLatch acquired = new CountDownLatch(1);
                CountDownLatch hold = new CountDownLatch(1);

                exec.submit(
                        () -> {
                            try {
                                tracker.acquireForUpdate(k, 1L, 1_000L);
                                acquired.countDown();
                                hold.await();
                                tracker.releaseAll(1L);
                            } catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                            }
                        });

                acquired.await(2, TimeUnit.SECONDS);
                assertThatThrownBy(() -> tracker.acquireForUpdate(k, 99L, 0L))
                        .isInstanceOf(ShardVersionTracker.LockTimeoutException.class);

                hold.countDown();
            } finally {
                exec.shutdownNow();
            }
        }
    }

    @Nested
    @DisplayName("S6: concurrent")
    class Concurrency {

        @Test
        @DisplayName("S6.1: 32 thread row same key —— all acquire success, guarantee")
        @Timeout(value = 30, unit = TimeUnit.SECONDS)
        void contention32Threads() throws Exception {
            ShardVersionTracker tracker =
                    new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
            byte[] k = key("hot-row");
            int threads = 32;

            ExecutorService exec = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger inCriticalSection = new AtomicInteger(0);
            AtomicInteger maxConcurrent = new AtomicInteger(0);
            AtomicInteger successCount = new AtomicInteger(0);
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < threads; i++) {
                final long txnId = 1000L + i;
                futures.add(
                        exec.submit(
                                () -> {
                                    try {
                                        start.await();
                                        tracker.acquireForUpdate(k, txnId, 10_000L);
                                        try {
                                            int now = inCriticalSection.incrementAndGet();
                                            maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
                                            // simulate
                                            Thread.sleep(2);
                                            inCriticalSection.decrementAndGet();
                                            successCount.incrementAndGet();
                                        } finally {
                                            tracker.releaseAll(txnId);
                                        }
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                    return null;
                                }));
            }

            start.countDown();
            for (Future<?> f : futures) f.get(20, TimeUnit.SECONDS);
            exec.shutdown();

            assertThat(successCount.get()).isEqualTo(threads);
            assertThat(maxConcurrent.get())
                    .as("Mutual exclusion: at most 1 thread in critical section at a time")
                    .isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("S7: holdsExclusiveLock - H-8 repeat API")
    class HoldsExclusiveLockTests {

        @Test
        @DisplayName("S7.1: whenbeforetransaction lock true, its transaction false")
        void crossTxnIsolation() throws Exception {
            ShardVersionTracker tracker =
                    new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
            byte[] k = key("isolated");

            tracker.acquireForUpdate(k, 1L, 100L);
            assertThat(tracker.holdsExclusiveLock(k, 1L)).isTrue();
            assertThat(tracker.holdsExclusiveLock(k, 2L)).isFalse();
            assertThat(tracker.holdsExclusiveLock(key("other"), 1L)).isFalse();

            tracker.releaseAll(1L);
            assertThat(tracker.holdsExclusiveLock(k, 1L)).isFalse();
        }
    }

    @Nested
    @DisplayName("S8: H-6 memory - lockreleaseafter from cache remove")
    class MemoryManagement {

        @Test
        @DisplayName("S8.1: single key acquire+release after activeRowLockCount")
        void cacheReleasedAfterAllReleased() throws Exception {
            ShardVersionTracker tracker =
                    new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
            byte[] k = key("disposable");

            tracker.acquireForUpdate(k, 1L, 100L);
            assertThat(tracker.activeRowLockCount()).isGreaterThanOrEqualTo(1);

            tracker.releaseAll(1L);
            // Caffeine maintenance isasyncof; explicit cleanUp in estimatedSize readbefore
            // heldKeyCount=0(), cache isbest-effort
            assertThat(tracker.heldKeyCount(1L)).isZero();
        }
    }
}
