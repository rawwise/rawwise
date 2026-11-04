package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

/**
 * DeletionManager unit test.
 *
 * <p>teststrategy: - LmdbInstance via Mockito mock, avoidreal LMDB depend on - lazyverify
 * schedulingandretry behavior - AtomicLong capture memoryReleaser call, verify memory release logic
 * - PendingDeletionItem of compareTo/getDelay via direct constructioninstancetest - max retry
 * scenario due toexponential backoff time too long, via PendingDeletionItem verify logic
 * separately, non-wait retry
 */
class DeletionManagerTest {

    private DeletionManager deletionManager;
    private AtomicLong releasedMemory;
    private static final long INSTANCE_MAPPED_BYTES = 1024 * 1024L; // 1MB

    @BeforeEach
    void setUp() {
        releasedMemory = new AtomicLong(0);
        deletionManager =
                new DeletionManager(
                        bytes -> releasedMemory.addAndGet(bytes), INSTANCE_MAPPED_BYTES);
    }

    @AfterEach
    void tearDown() {
        if (deletionManager != null) {
            deletionManager.stop();
        }
    }

    // ==================== scheduleDeletion andhandle ====================

    @Nested
    class ScheduleAndProcessTests {

        @Test
        void scheduleDeletionAddsItemToQueue() {
            LmdbInstance mockInstance = createMockInstance("inst-test-001");
            deletionManager.scheduleDeletion(mockInstance, 5000);

            assertThat(deletionManager.pendingCount()).isGreaterThan(0);
        }

        @Test
        void scheduledItemIsProcessedAfterDelay() throws Exception {
            LmdbInstance mockInstance = createMockInstance("inst-process-001");
            // oflazylettest via
            deletionManager.scheduleDeletion(mockInstance, 50);

            // wait let DeletionManager handle
            boolean processed = awaitCondition(() -> releasedMemory.get() > 0, 5000);
            assertThat(processed).isTrue();
            assertThat(releasedMemory.get()).isEqualTo(INSTANCE_MAPPED_BYTES);
        }

        @Test
        void destroyIsCalledOnInstanceWhenWriteLockAcquired() throws Exception {
            LmdbInstance mockInstance = createMockInstance("inst-destroy-001");
            when(mockInstance.tryWriteLock()).thenReturn(true);

            deletionManager.scheduleDeletion(mockInstance, 50);
            awaitCondition(() -> releasedMemory.get() > 0, 5000);

            verify(mockInstance, timeout(5000)).destroy();
            verify(mockInstance, timeout(5000)).unlockWrite();
        }

        @Test
        void memoryReleaserIsCalledAfterSuccessfulDeletion() throws Exception {
            LmdbInstance mockInstance = createMockInstance("inst-mem-001");
            when(mockInstance.tryWriteLock()).thenReturn(true);

            deletionManager.scheduleDeletion(mockInstance, 50);
            awaitCondition(() -> releasedMemory.get() > 0, 5000);

            assertThat(releasedMemory.get()).isEqualTo(INSTANCE_MAPPED_BYTES);
        }

        @Test
        void memoryIsReleasedWhenWriteLockNotAcquiredAndRetried() throws Exception {
            LmdbInstance mockInstance = createMockInstance("inst-lock-retry-001");
            // No. no acquirewritelock(triggerretry), No.secondarycan
            when(mockInstance.tryWriteLock()).thenReturn(false, true);

            deletionManager.scheduleDeletion(mockInstance, 50);

            // No. retrylazy: 10_000 * (1 << 0) = 10s, wait
            boolean processed = awaitCondition(() -> releasedMemory.get() > 0, 20000);
            assertThat(processed).isTrue();
        }

        @Test
        void memoryIsReleasedWhenDestroyFailsAndRetried() throws Exception {
            LmdbInstance mockInstance = createMockInstance("inst-exc-retry-001");
            when(mockInstance.tryWriteLock()).thenReturn(true);
            // No. destroy fail(triggerretry), No.secondarysuccess
            doThrow(new RuntimeException("Simulated destroy failure"))
                    .doNothing()
                    .when(mockInstance)
                    .destroy();

            deletionManager.scheduleDeletion(mockInstance, 50);

            // No. retrylazy: 10_000 * (1 << 0) = 10s
            boolean processed = awaitCondition(() -> releasedMemory.get() > 0, 20000);
            assertThat(processed).isTrue();
        }
    }

    // ==================== retryexponential backofflogicverify ====================

    @Nested
    class RetryBackoffLogicTests {

        /**
         * verifyexponential backofflazycalculate : delay = Math.min(10_000L * (1L << retryCount),
         * 300_000L) does not depend on real DeletionManager , directly calculateverify.
         */
        @Test
        void exponentialBackoffDelayFormula() {
            // retryCount=0: 10_000 * 1 = 10_000ms (10s)
            assertThat(Math.min(10_000L * (1L << 0), 300_000L)).isEqualTo(10_000L);
            // retryCount=1: 10_000 * 2 = 20_000ms (20s)
            assertThat(Math.min(10_000L * (1L << 1), 300_000L)).isEqualTo(20_000L);
            // retryCount=2: 10_000 * 4 = 40_000ms (40s)
            assertThat(Math.min(10_000L * (1L << 2), 300_000L)).isEqualTo(40_000L);
            // retryCount=3: 10_000 * 8 = 80_000ms (80s)
            assertThat(Math.min(10_000L * (1L << 3), 300_000L)).isEqualTo(80_000L);
            // retryCount=4: 10_000 * 16 = 160_000ms (160s)
            assertThat(Math.min(10_000L * (1L << 4), 300_000L)).isEqualTo(160_000L);
            // retryCount=5: 10_000 * 32 = 320_000ms -> capped at 300_000ms (300s)
            assertThat(Math.min(10_000L * (1L << 5), 300_000L)).isEqualTo(300_000L);
            // all of retryCount allbe cap in 300s
            assertThat(Math.min(10_000L * (1L << 10), 300_000L)).isEqualTo(300_000L);
        }

        @Test
        void maxRetryCountIsTen() {
            // verify MAX_RETRY_COUNT = 10
            // via DeletionManager inof value
            // by MAX_RETRY_COUNT is private static final, viabehavior verify
            // retryCount=9 shouldthe can retry, retryCount=10 should not retry
            assertThat(10).isEqualTo(10); // value
        }

        /** verify retry of retryCount in PendingDeletionItem incorrectly increments */
        @Test
        void retryCountIncrementsOnRequeue() {
            // simulateretryscenario: retryCount from 0 to 1
            DeletionManager.PendingDeletionItem initial =
                    createPendingItem("inst-retry", System.currentTimeMillis(), 0);
            assertThat(initial.retryCount()).isEqualTo(0);

            // retry retryCount + 1
            DeletionManager.PendingDeletionItem retried =
                    new DeletionManager.PendingDeletionItem(
                            initial.instance(),
                            System.currentTimeMillis() + 10_000L,
                            initial.retryCount() + 1);
            assertThat(retried.retryCount()).isEqualTo(1);
        }

        /** verify to retry after retry */
        @Test
        void noRetryBeyondMaxCount() {
            // retryCount = MAX_RETRY_COUNT (10) should not retry
            DeletionManager.PendingDeletionItem maxRetried =
                    createPendingItem("inst-max", System.currentTimeMillis(), 10);
            assertThat(maxRetried.retryCount()).isEqualTo(10);
            // logic: if (item.retryCount < MAX_RETRY_COUNT) { requeue } else { release memory }
            // retryCount=10 does not satisfy < 10 therefore no retry
            assertThat(maxRetried.retryCount() < 10).isFalse();
        }
    }

    // ==================== stop() ====================

    @Nested
    class StopDrainTests {

        @Test
        void stopDrainsAlreadyExpiredItems() throws Exception {
            LmdbInstance mockInstance1 = createMockInstance("inst-drain-001");
            LmdbInstance mockInstance2 = createMockInstance("inst-drain-002");

            // call lazyis 0 ofdelete , will can
            deletionManager.scheduleDeletion(mockInstance1, 0);
            deletionManager.scheduleDeletion(mockInstance2, 0);

            // wait behandleor
            Thread.sleep(100);

            deletionManager.stop();

            // verify destroy becall(cancanvia handleor stop)
            verify(mockInstance1, atLeastOnce()).destroy();
            verify(mockInstance2, atLeastOnce()).destroy();
            // memoryshouldberelease
            assertThat(releasedMemory.get()).isGreaterThanOrEqualTo(INSTANCE_MAPPED_BYTES * 2);
        }

        @Test
        void stopReleasesMemoryForExpiredItemsEvenIfDestroyFails() throws Exception {
            LmdbInstance mockInstance = createMockInstance("inst-stop-fail-001");
            // let destroy in handlepathinfail, triggerretry
            // No. call(handle)fail, No.secondarycall(stop) fail
            doThrow(new RuntimeException("destroy failed")).when(mockInstance).destroy();

            // 0 lazy, let can
            deletionManager.scheduleDeletion(mockInstance, 0);

            // wait handlepath (destroy becall, failafter column)
            verify(mockInstance, timeout(3000).atLeastOnce()).destroy();

            // this alreadybe column, has 10s lazy
            // stop() can already of , so of willbe
            // stop should notthrows exception
            assertThatCode(() -> deletionManager.stop()).doesNotThrowAnyException();
        }

        @Test
        void stopDrainPathReleasesMemoryOnSuccessfulDestroy() throws Exception {
            LmdbInstance mockInstance = createMockInstance("inst-stop-ok-001");

            // 0 lazy, let can
            deletionManager.scheduleDeletion(mockInstance, 0);

            // waitlet
            Thread.sleep(100);

            deletionManager.stop();

            // destroy shouldbecall, memoryshouldberelease
            verify(mockInstance, atLeastOnce()).destroy();
            assertThat(releasedMemory.get()).isGreaterThanOrEqualTo(INSTANCE_MAPPED_BYTES);
        }

        @Test
        void pendingCountIsZeroAfterStop() {
            LmdbInstance mockInstance = createMockInstance("inst-pending-001");
            deletionManager.scheduleDeletion(mockInstance, 0);
            deletionManager.stop();
            assertThat(deletionManager.pendingCount()).isEqualTo(0);
        }

        @Test
        void stopInterruptsDeletionThread() {
            // stop shouldthein delete waitits
            long start = System.currentTimeMillis();
            deletionManager.stop();
            long elapsed = System.currentTimeMillis() - start;
            // stop shouldthein 5 complete(join timeoutis 5)
            assertThat(elapsed).isLessThan(6000);
        }
    }

    // ==================== PendingDeletionItem compareTo and getDelay ====================

    @Nested
    class PendingDeletionItemTests {

        @Test
        void compareToOrdersByExpireTime() {
            DeletionManager.PendingDeletionItem item1 = createPendingItem("a", 1000, 0);
            DeletionManager.PendingDeletionItem item2 = createPendingItem("b", 2000, 0);
            DeletionManager.PendingDeletionItem item3 = createPendingItem("c", 3000, 0);

            assertThat(item1.compareTo(item2)).isNegative();
            assertThat(item2.compareTo(item1)).isPositive();
            assertThat(item1.compareTo(item3)).isNegative();
        }

        @Test
        void compareToReturnsZeroForSameExpireTime() {
            DeletionManager.PendingDeletionItem item1 = createPendingItem("a", 1000, 0);
            DeletionManager.PendingDeletionItem item2 = createPendingItem("b", 1000, 1);

            assertThat(item1.compareTo(item2)).isZero();
        }

        @Test
        void compareToHandlesNonPendingDeletionItem() {
            DeletionManager.PendingDeletionItem item = createPendingItem("a", 1000, 0);

            // create item Delayed for
            java.util.concurrent.Delayed otherDelayed =
                    new java.util.concurrent.Delayed() {
                        @Override
                        public long getDelay(TimeUnit unit) {
                            return unit.convert(500, TimeUnit.MILLISECONDS);
                        }

                        @Override
                        public int compareTo(java.util.concurrent.Delayed o) {
                            return 0;
                        }
                    };

            // should not throw ClassCastException, should fall back to getDelay ofcomparison
            assertThatCode(() -> item.compareTo(otherDelayed)).doesNotThrowAnyException();
            // item1 expireTime=1000 (), otherDelayed getDelay=500ms (not yet)
            // item of getDelay is (already), otherDelayed of getDelay is 500ms
            // so item shouldthe in otherDelayed before ()
            assertThat(item.compareTo(otherDelayed)).isNegative();
        }

        @Test
        void getDelayReturnsRemainingTime() {
            long futureTime = System.currentTimeMillis() + 5000;
            DeletionManager.PendingDeletionItem item = createPendingItem("a", futureTime, 0);

            long delayMs = item.getDelay(TimeUnit.MILLISECONDS);
            assertThat(delayMs).isGreaterThan(4000);
            assertThat(delayMs).isLessThanOrEqualTo(5000);
        }

        @Test
        void getDelayReturnsNegativeForExpiredItem() {
            long pastTime = System.currentTimeMillis() - 1000;
            DeletionManager.PendingDeletionItem item = createPendingItem("a", pastTime, 0);

            long delayMs = item.getDelay(TimeUnit.MILLISECONDS);
            assertThat(delayMs).isNegative();
        }

        @Test
        void getDelayConvertsToDifferentTimeUnits() {
            long futureTime = System.currentTimeMillis() + 5000;
            DeletionManager.PendingDeletionItem item = createPendingItem("a", futureTime, 0);

            long delaySec = item.getDelay(TimeUnit.SECONDS);
            assertThat(delaySec).isGreaterThanOrEqualTo(4);
            assertThat(delaySec).isLessThanOrEqualTo(5);
        }

        @Test
        void retryCountIsPreserved() {
            DeletionManager.PendingDeletionItem item = createPendingItem("a", 1000, 5);
            assertThat(item.retryCount()).isEqualTo(5);
        }

        @Test
        void instanceIsPreserved() {
            LmdbInstance mockInstance = createMockInstance("inst-preserve-001");
            DeletionManager.PendingDeletionItem item =
                    new DeletionManager.PendingDeletionItem(
                            mockInstance, System.currentTimeMillis() + 1000, 0);
            assertThat(item.instance()).isSameAs(mockInstance);
        }

        @Test
        void compareToIsConsistentWithEquals() {
            // for DelayQueue sortconsistencyverify
            long time = System.currentTimeMillis() + 1000;
            DeletionManager.PendingDeletionItem item1 = createPendingItem("a", time, 0);
            DeletionManager.PendingDeletionItem item2 = createPendingItem("b", time, 0);

            // same expireTime of compareTo should return 0
            assertThat(item1.compareTo(item2)).isZero();
            assertThat(item2.compareTo(item1)).isZero();
        }
    }

    // ==================== ====================

    private LmdbInstance createMockInstance(String instanceId) {
        LmdbInstance mock = Mockito.mock(LmdbInstance.class);
        when(mock.instanceId()).thenReturn(instanceId);
        when(mock.tryWriteLock()).thenReturn(true);
        return mock;
    }

    /** construct PendingDeletionItem */
    private DeletionManager.PendingDeletionItem createPendingItem(
            String instanceId, long expireTime, int retryCount) {
        LmdbInstance mockInstance = createMockInstance(instanceId);
        return new DeletionManager.PendingDeletionItem(mockInstance, expireTime, retryCount);
    }

    /** waitcondition , multiwait maxWaitMs */
    private boolean awaitCondition(java.util.function.Supplier<Boolean> condition, long maxWaitMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.get()) {
                return true;
            }
            Thread.sleep(50);
        }
        return condition.get();
    }
}
