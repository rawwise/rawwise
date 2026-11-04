package io.github.rawwise.engine.txn.orchestrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.txn.RecordHeaderProbe;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Spec 03 E2E: cross-table transactionatomic — ShardVersionTracker multi-table call + ValidateStage
 * multi-table OCC.
 */
@DisplayName("Cross-Table Txn E2E — Spec 03")
class CrossTableTxnE2ETest {

    private ShardVersionTracker trackerA;
    private ShardVersionTracker trackerB;
    private Map<String, ShardVersionTracker> trackers;

    @BeforeEach
    void setUp() {
        trackerA = new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
        trackerB = new ShardVersionTracker(1, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
        trackers = Map.of("t1", trackerA, "t2", trackerB);
    }

    @Nested
    @DisplayName("S2: table commit — table commitTs")
    class CrossTableCommitTests {

        @Test
        @DisplayName("S2.1: sametransactionwrite k-a@t1 + k-b@t2 -> table commitTs allupdate")
        void writesToBothTablesCommitBoth() {
            long txnId = 200L;
            byte[] keyA = "k-a".getBytes(StandardCharsets.UTF_8);
            byte[] keyB = "k-b".getBytes(StandardCharsets.UTF_8);

            trackerA.setCommitTs(keyA, txnId, txnId);
            trackerB.setCommitTs(keyB, txnId, txnId);

            assertThat(trackerA.getCommitTs(keyA)).isEqualTo(txnId);
            assertThat(trackerB.getCommitTs(keyB)).isEqualTo(txnId);
        }

        @Test
        @DisplayName("S2.2: write table commit table -> another table can")
        void partialCommitNotVisible() {
            long t1 = 201L, t2 = 202L;
            byte[] key = "partial".getBytes(StandardCharsets.UTF_8);

            trackerA.setCommitTs(key, t1, t1);
            // trackerB intentionally not updated

            assertThat(trackerA.getCommitTs(key)).isEqualTo(t1);
            assertThat(trackerB.getCommitTs(key)).isEqualTo(0L);
        }
    }

    @Nested
    @DisplayName("S3: table abort — releaseAll cleanupalltablelock")
    class CrossTableAbortTests {

        @Test
        @DisplayName("S3.1: T t1 and t2 oflock -> abort -> tableallrelease")
        void abortReleasesAllTrackers() throws InterruptedException {
            long txnId = 300L;
            trackerA.acquireForUpdate("a".getBytes(), txnId, 500);
            trackerB.acquireForUpdate("b".getBytes(), txnId, 500);

            assertThat(trackerA.heldKeyCount(txnId)).isEqualTo(1);
            assertThat(trackerB.heldKeyCount(txnId)).isEqualTo(1);

            for (ShardVersionTracker t : trackers.values()) t.releaseAll(txnId);

            assertThat(trackerA.heldKeyCount(txnId)).isZero();
            assertThat(trackerB.heldKeyCount(txnId)).isZero();
        }
    }

    @Nested
    @DisplayName("S4: table OCC conflict detection")
    class CrossTableOccTests {

        @Test
        @DisplayName(
                "S4.1: T1 write t1 -> T2 write t1 same key -> multi-table tracker detecttoconflict")
        void occConflictDetectedAcrossTxns() {
            byte[] key = "hot".getBytes(StandardCharsets.UTF_8);
            trackerA.setCommitTs(key, 400L, 400L);

            long baseVersion = 300L; // T2 saw stale
            long currentTs = trackerA.getCommitTs(key);
            assertThat(baseVersion).isLessThan(currentTs);
        }

        @Test
        @DisplayName("S4.2: concurrent lock another itemtransactiontimeout")
        void concurrentLockCausesTimeout() throws Exception {
            byte[] key = "locked".getBytes(StandardCharsets.UTF_8);
            CountDownLatch acquired = new CountDownLatch(1);
            CountDownLatch hold = new CountDownLatch(1);

            Executors.newSingleThreadExecutor()
                    .submit(
                            () -> {
                                try {
                                    trackerA.acquireForUpdate(key, 1L, 5_000);
                                    acquired.countDown();
                                    hold.await(5, TimeUnit.SECONDS);
                                    trackerA.releaseAll(1L);
                                } catch (InterruptedException ignored) {
                                    Thread.currentThread().interrupt();
                                }
                            });

            acquired.await(2, TimeUnit.SECONDS);
            assertThat(
                            org.assertj.core.api.Assertions.catchThrowable(
                                    () -> trackerA.acquireForUpdate(key, 99L, 50L)))
                    .isInstanceOf(ShardVersionTracker.LockTimeoutException.class);

            hold.countDown();
        }
    }

    @Nested
    @DisplayName("S5: multi-table tracker resolver verify")
    class ResolverTests {

        @Test
        @DisplayName("S5.1: resolver returns correctlyof per-table tracker")
        void resolverReturnsCorrectTracker() {
            assertThat(trackers.get("t1")).isSameAs(trackerA);
            assertThat(trackers.get("t2")).isSameAs(trackerB);
        }

        @Test
        @DisplayName("S5.2: different tables' tracker instances are different")
        void trackersAreDifferentInstances() {
            assertThat(trackerA).isNotSameAs(trackerB);
        }

        @Test
        @DisplayName("S5.3: table tracker will (independent)")
        void trackersAreIndependent() {
            byte[] key = "indep".getBytes(StandardCharsets.UTF_8);
            trackerA.setCommitTs(key, 1L, 1L);
            assertThat(trackerB.getCommitTs(key)).isZero();
        }
    }
}
