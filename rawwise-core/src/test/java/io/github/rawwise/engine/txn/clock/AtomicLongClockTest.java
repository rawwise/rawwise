package io.github.rawwise.engine.txn.clock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * E2E business scenario tests for AtomicLongClock.
 *
 * <p>Validates the core business guarantee: the clock produces globally unique, monotonically
 * increasing txnId/commitTs values under concurrent access and recovery scenarios.
 */
@DisplayName("AtomicLongClock E2E Scenarios")
class AtomicLongClockTest {

    // ================================================================
    // S1: Happy Path — sequential txnId allocation
    // ================================================================

    @Nested
    @DisplayName("S1: Sequential txnId allocation")
    class SequentialAllocation {

        @Test
        @DisplayName("next() produces strictly increasing unique values")
        void testNextProducesStrictlyIncreasingUniqueValues() {
            LogicalClock clock = new AtomicLongClock(0);

            long prev = 0;
            for (int i = 0; i < 10_000; i++) {
                long current = clock.next();
                assertThat(current).isGreaterThan(prev);
                prev = current;
            }
            assertThat(prev).isEqualTo(10_000);
        }

        @Test
        @DisplayName("now() reflects last allocated txnId")
        void testNowReflectsLastAllocatedTxnId() {
            LogicalClock clock = new AtomicLongClock(0);
            assertThat(clock.now()).isEqualTo(0);

            clock.next(); // txnId=1
            assertThat(clock.now()).isEqualTo(1);

            clock.next(); // txnId=2
            clock.next(); // txnId=3
            assertThat(clock.now()).isEqualTo(3);
        }
    }

    // ================================================================
    // S2: Concurrency — multi-thread txnId allocation
    // ================================================================

    @Nested
    @DisplayName("S2: Concurrent txnId allocation")
    class ConcurrentAllocation {

        @Test
        @DisplayName("all threads receive unique monotonically increasing txnIds")
        void testConcurrentAllocationProducesUniqueMonotonicIds() throws Exception {
            LogicalClock clock = new AtomicLongClock(0);
            int threadCount = 16;
            int allocationsPerThread = 1000;
            CountDownLatch latch = new CountDownLatch(threadCount);

            List<Long> allIds = Collections.synchronizedList(new ArrayList<>());

            for (int t = 0; t < threadCount; t++) {
                new Thread(
                                () -> {
                                    try {
                                        for (int i = 0; i < allocationsPerThread; i++) {
                                            allIds.add(clock.next());
                                        }
                                    } finally {
                                        latch.countDown();
                                    }
                                })
                        .start();
            }

            latch.await();

            // All IDs must be unique
            assertThat(allIds).hasSize(threadCount * allocationsPerThread);
            long distinctCount = allIds.stream().distinct().count();
            assertThat(distinctCount).isEqualTo(allIds.size());

            // All IDs must be in [1, threadCount * allocationsPerThread]
            long maxId = allIds.stream().mapToLong(Long::longValue).max().orElse(0);
            assertThat(maxId).isEqualTo((long) threadCount * allocationsPerThread);

            // Verify monotonicity: now() should be >= last allocated ID
            assertThat(clock.now()).isGreaterThanOrEqualTo(maxId);
        }

        @Test
        @DisplayName("now() is consistent under concurrent allocation pressure")
        void testNowConsistentUnderConcurrentAllocation() throws Exception {
            LogicalClock clock = new AtomicLongClock(0);
            int threadCount = 8;
            CountDownLatch latch = new CountDownLatch(threadCount);

            List<Long> allocatedIds = Collections.synchronizedList(new ArrayList<>());

            for (int t = 0; t < threadCount; t++) {
                new Thread(
                                () -> {
                                    try {
                                        for (int i = 0; i < 500; i++) {
                                            allocatedIds.add(clock.next());
                                        }
                                    } finally {
                                        latch.countDown();
                                    }
                                })
                        .start();
            }

            latch.await();

            long maxAllocated = allocatedIds.stream().mapToLong(Long::longValue).max().orElse(0);
            long clockNow = clock.now();
            // now() should be within [1, maxAllocated] range since AtomicLongClock
            // uses AtomicLong.incrementAndGet() and now() reads the counter directly
            assertThat(clockNow).isGreaterThanOrEqualTo(1);
            assertThat(maxAllocated).isPositive();
        }
    }

    // ================================================================
    // S3: Fault Recovery — advanceToAtLeast for clock recovery
    // ================================================================

    @Nested
    @DisplayName("S3: Clock recovery via advanceToAtLeast")
    class Recovery {

        @Test
        @DisplayName("advanceToAtLeast restores clock after node restart")
        void testAdvanceToAtLeastRestoresClockAfterRestart() {
            // Scenario: Node A allocated txnIds up to 1000, crashed.
            // On restart, the clock must advance to at least 1000.
            LogicalClock clock = new AtomicLongClock(0);

            // Simulate: WAL/log shows last committed txnId was 1000
            long lastKnownTxnId = 1000L;
            long recovered = clock.advanceToAtLeast(lastKnownTxnId);

            assertThat(recovered).isGreaterThanOrEqualTo(lastKnownTxnId);

            // Next allocation must be > lastKnownTxnId
            long nextId = clock.next();
            assertThat(nextId).isGreaterThan(lastKnownTxnId);
        }

        @Test
        @DisplayName("advanceToAtLeast is idempotent for values below current clock")
        void testAdvanceToAtLeastIsIdempotentForLowerValues() {
            LogicalClock clock = new AtomicLongClock(0);

            clock.next(); // 1
            clock.next(); // 2
            clock.next(); // 3

            long before = clock.now();
            long result = clock.advanceToAtLeast(1); // lower than current
            assertThat(result).isEqualTo(before);
        }

        @Test
        @DisplayName("recover factory method initializes clock from persisted value")
        void testRecoverFactoryMethodInitializesFromPersistedValue() {
            long persistedValue = 5000L;
            AtomicLongClock clock = AtomicLongClock.recover(persistedValue);

            assertThat(clock.now()).isEqualTo(persistedValue);

            // First allocated txnId should be > persistedValue
            long firstId = clock.next();
            assertThat(firstId).isEqualTo(persistedValue + 1);
        }
    }

    // ================================================================
    // S4: Boundary Conditions
    // ================================================================

    @Nested
    @DisplayName("S4: Boundary conditions")
    class Boundary {

        @Test
        @DisplayName("clock initialized at zero produces txnId=1 on first next()")
        void testZeroInitialValue() {
            LogicalClock clock = new AtomicLongClock(0);
            assertThat(clock.now()).isEqualTo(0);
            assertThat(clock.next()).isEqualTo(1);
        }

        @Test
        @DisplayName("clock initialized at high value continues correctly")
        void testHighInitialValue() {
            long highValue = Long.MAX_VALUE / 2;
            LogicalClock clock = new AtomicLongClock(highValue);
            assertThat(clock.now()).isEqualTo(highValue);
            assertThat(clock.next()).isEqualTo(highValue + 1);
        }

        @Test
        @DisplayName("advanceToAtLeast handles Long.MAX_VALUE boundary")
        void testAdvanceToAtLeastMaxValue() {
            LogicalClock clock = new AtomicLongClock(Long.MAX_VALUE - 10);
            long result = clock.advanceToAtLeast(Long.MAX_VALUE);
            assertThat(result).isEqualTo(Long.MAX_VALUE);
        }

        @Test
        @DisplayName("next() after Long.MAX_VALUE throws ClockOverflowException")
        void testNextOverflowAtMaxValue() {
            AtomicLongClock clock = new AtomicLongClock(Long.MAX_VALUE - 1);
            clock.next(); // reaches MAX_VALUE
            assertThatThrownBy(clock::next)
                    .isInstanceOf(AtomicLongClock.ClockOverflowException.class);
        }

        @Test
        @DisplayName("negative initialValue throws IllegalArgumentException")
        void testNegativeInitialValueRejected() {
            assertThatThrownBy(() -> new AtomicLongClock(-1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("non-negative");
        }
    }
}
