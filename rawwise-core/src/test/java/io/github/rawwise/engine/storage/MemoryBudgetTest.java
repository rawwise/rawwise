package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MemoryBudgetTest {

    @Test
    void canAllocateReturnsTrueWhenUnderThreshold() {
        MemoryBudget budget = new MemoryBudget(1000, 100);
        assertThat(budget.canAllocate(100)).isTrue();
        assertThat(budget.canAllocate(899)).isTrue();
    }

    @Test
    void canAllocateReturnsFalseWhenOverThreshold() {
        MemoryBudget budget = new MemoryBudget(1000, 100, 0.9);
        budget.allocate(900);
        assertThat(budget.canAllocate(100)).isFalse();
        assertThat(budget.canAllocate(1)).isFalse();
    }

    @Test
    void canAllocateReturnsFalseAtExactThreshold() {
        MemoryBudget budget = new MemoryBudget(1000, 100);
        budget.allocate(800);
        assertThat(budget.canAllocate(100)).isFalse();
    }

    @Test
    void canAllocateOneInstanceChecksInstanceSize() {
        MemoryBudget budget = new MemoryBudget(1000, 100);
        assertThat(budget.canAllocateOneInstance()).isTrue();
        budget.allocate(799);
        assertThat(budget.canAllocateOneInstance()).isTrue();
        budget.allocate(1);
        assertThat(budget.canAllocateOneInstance()).isFalse();
    }

    @Test
    void canAllocateReturnsFalseWhenMmapBudgetIsZero() {
        MemoryBudget budget = new MemoryBudget(0, 100);
        assertThat(budget.canAllocate(1)).isFalse();
        assertThat(budget.canAllocateOneInstance()).isFalse();
    }

    @Test
    void allocateAndReleaseUpdateUsedBytes() {
        MemoryBudget budget = new MemoryBudget(1000, 100);
        assertThat(budget.usedBytes()).isEqualTo(0);
        budget.allocate(500);
        assertThat(budget.usedBytes()).isEqualTo(500);
        budget.allocate(200);
        assertThat(budget.usedBytes()).isEqualTo(700);
        budget.release(300);
        assertThat(budget.usedBytes()).isEqualTo(400);
        budget.release(400);
        assertThat(budget.usedBytes()).isEqualTo(0);
    }

    @Test
    void releaseResetsToZeroOnUnderflow() {
        MemoryBudget budget = new MemoryBudget(1000, 100);
        budget.allocate(50);
        budget.release(100);
        assertThat(budget.usedBytes()).isEqualTo(0);
    }

    @Test
    void tryAllocateIsAtomic() {
        MemoryBudget budget = new MemoryBudget(1000, 100, 0.9);
        assertThat(budget.tryAllocate(100)).isTrue();
        assertThat(budget.usedBytes()).isEqualTo(100);
        assertThat(budget.tryAllocate(700)).isTrue();
        assertThat(budget.usedBytes()).isEqualTo(800);
        assertThat(budget.tryAllocate(100)).isFalse();
        assertThat(budget.usedBytes()).isEqualTo(800);
    }

    @Test
    void tryAllocateReturnsFalseWhenOverThreshold() {
        MemoryBudget budget = new MemoryBudget(1000, 100);
        budget.allocate(850);
        assertThat(budget.tryAllocate(50)).isFalse();
    }

    @Test
    void customUsageThreshold() {
        MemoryBudget budget = new MemoryBudget(1000, 100, 0.7);
        budget.allocate(500);
        assertThat(budget.canAllocate(100)).isTrue();
        budget.allocate(100);
        assertThat(budget.canAllocate(100)).isFalse();
    }

    @Test
    void limitBytesReturnsConfiguredLimit() {
        MemoryBudget budget = new MemoryBudget(2048, 256);
        assertThat(budget.limitBytes()).isEqualTo(2048);
    }

    @Test
    void instanceMappedBytesReturnsConfiguredValue() {
        MemoryBudget budget = new MemoryBudget(2048, 256);
        assertThat(budget.instanceMappedBytes()).isEqualTo(256);
    }

    @Test
    void constructorRejectsNegativeMmapBudgetBytes() {
        assertThatThrownBy(() -> new MemoryBudget(-1, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mmapBudgetBytes");
    }

    @Test
    void constructorAcceptsZeroMmapBudgetBytes() {
        MemoryBudget budget = new MemoryBudget(0, 100);
        assertThat(budget.limitBytes()).isEqualTo(0);
        assertThat(budget.canAllocate(1)).isFalse();
    }

    @Test
    void constructorRejectsNonPositiveInstanceMappedBytes() {
        assertThatThrownBy(() -> new MemoryBudget(1000, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instanceMappedBytes");
        assertThatThrownBy(() -> new MemoryBudget(1000, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instanceMappedBytes");
    }

    @Test
    void constructorRejectsInvalidUsageThreshold() {
        assertThatThrownBy(() -> new MemoryBudget(1000, 100, 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("usageThreshold");
        assertThatThrownBy(() -> new MemoryBudget(1000, 100, 1.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("usageThreshold");
    }

    @Test
    void concurrentTryAllocatePreventsOverspend() throws Exception {
        int threadCount = 16;
        long instanceSize = 100;
        long limit = 1000;
        double threshold = 0.9;
        MemoryBudget budget = new MemoryBudget(limit, instanceSize, threshold);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicLong totalAllocated = new AtomicLong(0);
        List<Future<Void>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            futures.add(
                    executor.submit(
                            () -> {
                                barrier.await();
                                for (int i = 0; i < 100; i++) {
                                    if (budget.tryAllocate(instanceSize)) {
                                        totalAllocated.addAndGet(instanceSize);
                                    }
                                }
                                return null;
                            }));
        }

        for (Future<Void> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(budget.usedBytes()).isEqualTo(totalAllocated.get());
        assertThat(budget.usedBytes()).isLessThan((long) (limit * threshold) + instanceSize);
    }

    @Test
    void concurrentAccessSafety() throws Exception {
        int threadCount = 16;
        int operationsPerThread = 10_000;
        long instanceSize = 100;
        long limit = instanceSize * threadCount * operationsPerThread * 10;
        MemoryBudget budget = new MemoryBudget(limit, instanceSize);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Future<Void>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            futures.add(
                    executor.submit(
                            () -> {
                                barrier.await();
                                for (int i = 0; i < operationsPerThread; i++) {
                                    budget.allocate(instanceSize);
                                    budget.release(instanceSize);
                                }
                                return null;
                            }));
        }

        for (Future<Void> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(budget.usedBytes()).isEqualTo(0);
    }
}
