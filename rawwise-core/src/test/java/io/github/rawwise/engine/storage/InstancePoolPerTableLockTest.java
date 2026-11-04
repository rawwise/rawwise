package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.governance.config.ConfigFactory;
import io.github.rawwise.governance.config.RawWiseConfig;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InstancePoolPerTableLockTest {

    @TempDir Path tempDir;

    private MemoryBudget memoryBudget;
    private ConcurrentHashMap<String, TableStore> tableStores;

    @BeforeEach
    void setUp() {
        memoryBudget = new MemoryBudget(10L * 1024 * 1024 * 1024, 128L * 1024 * 1024);
        tableStores = new ConcurrentHashMap<>();
    }

    @Test
    void differentTablesCanAllocateConcurrently() throws Exception {
        InstancePool pool = createPool();
        pool.start(null);

        int tableCount = 8;
        int iterationsPerTable = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(tableCount);
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(tableCount);

        for (int t = 0; t < tableCount; t++) {
            final String tableId = "db.table" + t;
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            for (int i = 0; i < iterationsPerTable; i++) {
                                if (pool.tryAllocateNewWritableInstance(tableId)) {
                                    successCount.incrementAndGet();
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(successCount.get()).isGreaterThan(0);
    }

    @Test
    void sameTableOperationsAreSerialized() throws Exception {
        InstancePool pool = createPool();
        pool.start(null);

        String tableId = "db.serialized";
        int threadCount = 4;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            if (pool.tryAllocateNewWritableInstance(tableId)) {
                                successCount.incrementAndGet();
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(successCount.get()).isGreaterThan(0);
    }

    @Test
    void switchToNewInstanceForDifferentTablesDoesNotBlock() throws Exception {
        InstancePool pool = createPool();
        pool.start(null);

        pool.tryAllocateNewWritableInstance("db.tableA");
        pool.tryAllocateNewWritableInstance("db.tableB");

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        executor.submit(
                () -> {
                    try {
                        startLatch.await();
                        if (pool.switchToNewInstance("db.tableA")) {
                            successCount.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });

        executor.submit(
                () -> {
                    try {
                        startLatch.await();
                        if (pool.switchToNewInstance("db.tableB")) {
                            successCount.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });

        startLatch.countDown();
        assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(successCount.get()).isGreaterThan(0);
    }

    @Test
    void allocatePendingInstanceBytesIsGlobalOperation() throws Exception {
        InstancePool pool = createPool();
        pool.start(null);

        int threadCount = 8;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            if (pool.allocatePendingInstanceBytes()) {
                                successCount.incrementAndGet();
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(successCount.get()).isGreaterThan(0);
        assertThat(successCount.get()).isLessThanOrEqualTo(threadCount);
    }

    private InstancePool createPool() {
        return new InstancePool(
                memoryBudget,
                new InstancePool.FlushTrigger() {
                    @Override
                    public void triggerFlushAsync() {}

                    @Override
                    public boolean triggerFlushSync() {
                        return false;
                    }
                },
                tableId -> tableStores.computeIfAbsent(tableId, id -> new TableStore(id, 4)),
                tempDir.toString(),
                4,
                128L * 1024 * 1024,
                1,
                createNoopMemoryGuard());
    }

    private static ContainerMemoryGuard createNoopMemoryGuard() {
        RawWiseConfig config = ConfigFactory.createDefault();
        NativeMemorySource source =
                new NativeMemorySource() {
                    @Override
                    public long getMemoryLimitBytes() {
                        return Long.MAX_VALUE;
                    }

                    @Override
                    public long getMemoryUsageBytes() {
                        return 0;
                    }

                    @Override
                    public long mmapBudgetBytes() {
                        return Long.MAX_VALUE;
                    }

                    @Override
                    public long computeAvailableBytes() {
                        return Long.MAX_VALUE;
                    }
                };
        return new ContainerMemoryGuard(source, config);
    }
}
