package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.backend.wal.WalWriteAdapter;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.model.op.KvWriteBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WritePathExecutorAsyncTest {

    private static final long SHORT_TIMEOUT_MS = 200L;

    private Map<String, AtomicInteger> totalRecords;
    private ConcurrentHashMap<String, TableStore> tableStores;

    @BeforeEach
    void setUp() {
        totalRecords = new ConcurrentHashMap<>();
        tableStores = new ConcurrentHashMap<>();
    }

    @Test
    void signalInstanceAvailableDoesNotThrow() {
        WritePathExecutor executor = createExecutor(30000L);
        executor.signalInstanceAvailable();
        executor.signalInstanceAvailable();
    }

    @Test
    void signalInstanceAvailableWakesWaitingThread() throws Exception {
        WritePathExecutor executor = createExecutor(30000L);
        CountDownLatch waitingStarted = new CountDownLatch(1);
        AtomicBoolean wokeUp = new AtomicBoolean(false);

        Thread waiter =
                new Thread(
                        () -> {
                            waitingStarted.countDown();
                            executor.signalInstanceAvailable();
                            wokeUp.set(true);
                        });

        waiter.start();
        waitingStarted.await(5, TimeUnit.SECONDS);
        waiter.join(5000);

        assertThat(wokeUp.get()).isTrue();
    }

    @Test
    void writeTimeoutExceededThrowsRecoverableWriteException() {
        MemoryBudget zeroBudget = new MemoryBudget(0, 128L * 1024 * 1024);
        WritePathExecutor executor = createExecutorWithBudget(zeroBudget, SHORT_TIMEOUT_MS);

        KvWriteBuilder builder =
                KvWriteBuilder.builder().key(new byte[] {1, 2, 3}).value(new byte[] {4, 5, 6});

        assertThatThrownBy(() -> executor.execute(builder, "test.table", null))
                .isInstanceOf(DynamicMmapStore.RecoverableWriteException.class)
                .hasMessageContaining("timeout");
    }

    @Test
    void concurrentSignalInstanceAvailableIsThreadSafe() throws Exception {
        WritePathExecutor executor = createExecutor(30000L);
        int threadCount = 16;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(
                    () -> {
                        try {
                            for (int j = 0; j < 100; j++) {
                                executor.signalInstanceAvailable();
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
        }

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void extractWriteKeyReturnsKeyForKvWriteBuilder() {
        KvWriteBuilder kvBuilder =
                KvWriteBuilder.builder().key(new byte[] {1, 2, 3}).value(new byte[] {4, 5, 6});
        assertThat(WritePathExecutor.extractWriteKey(kvBuilder)).isEqualTo(new byte[] {1, 2, 3});
    }

    @Test
    void extractWriteKeyReturnsNullForNullKey() {
        KvWriteBuilder kvBuilder = KvWriteBuilder.builder().value(new byte[] {4, 5, 6});
        assertThat(WritePathExecutor.extractWriteKey(kvBuilder)).isNull();
    }

    @Test
    void timeoutUsesConfiguredValue() {
        long start = System.currentTimeMillis();
        MemoryBudget zeroBudget = new MemoryBudget(0, 128L * 1024 * 1024);
        WritePathExecutor executor = createExecutorWithBudget(zeroBudget, SHORT_TIMEOUT_MS);

        KvWriteBuilder builder = KvWriteBuilder.builder().key(new byte[] {1}).value(new byte[] {2});

        assertThatThrownBy(() -> executor.execute(builder, "test.table", null))
                .isInstanceOf(DynamicMmapStore.RecoverableWriteException.class);
        long elapsed = System.currentTimeMillis() - start;
        assertThat(elapsed).isLessThan(10000L);
    }

    private WritePathExecutor createExecutor(long writeTimeoutMs) {
        MemoryBudget budget = new MemoryBudget(1024L * 1024 * 1024, 128L * 1024 * 1024);
        return createExecutorWithBudget(budget, writeTimeoutMs);
    }

    private WritePathExecutor createExecutorWithBudget(MemoryBudget budget, long writeTimeoutMs) {
        InstancePool pool =
                new InstancePool(
                        budget,
                        new InstancePool.FlushTrigger() {
                            @Override
                            public void triggerFlushAsync() {}

                            @Override
                            public boolean triggerFlushSync() {
                                return false;
                            }
                        },
                        tableId ->
                                tableStores.computeIfAbsent(tableId, id -> new TableStore(id, 4)),
                        "/tmp/rawwise-test/writepath-async-" + System.nanoTime(),
                        4,
                        128L * 1024 * 1024,
                        1,
                        null);

        WalWriteAdapter walWriteAdapter =
                new WalWriteAdapter(
                        null,
                        io.github.rawwise.engine.storage.DurabilityLevel.PROCESS,
                        new AtomicLong(0));
        WritePathConfig writePathConfig = new WritePathConfig(10000, 100L * 1024 * 1024, 128, 0.70);

        return new WritePathExecutor(
                pool,
                walWriteAdapter,
                budget,
                totalRecords,
                writePathConfig,
                128L * 1024 * 1024,
                () -> false,
                tableId -> tableStores.computeIfAbsent(tableId, id -> new TableStore(id, 4)),
                (tableId, key) -> {},
                writeTimeoutMs);
    }
}
