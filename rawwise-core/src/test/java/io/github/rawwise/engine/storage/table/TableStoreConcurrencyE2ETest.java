package io.github.rawwise.engine.storage.table;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.LmdbInstance;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TableStoreConcurrencyE2ETest {

    @TempDir Path tempDir;

    private TableStore tableStore;
    private List<LmdbInstance> createdInstances;

    @BeforeEach
    void setUp() {
        tableStore = new TableStore("test.table", 2);
        createdInstances = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (LmdbInstance inst : createdInstances) {
            try {
                inst.close();
            } catch (Exception ignored) {
            }
        }
        try {
            tableStore.close();
        } catch (Exception ignored) {
        }
    }

    private LmdbInstance createInstance(String id) throws IOException {
        LmdbInstance instance = new LmdbInstance(id, tempDir, 10 * 1024 * 1024);
        createdInstances.add(instance);
        return instance;
    }

    @Test
    @DisplayName("E2E-4: concurrentaddWritableInstance exceedswritableNum")
    void shouldNotExceedWritableNumConcurrently() throws Exception {
        int writableNum = 2;
        int threadCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            LmdbInstance instance = createInstance("inst-" + idx);
                            tableStore.addWritableInstance(instance);
                            if (tableStore.getWritableInstances().contains(instance)) {
                                successCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            // ignored
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(tableStore.getWritableInstances().size()).isLessThanOrEqualTo(writableNum);
        assertThat(successCount.get()).isLessThanOrEqualTo(writableNum);
    }

    @Test
    @DisplayName("E2E-5: addWritableInstanceafterremoveInstancereleaseSemaphore can")
    void shouldRecycleSemaphoreOnAddRemove() throws Exception {
        LmdbInstance inst1 = createInstance("inst-1");
        LmdbInstance inst2 = createInstance("inst-2");

        tableStore.addWritableInstance(inst1);
        tableStore.addWritableInstance(inst2);
        assertThat(tableStore.getWritableInstances()).hasSize(2);

        tableStore.removeInstance(inst1);
        assertThat(tableStore.getWritableInstances()).hasSize(1);

        LmdbInstance inst3 = createInstance("inst-3");
        tableStore.addWritableInstance(inst3);
        assertThat(tableStore.getWritableInstances()).hasSize(2);
    }

    @Test
    @DisplayName("exceedswritableNumafteraddWritableInstancebedenied")
    void shouldRejectWhenWritableLimitReached() throws Exception {
        LmdbInstance inst1 = createInstance("inst-1");
        LmdbInstance inst2 = createInstance("inst-2");
        LmdbInstance inst3 = createInstance("inst-3");

        tableStore.addWritableInstance(inst1);
        tableStore.addWritableInstance(inst2);
        tableStore.addWritableInstance(inst3);

        assertThat(tableStore.getWritableInstances()).hasSize(2);
        assertThat(
                        tableStore.getWritableInstances().stream()
                                .map(LmdbInstance::instanceId)
                                .toList())
                .doesNotContain("inst-3");
    }

    @Test
    @DisplayName("duplicateaddWritableInstance count")
    void shouldNotAddDuplicateInstance() throws Exception {
        LmdbInstance inst1 = createInstance("inst-1");

        tableStore.addWritableInstance(inst1);
        tableStore.addWritableInstance(inst1);

        assertThat(tableStore.getWritableInstances()).hasSize(1);

        LmdbInstance inst2 = createInstance("inst-2");
        tableStore.addWritableInstance(inst2);
        assertThat(tableStore.getWritableInstances()).hasSize(2);
    }
}
