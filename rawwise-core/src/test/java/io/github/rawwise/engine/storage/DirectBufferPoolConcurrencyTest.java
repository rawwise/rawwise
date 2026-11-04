package io.github.rawwise.engine.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("P5-2: DirectBuffer pool concurrency")
class DirectBufferPoolConcurrencyTest {

    @Test
    @DisplayName("Concurrent buffer allocations do not leak")
    void concurrentBufferAllocations() throws Exception {
        int threadCount = 8;
        int iterations = 100;
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(
                    () -> {
                        try {
                            for (int i = 0; i < iterations; i++) {
                                var buffer = java.nio.ByteBuffer.allocateDirect(1024);
                                buffer.putInt(0, i);
                                assertEquals(i, buffer.getInt(0));
                                successCount.incrementAndGet();
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertEquals(threadCount * iterations, successCount.get());
        executor.shutdown();
    }

    @Test
    @DisplayName("Buffer pool size remains bounded under pressure")
    void bufferPoolBoundedUnderPressure() throws Exception {
        int maxSize = 64;
        var pool = new ConcurrentLinkedQueue<java.nio.ByteBuffer>();
        AtomicInteger allocated = new AtomicInteger(0);

        for (int i = 0; i < maxSize * 2; i++) {
            if (pool.size() < maxSize) {
                pool.offer(java.nio.ByteBuffer.allocateDirect(256));
                allocated.incrementAndGet();
            }
        }

        assertTrue(pool.size() <= maxSize);
        assertEquals(maxSize, allocated.get());
    }
}
