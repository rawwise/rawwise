package io.github.rawwise.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;

public abstract class AbstractConcurrencyTest {
    protected final AtomicInteger errorCount = new AtomicInteger(0);

    protected ExecutorService createThreadPool(int threads) {
        return Executors.newFixedThreadPool(threads);
    }

    protected CyclicBarrier createBarrier(int parties) {
        return new CyclicBarrier(parties);
    }

    protected CountDownLatch createLatch(int count) {
        return new CountDownLatch(count);
    }

    protected void await(CountDownLatch latch, long timeoutMs) throws InterruptedException {
        if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new AssertionError("Latch await timed out after " + timeoutMs + "ms");
        }
    }

    protected void awaitAtMost(long timeoutMs, ThrowingRunnable assertion) {
        Awaitility.await().atMost(timeoutMs, TimeUnit.MILLISECONDS).untilAsserted(assertion);
    }

    protected void assertNoErrors() {
        if (errorCount.get() > 0) {
            throw new AssertionError("Concurrent test encountered " + errorCount.get() + " errors");
        }
    }
}
