package io.github.rawwise.backend.wal;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class WAL {
    private static final int TRY_NEXT_MAX_RETRIES = 3;
    private static final long TRY_NEXT_BACKOFF_MS = 1;

    protected Disruptor<WALEvent> disruptor;
    protected RingBuffer<WALEvent> ringBuffer;
    private final WALConfig config;
    private final WALConsumer consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final long appendTimeoutMs;
    private final boolean groupCommitEnabled;

    public WAL(WALConfig config) {
        this(config, "BINARY");
    }

    public WAL(WALConfig config, String fileFormat) {
        this(config, fileFormat, new Configuration());
    }

    public WAL(WALConfig config, String fileFormat, Configuration hadoopConfig) {
        this.config = config;
        this.appendTimeoutMs = config.appendTimeoutMs();
        this.groupCommitEnabled = config.groupCommitEnabled();
        this.consumer = new WALConsumer(config, fileFormat, hadoopConfig);
        this.consumer.setOnConsumerStopped(this::notifyConsumerStopped);
        this.disruptor =
                new Disruptor<>(
                        new WALEventFactory(),
                        config.bufferSize(),
                        daemonThreadFactory(),
                        ProducerType.MULTI,
                        new BlockingWaitStrategy());

        disruptor.setDefaultExceptionHandler(new WALExceptionHandler<>());
        disruptor.handleEventsWith(consumer);

        this.ringBuffer = disruptor.getRingBuffer();
        this.disruptor.start();
        this.running.set(true);
    }

    private void notifyConsumerStopped() {
        running.set(false);
    }

    public boolean isRunning() {
        return running.get() && consumer.isRunning();
    }

    public boolean isGroupCommitEnabled() {
        return groupCommitEnabled;
    }

    public WALConsumer getConsumer() {
        return consumer;
    }

    public void close() {
        rwLock.writeLock().lock();
        try {
            if (running.compareAndSet(true, false)) {
                if (disruptor != null) {
                    try {
                        disruptor.shutdown(10, TimeUnit.SECONDS);
                    } catch (com.lmax.disruptor.TimeoutException e) {
                        log.warn("[WAL] Disruptor shutdown timed out, forcing halt");
                        disruptor.halt();
                    }
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public long append(WALRecord data, boolean sync) {
        if (!rwLock.readLock().tryLock()) {
            throw new WALWriteException("WAL is shutting down");
        }
        try {
            if (!isRunning()) {
                throw new WALWriteException("WAL is not running, cannot append");
            }
            long sequence = tryNextWithRetry();
            try {
                WALEvent event = ringBuffer.get(sequence);
                event.set(data);
            } finally {
                ringBuffer.publish(sequence);
            }

            if (sync) {
                waitForSequence(sequence);
            }

            return sequence;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public long appendWithGroupCommit(WALRecord data) {
        long sequence;
        if (!rwLock.readLock().tryLock()) {
            throw new WALWriteException("WAL is shutting down");
        }
        try {
            if (!isRunning()) {
                throw new WALWriteException("WAL is not running, cannot append with group commit");
            }
            if (!groupCommitEnabled) {
                throw new IllegalStateException(
                        "Group commit is not enabled, use append() instead");
            }
            sequence = tryNextWithRetry();
            try {
                WALEvent event = ringBuffer.get(sequence);
                event.set(data);
            } finally {
                ringBuffer.publish(sequence);
            }
        } finally {
            rwLock.readLock().unlock();
        }
        waitForForceSync(sequence);
        return sequence;
    }

    public void insertSyncMarker(String tableIdentifier) {
        long sequence;
        if (!rwLock.readLock().tryLock()) {
            throw new WALWriteException("WAL is shutting down");
        }
        try {
            if (!isRunning()) {
                throw new WALWriteException("WAL is not running, cannot insert sync marker");
            }
            sequence = tryNextWithRetry();
            try {
                WALEvent event = ringBuffer.get(sequence);
                event.set(WALRecord.createSyncMarker(tableIdentifier));
            } finally {
                ringBuffer.publish(sequence);
            }
        } finally {
            rwLock.readLock().unlock();
        }
        if (groupCommitEnabled) {
            waitForForceSync(sequence);
        } else {
            waitForSequence(sequence);
        }
    }

    private long tryNextWithRetry() {
        long deadline = System.currentTimeMillis() + appendTimeoutMs;
        for (int i = 0; i < TRY_NEXT_MAX_RETRIES; i++) {
            if (System.currentTimeMillis() > deadline) {
                throw new WALWriteException("WAL ring buffer is full (timeout)");
            }
            try {
                return ringBuffer.tryNext();
            } catch (InsufficientCapacityException e) {
                if (i < TRY_NEXT_MAX_RETRIES - 1) {
                    LockSupport.parkNanos(TRY_NEXT_BACKOFF_MS * 1_000_000L);
                }
            }
        }
        throw new WALWriteException("WAL ring buffer is full");
    }

    private void waitForSequence(long sequence) {
        long startTime = System.currentTimeMillis();

        while (consumer.getLastCommittedSequence() < sequence) {
            if (System.currentTimeMillis() - startTime > appendTimeoutMs) {
                throw new WALWriteException("Timeout waiting for sequence: " + sequence);
            }

            if (!running.get()) {
                throw new WALWriteException("WAL stopped while waiting for sequence: " + sequence);
            }

            LockSupport.parkNanos(1_000_000L);
        }
    }

    private void waitForForceSync(long sequence) {
        long startTime = System.currentTimeMillis();
        long remainingMs = appendTimeoutMs;

        consumer.getForceSyncLock().lock();
        try {
            while (consumer.getLastForceSyncedSequence() < sequence) {
                if (System.currentTimeMillis() - startTime > appendTimeoutMs) {
                    throw new WALWriteException(
                            "Timeout waiting for forceSync of sequence: " + sequence);
                }
                if (!running.get()) {
                    throw new WALWriteException(
                            "WAL stopped while waiting for forceSync of sequence: " + sequence);
                }
                try {
                    remainingMs = appendTimeoutMs - (System.currentTimeMillis() - startTime);
                    if (remainingMs <= 0) {
                        throw new WALWriteException(
                                "Timeout waiting for forceSync of sequence: " + sequence);
                    }
                    if (!consumer.getForceSyncCondition()
                            .await(remainingMs, TimeUnit.MILLISECONDS)) {
                        if (consumer.getLastForceSyncedSequence() >= sequence) {
                            return;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new WALWriteException(
                            "Interrupted while waiting for forceSync of sequence: " + sequence);
                }
            }
        } finally {
            consumer.getForceSyncLock().unlock();
        }
    }

    private static ThreadFactory daemonThreadFactory() {
        return r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("wal-disruptor");
            return t;
        };
    }
}
