package io.github.rawwise.engine.storage.ingestion;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractIngestionDataSink implements IngestionDataSink {
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private volatile boolean closed = false;

    @FunctionalInterface
    protected interface SinkSupplier<T> {
        T get() throws SinkRetryableException, SinkFatalException;
    }

    @FunctionalInterface
    protected interface SinkRunnable {
        void run() throws SinkRetryableException, SinkFatalException;
    }

    protected boolean isClosed() {
        return closed;
    }

    protected void checkNotClosed() throws SinkFatalException {
        if (closed) throw new SinkFatalException(getClass().getSimpleName() + " is closed");
    }

    protected void validateFlushArgs(List<byte[]> keys, List<byte[]> values)
            throws SinkFatalException {
        if (keys == null || values == null) {
            throw new SinkFatalException("keys and values must not be null");
        }
        if (keys.isEmpty()) {
            throw new SinkFatalException("keys must not be empty");
        }
        if (keys.size() != values.size()) {
            throw new SinkFatalException("keys and values must have the same size");
        }
    }

    protected <T> T withReadLock(SinkSupplier<T> action)
            throws SinkRetryableException, SinkFatalException {
        rwLock.readLock().lock();
        try {
            checkNotClosed();
            return action.get();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    protected void withReadLock(SinkRunnable action)
            throws SinkRetryableException, SinkFatalException {
        rwLock.readLock().lock();
        try {
            checkNotClosed();
            action.run();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    protected <T> T withWriteLock(SinkSupplier<T> action)
            throws SinkRetryableException, SinkFatalException {
        rwLock.writeLock().lock();
        try {
            checkNotClosed();
            return action.get();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    protected void withWriteLock(SinkRunnable action)
            throws SinkRetryableException, SinkFatalException {
        rwLock.writeLock().lock();
        try {
            checkNotClosed();
            action.run();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    protected abstract void cleanupPendingData();

    @Override
    public void close() {
        rwLock.writeLock().lock();
        try {
            if (closed) return;
            closed = true;
            try {
                cleanupPendingData();
            } catch (Exception e) {
                log.error("Error during cleanupPendingData on close", e);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
