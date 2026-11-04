package io.github.rawwise.engine.storage.writer;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRollingFileWriter<T> implements RollingFileWriter<T> {

    private final RollingPolicy rollingPolicy;
    private final AtomicLong currentFileSize = new AtomicLong(0);
    private final AtomicLong recordsWrittenCount = new AtomicLong(0);
    private final AtomicLong recordsFailedCount = new AtomicLong(0);
    private volatile boolean closed = false;
    private final ReentrantLock writeLock = new ReentrantLock();

    protected AbstractRollingFileWriter(RollingPolicy rollingPolicy) {
        this.rollingPolicy = Objects.requireNonNull(rollingPolicy);
    }

    @Override
    public RollingPolicy rollingPolicy() {
        return rollingPolicy;
    }

    @Override
    public WriteResult write(T record) throws IOException {
        writeLock.lock();
        try {
            if (closed) throw new IllegalStateException("Writer is closed");
            if (!isStreamOpen()) {
                doPrepareNewFile();
                doSwitchToNewFile();
            }
            long recordSize = estimateRecordSize(record);
            if (rollingPolicy.shouldRoll(currentFileSize.get(), recordSize)) {
                doRoll();
            }
            long bytesWritten = doWrite(record);
            currentFileSize.addAndGet(bytesWritten);
            recordsWrittenCount.incrementAndGet();
            return WriteResult.SUCCESS;
        } catch (IOException e) {
            recordsFailedCount.incrementAndGet();
            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    protected void doRoll() throws IOException {
        rollingPolicy.onRollStart();
        try {
            doPrepareNewFile();
        } catch (IOException e) {
            rollingPolicy.onRollFailed(e);
            throw e;
        }
        IOException closeException = null;
        try {
            doFlush();
        } catch (IOException e) {
            log.warn("Failed to flush current file during roll", e);
        }
        try {
            doCloseCurrentFile();
        } catch (IOException e) {
            log.warn("Failed to close current file during roll, possible resource leak", e);
            closeException = e;
        }
        doSwitchToNewFile();
        currentFileSize.set(0);
        if (closeException != null) {
            rollingPolicy.onRollFailed(closeException);
            throw closeException;
        }
        rollingPolicy.onRollComplete();
    }

    @Override
    public void roll() throws IOException {
        writeLock.lock();
        try {
            doRoll();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        writeLock.lock();
        try {
            if (!closed && isStreamOpen()) doFlush();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public long recordsWritten() {
        return recordsWrittenCount.get();
    }

    @Override
    public long recordsFailed() {
        return recordsFailedCount.get();
    }

    @Override
    public long length() {
        return currentFileSize.get();
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (!closed) {
                IOException firstException = null;
                try {
                    doFlush();
                } catch (IOException e) {
                    firstException = e;
                }
                try {
                    doCloseCurrentFile();
                } catch (IOException e) {
                    if (firstException != null) firstException.addSuppressed(e);
                    else firstException = e;
                }
                closed = true;
                if (firstException != null) throw firstException;
            }
        } finally {
            writeLock.unlock();
        }
    }

    protected long getCurrentFileSize() {
        return currentFileSize.get();
    }

    protected void addBytesWritten(long bytes) {
        currentFileSize.addAndGet(bytes);
    }

    protected long estimateRecordSize(T record) {
        return 0;
    }

    protected abstract boolean isStreamOpen();

    protected abstract long doWrite(T record) throws IOException;

    protected abstract void doPrepareNewFile() throws IOException;

    protected abstract void doCloseCurrentFile() throws IOException;

    protected abstract void doSwitchToNewFile() throws IOException;

    protected abstract void doFlush() throws IOException;
}
