package io.github.rawwise.backend.wal;

import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.writer.RollingPolicy;
import io.github.rawwise.engine.storage.writer.WriteResult;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractWALWriter implements WALWriter {

    private final RollingPolicy rollingPolicy;
    private final long syncIntervalMs;
    private final boolean skipAutoSync;
    protected final AtomicLong currentFileSize = new AtomicLong(0);
    protected final AtomicLong totalWrittenBytes = new AtomicLong(0);
    protected final AtomicLong recordsWrittenCount = new AtomicLong(0);
    protected final AtomicLong recordsFailedCount = new AtomicLong(0);
    private volatile long lastCommittedSequence = -1;
    protected volatile long lastWrittenSequence = -1;
    private volatile boolean closed = false;
    private volatile long lastSyncTimeMs = 0;
    protected final ReentrantLock writeLock = new ReentrantLock();

    protected final String storeId;

    protected AbstractWALWriter(String storeId, RollingPolicy rollingPolicy, long syncIntervalMs) {
        this(storeId, rollingPolicy, syncIntervalMs, false);
    }

    protected AbstractWALWriter(
            String storeId,
            RollingPolicy rollingPolicy,
            long syncIntervalMs,
            boolean skipAutoSync) {
        this.storeId = Objects.requireNonNull(storeId, "storeId must not be null");
        this.rollingPolicy =
                Objects.requireNonNull(rollingPolicy, "rollingPolicy must not be null");
        this.syncIntervalMs = syncIntervalMs;
        this.skipAutoSync = skipAutoSync;
    }

    @Override
    public WriteResult write(WALRecord record) throws IOException {
        writeLock.lock();
        try {
            if (closed) throw new IllegalStateException("Writer is closed");
            if (!isStreamOpen()) {
                doPrepareNewFile();
                doSwitchToNewFile();
            }
            byte[] data = serializeRecord(record);
            if (rollingPolicy.shouldRoll(currentFileSize.get(), data.length)) {
                doRoll();
            }
            doWriteBytes(data);
            currentFileSize.addAndGet(data.length);
            totalWrittenBytes.addAndGet(data.length);
            recordsWrittenCount.incrementAndGet();
            lastWrittenSequence = record.sequence();
            try {
                maybeSync();
            } catch (IOException syncEx) {
                recordsWrittenCount.decrementAndGet();
                throw new IOException(
                        "Write committed to OS buffer but sync failed for sequence "
                                + record.sequence(),
                        syncEx);
            }
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
        try {
            doForceSyncInternal();
        } catch (IOException syncEx) {
            try {
                doCleanupPreparedFile();
            } catch (IOException ignored) {
            }
            rollingPolicy.onRollFailed(syncEx);
            log.error("Roll failed due to sync error, writer entering degraded mode", syncEx);
            try {
                close();
            } catch (IOException closeEx) {
                syncEx.addSuppressed(closeEx);
            }
            throw syncEx;
        }
        try {
            doCloseCurrentFile();
        } catch (IOException e) {
            log.warn("Failed to close current file during roll", e);
        }
        doSwitchToNewFile();
        currentFileSize.set(0);
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
    public void forceSync() throws IOException {
        writeLock.lock();
        try {
            doForceSyncInternal();
        } finally {
            writeLock.unlock();
        }
    }

    protected void doForceSyncInternal() throws IOException {
        doSync();
        lastCommittedSequence = getLastWrittenSequenceInternal();
        lastSyncTimeMs = System.currentTimeMillis();
    }

    @Override
    public void flush() throws IOException {
        writeLock.lock();
        try {
            if (!closed && isStreamOpen()) doFlushStream();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long writtenBytes() {
        return totalWrittenBytes.get();
    }

    @Override
    public long committedLSN() {
        return lastCommittedSequence;
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
    public boolean isClosed() {
        return closed;
    }

    @Override
    public Optional<Storage.StoragePath> currentFilePath() {
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (!closed) {
                IOException firstException = null;
                try {
                    doForceSyncInternal();
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

    protected RollingPolicy getRollingPolicy() {
        return rollingPolicy;
    }

    private void maybeSync() throws IOException {
        if (skipAutoSync) return;
        long now = System.currentTimeMillis();
        if (now - lastSyncTimeMs >= syncIntervalMs) {
            doForceSyncInternal();
        }
    }

    protected byte[] serializeRecord(WALRecord record) {
        return WALRecordCodec.serializeRecord(record);
    }

    protected byte[] serializeHeader(String executorId) {
        String json = "{\"storeId\":\"" + storeId + "\",\"executorId\":\"" + executorId + "\"}";
        byte[] jsonBytes = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(4 + 4 + 4 + jsonBytes.length + 4);
        buf.put("NWL0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        buf.putInt(1);
        buf.putInt(jsonBytes.length);
        buf.put(jsonBytes);
        buf.putInt((int) crc32(jsonBytes));
        return buf.array();
    }

    private long crc32(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return crc.getValue();
    }

    protected void setCurrentFileSize(long size) {
        currentFileSize.set(size);
    }

    protected long getLastWrittenSequenceInternal() {
        return lastWrittenSequence;
    }

    protected abstract boolean isStreamOpen();

    protected abstract void doWriteBytes(byte[] data) throws IOException;

    protected abstract void doPrepareNewFile() throws IOException;

    protected abstract void doCloseCurrentFile() throws IOException;

    protected abstract void doSwitchToNewFile() throws IOException;

    protected abstract void doCleanupPreparedFile() throws IOException;

    protected abstract void doSync() throws IOException;

    protected abstract void doFlushStream() throws IOException;
}
