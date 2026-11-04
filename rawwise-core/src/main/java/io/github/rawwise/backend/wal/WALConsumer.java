package io.github.rawwise.backend.wal;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import io.github.rawwise.engine.spi.FaultInjector;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Slf4j
public class WALConsumer implements EventHandler<WALEvent>, LifecycleAware {

    private static final int MAX_WRITE_RETRIES = 3;
    private static final long RETRY_BASE_DELAY_MS = 100;
    private static final long RECOVERY_CHECK_INTERVAL_MS = 30_000;
    private static final int CIRCUIT_BREAKER_FAILURE_THRESHOLD = 10;
    private static final int CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS = 120;

    private volatile WALWriter writer;
    private final WALConfig config;
    private final Configuration hadoopConfig;
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong consecutiveFailureCount = new AtomicLong(0);
    private final ReentrantLock recoveryLock = new ReentrantLock();

    private volatile boolean running = false;
    private final AtomicLong lastCommittedSequence = new AtomicLong(-1);
    private volatile long lastHdfsRecoveryAttemptTime = 0;
    private volatile long lastWalPersistedSequence = -1;
    private volatile boolean hdfsUnavailable = false;
    private volatile boolean circuitBreakerOpen = false;
    private volatile long circuitBreakerOpenTime = 0;
    private final AtomicLong skippedWriteCount = new AtomicLong(0);
    private final AtomicLong totalSkippedWriteCount = new AtomicLong(0);
    private volatile ScheduledExecutorService recoveryScheduler;
    private volatile Runnable onConsumerStopped;
    private volatile Runnable onCircuitBreakerOpen;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final boolean groupCommitEnabled;
    private final int batchSizeThreshold;
    private final long batchFlushIntervalMs;
    private final AtomicLong batchCounter = new AtomicLong(0);
    private volatile long lastForceSyncTime = System.currentTimeMillis();
    private volatile long lastForceSyncedSequence = -1;
    private final ReentrantLock forceSyncLock = new ReentrantLock();
    private final java.util.concurrent.locks.Condition forceSyncCondition =
            forceSyncLock.newCondition();
    private volatile ScheduledExecutorService groupCommitFlushScheduler;
    private volatile ScheduledFuture<?> groupCommitFlushTask;
    private final FaultInjector faultInjector;

    public WALConsumer(WALConfig config, String fileFormat, Configuration hadoopConfig) {
        this(config, fileFormat, hadoopConfig, null);
    }

    public WALConsumer(
            WALConfig config,
            String fileFormat,
            Configuration hadoopConfig,
            FaultInjector faultInjector) {
        this.config = config;
        this.hadoopConfig = hadoopConfig;
        this.faultInjector = faultInjector != null ? faultInjector : FaultInjector.NOOP;
        this.groupCommitEnabled = config.groupCommitEnabled();
        this.batchSizeThreshold = config.groupCommitBatchSize();
        this.batchFlushIntervalMs = config.groupCommitFlushIntervalMs();

        if (config.isHdfsEnabled()) {
            this.writer =
                    new HdfsWALWriter(
                            config.storeId(),
                            config.executorId(),
                            config.hdfsBasePath(),
                            config.segmentSize(),
                            config.syncIntervalMs(),
                            hadoopConfig,
                            groupCommitEnabled);
        } else {
            this.writer =
                    new LocalWALWriter(
                            config.storeId(),
                            config.logDir(),
                            config.segmentSize(),
                            config.syncIntervalMs(),
                            groupCommitEnabled);
        }
    }

    @Override
    public void onEvent(WALEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!running) {
            return;
        }

        try {
            WALRecord entry = event.getEntry();
            if (entry == null) {
                return;
            }

            WALRecord newEntry =
                    new DefaultWALRecord(
                            sequence,
                            entry.value(),
                            entry.timestamp(),
                            entry.commitId(),
                            entry.uniqTableIdentifier(),
                            entry.marker());

            boolean written = writeWithRetry(newEntry);
            faultInjector.inject(FaultInjector.InjectionPoint.DURING_WAL_FLUSH_SIGTERM);
            lastCommittedSequence.updateAndGet(current -> Math.max(current, sequence));
            if (written) {
                lastWalPersistedSequence = sequence;
            }

            maybeRecoverFromHdfsUnavailable();
            processedCount.incrementAndGet();
            event.markProcessed();

            if (groupCommitEnabled) {
                long count = batchCounter.incrementAndGet();
                long now = System.currentTimeMillis();
                if (count >= batchSizeThreshold
                        || now - lastForceSyncTime >= batchFlushIntervalMs) {
                    doGroupForceSync();
                }
            } else {
                if (endOfBatch && writer != null) {
                    writer.forceSync();
                }
            }

        } catch (Exception e) {
            errorCount.incrementAndGet();
            log.error("Critical error processing event at sequence: {}", sequence, e);

            if (!attemptRecovery(lastCommittedSequence.get(), e)) {
                running = false;
                if (onConsumerStopped != null) {
                    onConsumerStopped.run();
                }
                throw e;
            }
        }
    }

    private void doGroupForceSync() {
        forceSyncLock.lock();
        try {
            if (writer == null) {
                return;
            }
            long sequenceAtForceSync = lastCommittedSequence.get();
            writer.forceSync();
            faultInjector.inject(FaultInjector.InjectionPoint.DURING_WAL_FLUSH);
            lastForceSyncedSequence = sequenceAtForceSync;
            batchCounter.set(0);
            lastForceSyncTime = System.currentTimeMillis();
            forceSyncCondition.signalAll();
        } catch (IOException e) {
            log.error(
                    "Group commit forceSync failed at sequence {}", lastCommittedSequence.get(), e);
            batchCounter.set(0);
            lastForceSyncTime = System.currentTimeMillis();
            forceSyncCondition.signalAll();
        } finally {
            forceSyncLock.unlock();
        }
    }

    @Override
    public void onStart() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        running = true;

        recoveryScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "wal-hdfs-recovery");
                            t.setDaemon(true);
                            return t;
                        });
        recoveryScheduler.scheduleAtFixedRate(
                this::checkHdfsRecovery,
                RECOVERY_CHECK_INTERVAL_MS,
                RECOVERY_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        if (groupCommitEnabled) {
            groupCommitFlushScheduler =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "wal-group-commit-flush");
                                t.setDaemon(true);
                                return t;
                            });
            groupCommitFlushTask =
                    groupCommitFlushScheduler.scheduleWithFixedDelay(
                            () -> {
                                if (batchCounter.get() > 0 && running) {
                                    doGroupForceSync();
                                }
                            },
                            batchFlushIntervalMs,
                            batchFlushIntervalMs,
                            TimeUnit.MILLISECONDS);
            log.info(
                    "WALConsumer group commit enabled: batchSize={}, flushIntervalMs={}",
                    batchSizeThreshold,
                    batchFlushIntervalMs);
        }

        log.info("WALConsumer started with writer: {}", writer.getClass().getSimpleName());
    }

    @Override
    public void onShutdown() {
        running = false;

        if (groupCommitFlushTask != null) {
            groupCommitFlushTask.cancel(false);
        }
        if (groupCommitFlushScheduler != null) {
            groupCommitFlushScheduler.shutdown();
            try {
                if (!groupCommitFlushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    groupCommitFlushScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                groupCommitFlushScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (groupCommitEnabled && batchCounter.get() > 0) {
            doGroupForceSync();
        }

        if (recoveryScheduler != null) {
            recoveryScheduler.shutdown();
            try {
                if (!recoveryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    recoveryScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                recoveryScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        try {
            if (writer != null) {
                writer.close();
            }
            log.info(
                    "WALConsumer shutdown completed. Processed: {}, Errors: {}, TotalSkippedWrites: {}",
                    processedCount.get(),
                    errorCount.get(),
                    totalSkippedWriteCount.get());
        } catch (Exception e) {
            log.error("Error during shutdown", e);
        }
    }

    public long getLastCommittedSequence() {
        return lastCommittedSequence.get();
    }

    public long getLastForceSyncedSequence() {
        return lastForceSyncedSequence;
    }

    java.util.concurrent.locks.Condition getForceSyncCondition() {
        return forceSyncCondition;
    }

    ReentrantLock getForceSyncLock() {
        return forceSyncLock;
    }

    public long getLastWalPersistedSequence() {
        return lastWalPersistedSequence;
    }

    public long getTotalSkippedWriteCount() {
        return totalSkippedWriteCount.get();
    }

    public boolean isHdfsUnavailable() {
        return hdfsUnavailable;
    }

    public boolean isRunning() {
        return running;
    }

    public void setOnConsumerStopped(Runnable callback) {
        this.onConsumerStopped = callback;
    }

    public void setCircuitBreakerOpenCallback(Runnable callback) {
        this.onCircuitBreakerOpen = callback;
    }

    public boolean isCircuitBreakerOpen() {
        return circuitBreakerOpen;
    }

    public long getConsecutiveFailureCount() {
        return consecutiveFailureCount.get();
    }

    private boolean writeWithRetry(WALRecord entry) throws IOException {
        if (circuitBreakerOpen) {
            long elapsedSeconds = (System.currentTimeMillis() - circuitBreakerOpenTime) / 1000;
            if (elapsedSeconds < CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS) {
                log.warn(
                        "Circuit breaker is OPEN. Degrading to LMDB-only for sequence {}. "
                                + "HDFS has been unavailable for {}s. Data persists in LMDB only.",
                        entry.sequence(),
                        elapsedSeconds);
                skippedWriteCount.incrementAndGet();
                totalSkippedWriteCount.incrementAndGet();
                return false;
            }
            log.info("Circuit breaker timeout elapsed, attempting half-open state");
            circuitBreakerOpen = false;
            hdfsUnavailable = false;
            consecutiveFailureCount.set(0);
        }

        if (hdfsUnavailable) {
            skippedWriteCount.incrementAndGet();
            totalSkippedWriteCount.incrementAndGet();
            return false;
        }

        IOException lastError = null;
        for (int i = 0; i < MAX_WRITE_RETRIES; i++) {
            try {
                writer.write(entry);
                faultInjector.inject(FaultInjector.InjectionPoint.DURING_HDFS_WRITE);
                consecutiveFailureCount.set(0);
                return true;
            } catch (IOException e) {
                lastError = e;
                long failures = consecutiveFailureCount.incrementAndGet();
                log.warn("HDFS WAL write failed (failure #{}): {}", failures, e.getMessage());

                if (failures >= CIRCUIT_BREAKER_FAILURE_THRESHOLD) {
                    circuitBreakerOpen = true;
                    circuitBreakerOpenTime = System.currentTimeMillis();
                    hdfsUnavailable = true;
                    log.error(
                            "Circuit breaker OPENED after {} consecutive HDFS write failures. "
                                    + "WAL writes will be skipped (data persists in LMDB) until HDFS recovers.",
                            failures);
                    if (onCircuitBreakerOpen != null) {
                        onCircuitBreakerOpen.run();
                    }
                    skippedWriteCount.incrementAndGet();
                    totalSkippedWriteCount.incrementAndGet();
                    return false;
                }

                if (i < MAX_WRITE_RETRIES - 1) {
                    long delay = RETRY_BASE_DELAY_MS * (1L << i);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw lastError;
                    }
                }
            }
        }

        hdfsUnavailable = true;
        skippedWriteCount.incrementAndGet();
        totalSkippedWriteCount.incrementAndGet();
        log.warn(
                "HDFS WAL write failed after {} retries. WAL writes will be skipped until HDFS recovers. "
                        + "Data is still persisted in LMDB (process-level durability). Cross-node durability is NOT guaranteed.",
                MAX_WRITE_RETRIES);
        return false;
    }

    private boolean isHdfsWritable() {
        try {
            Path hdfsPath = new Path(config.hdfsBasePath());
            FileSystem fs = hdfsPath.getFileSystem(hadoopConfig);
            if (!fs.exists(hdfsPath)) {
                return false;
            }
            Path probePath = new Path(hdfsPath, ".recovery_probe_" + System.currentTimeMillis());
            try (FSDataOutputStream out = fs.create(probePath, true)) {
                out.write(0);
                out.hsync();
            }
            fs.delete(probePath, false);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void checkHdfsRecovery() {
        if (!hdfsUnavailable || !running) {
            return;
        }

        long now = System.currentTimeMillis();
        if (now - lastHdfsRecoveryAttemptTime < RECOVERY_CHECK_INTERVAL_MS) {
            return;
        }
        lastHdfsRecoveryAttemptTime = now;

        performRecovery();
    }

    private void performRecovery() {
        recoveryLock.lock();
        try {
            if (!isHdfsWritable()) {
                log.debug("HDFS still unavailable, WAL writes remain skipped");
                return;
            }

            try {
                writer.close();
            } catch (IOException e) {
                log.warn("Failed to close old WAL writer during HDFS recovery", e);
            }

            writer =
                    new HdfsWALWriter(
                            config.storeId(),
                            config.executorId(),
                            config.hdfsBasePath(),
                            config.segmentSize(),
                            config.syncIntervalMs(),
                            hadoopConfig,
                            groupCommitEnabled);

            hdfsUnavailable = false;
            circuitBreakerOpen = false;
            consecutiveFailureCount.set(0);
            long skipped = skippedWriteCount.getAndSet(0);
            log.info(
                    "Recovered HDFS WAL writer. Resuming WAL writes. {} writes were skipped during outage.",
                    skipped);
        } finally {
            recoveryLock.unlock();
        }
    }

    private void maybeRecoverFromHdfsUnavailable() {
        if (!hdfsUnavailable) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastHdfsRecoveryAttemptTime < RECOVERY_CHECK_INTERVAL_MS) {
            return;
        }
        lastHdfsRecoveryAttemptTime = now;
        performRecovery();
    }

    private boolean attemptRecovery(long failedSequence, Exception error) {
        recoveryLock.lock();
        try {
            log.warn("Attempting recovery after failure at sequence: {}", failedSequence);

            boolean isHdfsError = isHdfsError(error);
            if (isHdfsError) {
                hdfsUnavailable = true;
                skippedWriteCount.incrementAndGet();
                totalSkippedWriteCount.incrementAndGet();
                log.warn(
                        "HDFS error detected. WAL writes will be skipped until HDFS recovers. "
                                + "Data is still persisted in LMDB.");
                return true;
            }

            try {
                writer.forceSync();
                log.info(
                        "Recovery completed via forceSync, resuming from sequence: {}",
                        failedSequence + 1);
                return true;
            } catch (IOException e) {
                log.error("Failed to sync during recovery", e);
            }

            return false;
        } finally {
            recoveryLock.unlock();
        }
    }

    private boolean isHdfsError(Throwable error) {
        if (error == null) {
            return false;
        }
        String className = error.getClass().getName();
        if (className.startsWith("org.apache.hadoop.")) {
            return true;
        }
        String message = error.getMessage();
        if (message != null
                && (message.contains("HDFS")
                        || message.contains("NameNode")
                        || message.contains("DataNode")
                        || message.contains("ConnectException")
                        || message.contains("NoRouteToHostException"))) {
            return true;
        }
        return isHdfsError(error.getCause());
    }
}
