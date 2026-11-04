package io.github.rawwise.engine.clean;

import io.github.rawwise.governance.metrics.RawWiseMetrics;
import io.micrometer.core.instrument.Counter;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CleanMetrics {

    private final AtomicLong orphanFilesScanned = new AtomicLong();
    private final AtomicLong orphanFilesDeleted = new AtomicLong();
    private final AtomicLong ttlFilesScanned = new AtomicLong();
    private final AtomicLong ttlFilesDeleted = new AtomicLong();
    private final AtomicLong ttlFilesRewritten = new AtomicLong();
    private final AtomicLong snapshotsExpired = new AtomicLong();
    private final AtomicLong bytesReclaimed = new AtomicLong();
    private final AtomicLong bytesScheduledForDeletion = new AtomicLong();
    private final AtomicLong bytesPhysicallyDeleted = new AtomicLong();
    private final AtomicLong bytesInDeadLetter = new AtomicLong();
    private final AtomicLong cleanCommitConflicts = new AtomicLong();
    private final AtomicLong ttlColumnNotFoundCount = new AtomicLong();
    private final AtomicLong durationMs = new AtomicLong();

    private Counter orphanScannedCounter;
    private Counter orphanDeletedCounter;
    private Counter ttlScannedCounter;
    private Counter ttlDeletedCounter;
    private Counter ttlRewrittenCounter;
    private Counter snapshotsExpiredCounter;
    private Counter commitConflictCounter;

    public CleanMetrics() {
        if (RawWiseMetrics.isInitialized()) {
            registerMicrometer();
        }
    }

    private void registerMicrometer() {
        orphanScannedCounter = RawWiseMetrics.counter("rawwise.clean.orphan.scanned");
        orphanDeletedCounter = RawWiseMetrics.counter("rawwise.clean.orphan.deleted");
        ttlScannedCounter = RawWiseMetrics.counter("rawwise.clean.ttl.scanned");
        ttlDeletedCounter = RawWiseMetrics.counter("rawwise.clean.ttl.deleted");
        ttlRewrittenCounter = RawWiseMetrics.counter("rawwise.clean.ttl.rewritten");
        snapshotsExpiredCounter = RawWiseMetrics.counter("rawwise.clean.snapshots.expired");
        commitConflictCounter = RawWiseMetrics.counter("rawwise.clean.commit.conflicts");
    }

    public void recordOrphanScan(long count) {
        orphanFilesScanned.addAndGet(count);
        if (orphanScannedCounter != null) orphanScannedCounter.increment(count);
    }

    public void recordOrphanDelete(long count) {
        orphanFilesDeleted.addAndGet(count);
        if (orphanDeletedCounter != null) orphanDeletedCounter.increment(count);
    }

    public void recordTTLScan(long count) {
        ttlFilesScanned.addAndGet(count);
        if (ttlScannedCounter != null) ttlScannedCounter.increment(count);
    }

    public void recordTTLDelete(long count) {
        ttlFilesDeleted.addAndGet(count);
        if (ttlDeletedCounter != null) ttlDeletedCounter.increment(count);
    }

    public void recordTTLRewrite(long count) {
        ttlFilesRewritten.addAndGet(count);
        if (ttlRewrittenCounter != null) ttlRewrittenCounter.increment(count);
    }

    public void recordSnapshotExpire(long count) {
        snapshotsExpired.addAndGet(count);
        if (snapshotsExpiredCounter != null) snapshotsExpiredCounter.increment(count);
    }

    public void addBytesScheduledForDeletion(long bytes) {
        bytesScheduledForDeletion.addAndGet(bytes);
    }

    public void addBytesPhysicallyDeleted(long bytes) {
        bytesPhysicallyDeleted.addAndGet(bytes);
    }

    public void addBytesInDeadLetter(long bytes) {
        bytesInDeadLetter.addAndGet(bytes);
    }

    public void recordCleanCommitConflict() {
        cleanCommitConflicts.incrementAndGet();
        if (commitConflictCounter != null) commitConflictCounter.increment();
    }

    public void recordTTLColumnNotFound() {
        ttlColumnNotFoundCount.incrementAndGet();
    }

    public void setDurationMs(long ms) {
        durationMs.set(ms);
    }

    public long getOrphanFilesScanned() {
        return orphanFilesScanned.get();
    }

    public long getOrphanFilesDeleted() {
        return orphanFilesDeleted.get();
    }

    public long getTtlFilesScanned() {
        return ttlFilesScanned.get();
    }

    public long getTtlFilesDeleted() {
        return ttlFilesDeleted.get();
    }

    public long getTtlFilesRewritten() {
        return ttlFilesRewritten.get();
    }

    public long getSnapshotsExpired() {
        return snapshotsExpired.get();
    }

    public long getBytesScheduledForDeletion() {
        return bytesScheduledForDeletion.get();
    }

    public long getBytesPhysicallyDeleted() {
        return bytesPhysicallyDeleted.get();
    }

    public long getBytesInDeadLetter() {
        return bytesInDeadLetter.get();
    }

    public long getCleanCommitConflicts() {
        return cleanCommitConflicts.get();
    }

    public long getTtlColumnNotFoundCount() {
        return ttlColumnNotFoundCount.get();
    }

    public long getDurationMs() {
        return durationMs.get();
    }

    public String snapshot() {
        return String.format(
                "CleanMetrics{orphanScanned=%d, orphanDeleted=%d, ttlScanned=%d, ttlDeleted=%d, ttlRewritten=%d, snapshotsExpired=%d, bytesReclaimed=%d, bytesScheduledForDeletion=%d, bytesPhysicallyDeleted=%d, bytesInDeadLetter=%d, cleanCommitConflicts=%d, ttlColumnNotFound=%d, durationMs=%d}",
                orphanFilesScanned.get(),
                orphanFilesDeleted.get(),
                ttlFilesScanned.get(),
                ttlFilesDeleted.get(),
                ttlFilesRewritten.get(),
                snapshotsExpired.get(),
                bytesReclaimed.get(),
                bytesScheduledForDeletion.get(),
                bytesPhysicallyDeleted.get(),
                bytesInDeadLetter.get(),
                cleanCommitConflicts.get(),
                ttlColumnNotFoundCount.get(),
                durationMs.get());
    }
}
