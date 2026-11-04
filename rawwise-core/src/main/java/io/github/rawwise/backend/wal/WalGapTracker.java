package io.github.rawwise.backend.wal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WalGapTracker {
    private final ConcurrentLinkedDeque<GapEntry> pendingRecords;
    private final AtomicLong totalGapCount;
    private final int maxPendingGapRecords;

    public WalGapTracker(int maxPendingGapRecords) {
        this.pendingRecords = new ConcurrentLinkedDeque<>();
        this.totalGapCount = new AtomicLong(0);
        this.maxPendingGapRecords = maxPendingGapRecords;
    }

    public void recordGap(long sequence, byte[] walData) {
        if (pendingRecords.size() >= maxPendingGapRecords) {
            pendingRecords.pollFirst();
            log.warn(
                    "WalGapTracker at capacity ({}), dropping oldest gap entry",
                    maxPendingGapRecords);
        }
        pendingRecords.addLast(new GapEntry(sequence, walData));
        totalGapCount.incrementAndGet();
    }

    public List<GapEntry> getPendingRecords() {
        List<GapEntry> snapshot = new ArrayList<>(pendingRecords);
        return Collections.unmodifiableList(snapshot);
    }

    public long getTotalGapCount() {
        return totalGapCount.get();
    }

    public int getPendingGapCount() {
        return pendingRecords.size();
    }

    public void clearPending() {
        pendingRecords.clear();
        log.info("WalGapTracker: pending gap records cleared");
    }

    public void reset() {
        pendingRecords.clear();
        totalGapCount.set(0);
    }

    public record GapEntry(long sequence, byte[] walData) {
        public GapEntry {
            if (walData == null) {
                walData = new byte[0];
            }
        }
    }
}
