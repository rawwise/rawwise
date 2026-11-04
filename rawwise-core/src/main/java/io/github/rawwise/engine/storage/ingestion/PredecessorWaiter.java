package io.github.rawwise.engine.storage.ingestion;

import io.github.rawwise.engine.storage.manifest.CommitResultLog;
import io.github.rawwise.engine.storage.manifest.SnapshotOperations;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;

public class PredecessorWaiter {
    private final SnapshotOperations snapshotOps;
    private final CommitResultLog commitResultLog;
    private final Clock clock;
    private final long pollIntervalMs;
    private final long timeoutMs;

    public PredecessorWaiter(
            SnapshotOperations snapshotOps,
            CommitResultLog commitResultLog,
            Clock clock,
            long pollIntervalMs,
            long timeoutMs) {
        this.snapshotOps = Objects.requireNonNull(snapshotOps, "snapshotOps must not be null");
        this.commitResultLog =
                Objects.requireNonNull(commitResultLog, "commitResultLog must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.pollIntervalMs = pollIntervalMs;
        this.timeoutMs = timeoutMs;
    }

    public void waitFor(String tableId, long myTxnId, TxnIdAllocator txnIdAllocator)
            throws SinkRetryableException {
        Optional<Long> predTxnId = txnIdAllocator.getPredecessor(tableId, myTxnId);
        if (predTxnId.isEmpty()) {
            return;
        }

        long targetSnapshotId = commitResultLog.getMaxSnapshot(tableId, predTxnId.get());
        if (targetSnapshotId < 0) {
            return;
        }

        long deadline = clock.millis() + timeoutMs;
        while (clock.millis() < deadline) {
            Optional<Snapshot> latest;
            try {
                latest = snapshotOps.latestSnapshot(tableId);
            } catch (IOException e) {
                throw new SinkRetryableException("Failed to fetch latest snapshot", e);
            }
            if (latest.isPresent() && latest.get().snapshotId() >= targetSnapshotId) {
                return;
            }
            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SinkRetryableException("Predecessor wait interrupted");
            }
        }
        throw new SinkRetryableException(
                "Predecessor wait timeout for txnId=" + myTxnId + ", predTxnId=" + predTxnId.get());
    }

    @FunctionalInterface
    public interface TxnIdAllocator {
        Optional<Long> getPredecessor(String tableId, long txnId);
    }
}
