package io.github.rawwise.backend.wal;

public record WalCheckpoint(String tableId, long maxCleanedSequence, long checkpointTimestamp) {
    public WalCheckpoint {
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (maxCleanedSequence < 0) {
            throw new IllegalArgumentException("maxCleanedSequence must be non-negative");
        }
    }
}
