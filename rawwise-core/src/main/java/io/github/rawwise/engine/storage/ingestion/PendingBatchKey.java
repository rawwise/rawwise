package io.github.rawwise.engine.storage.ingestion;

public record PendingBatchKey(String tableId, long txnId) {
    public PendingBatchKey {
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be null or blank");
        }
    }

    public static PendingBatchKey of(String tableId, long txnId) {
        return new PendingBatchKey(tableId, txnId);
    }
}
