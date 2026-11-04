package io.github.rawwise.backend.wal;

public class WalSyncFailedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final long txnId;

    public WalSyncFailedException(long txnId, String message) {
        super("WAL sync failed for txn " + txnId + ": " + message);
        this.txnId = txnId;
    }

    public WalSyncFailedException(long txnId, String message, Throwable cause) {
        super("WAL sync failed for txn " + txnId + ": " + message, cause);
        this.txnId = txnId;
    }

    public long getTxnId() {
        return txnId;
    }
}
