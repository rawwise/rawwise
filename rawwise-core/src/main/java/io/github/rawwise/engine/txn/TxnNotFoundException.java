package io.github.rawwise.engine.txn;

/**
 * whenspecifiedtxnIdinActive transaction indexwhen not found inthrowthisexception.
 *
 * <p>typicalscenario:
 *
 * <ul>
 *   <li>transactionalreadyviacommitorrollback, notinActive transaction indexin
 * </ul>
 */
public class TxnNotFoundException extends Exception {

    private final long txnId;

    public TxnNotFoundException(long txnId) {
        super("Transaction not found: txnId=" + txnId);
        this.txnId = txnId;
    }

    public TxnNotFoundException(long txnId, String message) {
        super("Transaction not found: txnId=" + txnId + ", " + message);
        this.txnId = txnId;
    }

    public TxnNotFoundException(long txnId, String message, Throwable cause) {
        super("Transaction not found: txnId=" + txnId + ", " + message, cause);
        this.txnId = txnId;
    }

    public long txnId() {
        return txnId;
    }
}
