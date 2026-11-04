package io.github.rawwise.frontend.api.session;

import io.github.rawwise.engine.txn.IsolationLevel;
import java.util.Set;

public record TxnSession(
        long txnId,
        IsolationLevel isolationLevel,
        Set<String> tableIds,
        long createdAtMs,
        long lastActivityMs) {
    public boolean isActive() {
        return txnId != 0;
    }

    public TxnSession clear() {
        return new TxnSession(0, null, null, 0, 0);
    }

    public TxnSession withActivity() {
        return new TxnSession(
                txnId, isolationLevel, tableIds, createdAtMs, System.currentTimeMillis());
    }
}
