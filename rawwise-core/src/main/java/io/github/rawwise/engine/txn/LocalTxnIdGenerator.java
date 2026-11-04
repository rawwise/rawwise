package io.github.rawwise.engine.txn;

import java.util.concurrent.atomic.AtomicLong;

public class LocalTxnIdGenerator implements TxnIdGenerator {

    private final AtomicLong localId = new AtomicLong(0);

    @Override
    public long nextTxnId() {
        return localId.incrementAndGet();
    }
}
