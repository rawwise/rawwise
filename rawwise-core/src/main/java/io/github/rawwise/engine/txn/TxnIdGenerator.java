package io.github.rawwise.engine.txn;

import java.io.IOException;

public interface TxnIdGenerator {

    long nextTxnId() throws IOException;
}
