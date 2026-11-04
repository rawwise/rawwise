package io.github.rawwise.backend.wal;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public record PendingTxn(
        long txnId, List<WALRecord> records, CountDownLatch latch, AtomicBoolean failed) {}
