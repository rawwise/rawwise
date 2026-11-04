package io.github.rawwise.engine.txn;

import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TxnIdAllocator implements TxnIdGenerator {

    private static final byte[] COUNTER_KEY = "txn:counter:global".getBytes();
    private final Metastore store;
    private final int segmentSize;
    private final AtomicLong segmentBase = new AtomicLong();
    private final AtomicLong segmentRemaining = new AtomicLong();

    public TxnIdAllocator(Metastore store, int segmentSize) {
        if (store == null) {
            throw new IllegalArgumentException("Metastore must not be null");
        }
        if (segmentSize <= 0) {
            throw new IllegalArgumentException("Segment size must be positive");
        }
        this.store = store;
        this.segmentSize = segmentSize;
    }

    @Override
    public long nextTxnId() throws IOException {
        while (true) {
            long remaining = segmentRemaining.getAndDecrement();
            if (remaining > 0) {
                return segmentBase.get() + (segmentSize - remaining);
            }
            synchronized (this) {
                if (segmentRemaining.get() <= 0) {
                    long allocated = allocateSegment();
                    segmentBase.set(allocated);
                    segmentRemaining.set(segmentSize);
                }
            }
        }
    }

    private long allocateSegment() throws IOException {
        while (true) {
            byte[] currentBytes = store.get(COUNTER_KEY).orElse(new byte[8]);
            long current = ByteBuffer.wrap(currentBytes).getLong();
            long next = current + segmentSize;
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putLong(next);
            byte[] nextBytes = buf.array();

            List<Precondition> preconditions;
            if (Arrays.equals(currentBytes, new byte[8])) {
                preconditions = List.of(new Precondition.NotExists(COUNTER_KEY));
            } else {
                preconditions = List.of(new Precondition.ValueEquals(COUNTER_KEY, currentBytes));
            }

            boolean committed =
                    store.compareAndSwap(
                            preconditions, List.of(new Mutation.Put(COUNTER_KEY, nextBytes)));
            if (committed) return current + 1;
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("TxnId allocation interrupted", e);
            }
        }
    }
}
