package io.github.rawwise.common.collection.queue;

import static io.github.rawwise.common.Strs.leftPad;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.*;

public class RocksdbPriorityQueue {
    private static final String PRIORITY_QUEUE_CF = "priority_queue";

    private final RocksDB db;
    private final AtomicLong counter = new AtomicLong();
    private final ColumnFamilyHandle priorityQueueCFHandle;

    public RocksdbPriorityQueue(
            RocksDB db, Map<String, ColumnFamilyHandle> cfHandleMap, String queueName)
            throws RocksDBException {
        this.db = db;
        String queueCFName = queueName(queueName);
        ColumnFamilyHandle queueCFHandle = cfHandleMap.get(queueCFName);
        if (queueCFHandle == null) {
            this.priorityQueueCFHandle =
                    db.createColumnFamily(
                            new ColumnFamilyDescriptor(
                                    queueCFName.getBytes(StandardCharsets.UTF_8)));
            cfHandleMap.put(queueCFName, this.priorityQueueCFHandle);
        } else {
            this.priorityQueueCFHandle = queueCFHandle;
        }
    }

    public void enqueue(byte[] data, int priority) throws RocksDBException {
        long adjustedPriority = -priority;
        long uniqueId = counter.incrementAndGet();
        db.put(priorityQueueCFHandle, key(adjustedPriority, uniqueId).getBytes(), data);
    }

    public byte[] dequeue() throws RocksDBException {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            if (iterator.isValid()) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                db.delete(priorityQueueCFHandle, key);
                return value;
            }
        }
        // empty queue
        return null;
    }

    private String key(long adjustedPriority, long uniqueId) {
        return leftPad(Long.toString(adjustedPriority), 20)
                + "_"
                + leftPad(Long.toString(uniqueId), 20);
    }

    public void close() {
        db.close();
    }

    private String queueName(String queueName) {
        return PRIORITY_QUEUE_CF + "_" + queueName;
    }
}
