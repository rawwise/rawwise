package io.github.rawwise.common.collection.queue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import org.rocksdb.*;

public class RocksdbQueue {
    private static final String QUEUE_CF = "queue";
    private static final String HEAD_KEY = "head";
    private static final String TAIL_KEY = "tail";
    private final RocksDB db;
    private final ColumnFamilyHandle queueCFHandle;

    public RocksdbQueue(RocksDB db, Map<String, ColumnFamilyHandle> cfHandleMap, String queueName)
            throws RocksDBException {
        this.db = db;
        String queueCFName = queueName(queueName);
        ColumnFamilyHandle queueCFHandle = cfHandleMap.get(queueCFName);
        if (queueCFHandle == null) {
            this.queueCFHandle =
                    db.createColumnFamily(
                            new ColumnFamilyDescriptor(
                                    queueCFName.getBytes(StandardCharsets.UTF_8)));
            cfHandleMap.put(queueCFName, this.queueCFHandle);
        } else {
            this.queueCFHandle = queueCFHandle;
        }
        db.put(this.queueCFHandle, HEAD_KEY.getBytes(StandardCharsets.UTF_8), "0".getBytes());
        db.put(this.queueCFHandle, TAIL_KEY.getBytes(StandardCharsets.UTF_8), "0".getBytes());
    }

    public void enqueue(byte[] data) throws RocksDBException {
        long tail = getTail();
        tail++;
        db.put(queueCFHandle, String.valueOf(tail).getBytes(), data);
        setTail(tail);
    }

    public byte[] dequeue() throws RocksDBException {
        long head = getHead();
        long tail = getTail();

        // empty queue
        if (head >= tail) {
            return null;
        }
        long old = head;
        head++;
        byte[] dataBytes = db.get(queueCFHandle, String.valueOf(head).getBytes());
        setHead(head);
        db.delete(String.valueOf(old).getBytes(StandardCharsets.UTF_8));
        return dataBytes;
    }

    public void close() throws RocksDBException {
        db.close();
    }

    private long getHead() throws RocksDBException {
        byte[] headBytes = db.get(queueCFHandle, HEAD_KEY.getBytes());
        return Long.parseLong(new String(headBytes, StandardCharsets.UTF_8));
    }

    private long getTail() throws RocksDBException {
        byte[] tailBytes = db.get(queueCFHandle, TAIL_KEY.getBytes());
        return Long.parseLong(new String(tailBytes, StandardCharsets.UTF_8));
    }

    private void setHead(long head) throws RocksDBException {
        db.put(queueCFHandle, HEAD_KEY.getBytes(), String.valueOf(head).getBytes());
    }

    private void setTail(long tail) throws RocksDBException {
        db.put(queueCFHandle, TAIL_KEY.getBytes(), String.valueOf(tail).getBytes());
    }

    private OptionalInt queueCFIndex(Options options, String queueName) throws RocksDBException {
        List<byte[]> columnFamilies = RocksDB.listColumnFamilies(options, db.getName());
        String realQueueName = queueName(queueName);
        return IntStream.range(0, columnFamilies.size())
                .filter(index -> Arrays.equals(columnFamilies.get(index), realQueueName.getBytes()))
                .findFirst();
    }

    private String queueName(String queueName) {
        return QUEUE_CF + "_" + queueName;
    }
}
