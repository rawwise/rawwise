package io.github.rawwise.model.record.keygen;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TimestampKeyGenerator implements KeyGenerator {

    private final ConcurrentMap<Integer, PartitionState> stateMap = new ConcurrentHashMap<>();

    private static class PartitionState {
        private final AtomicLong lastTimestamp = new AtomicLong(-1);
        private final AtomicInteger seq = new AtomicInteger(0);
    }

    @Override
    public byte[] generateKey(int partitionId) {
        PartitionState state = stateMap.computeIfAbsent(partitionId, k -> new PartitionState());
        long now = System.currentTimeMillis();
        long last = state.lastTimestamp.get();

        if (now < last) {
            // time rollback,exception(wait until exceeding last timestamp)
            while (now < last) {
                now = System.currentTimeMillis();
            }
        }

        int sequence;
        if (now == last) {
            // within same millisecond,sequence number increments
            sequence = state.seq.incrementAndGet();
        } else {
            // new millisecond,reset sequence number
            state.seq.set(0);
            sequence = 0;
            state.lastTimestamp.set(now);
        }

        // encode as 16-byte big-endian
        byte[] key = new byte[16];
        // timestamp 8 bytes
        key[0] = (byte) (now >>> 56);
        key[1] = (byte) (now >>> 48);
        key[2] = (byte) (now >>> 40);
        key[3] = (byte) (now >>> 32);
        key[4] = (byte) (now >>> 24);
        key[5] = (byte) (now >>> 16);
        key[6] = (byte) (now >>> 8);
        key[7] = (byte) now;
        // partition ID 4 bytes
        key[8] = (byte) (partitionId >>> 24);
        key[9] = (byte) (partitionId >>> 16);
        key[10] = (byte) (partitionId >>> 8);
        key[11] = (byte) partitionId;
        // sequence number 4 bytes
        key[12] = (byte) (sequence >>> 24);
        key[13] = (byte) (sequence >>> 16);
        key[14] = (byte) (sequence >>> 8);
        key[15] = (byte) sequence;

        return key;
    }
}
