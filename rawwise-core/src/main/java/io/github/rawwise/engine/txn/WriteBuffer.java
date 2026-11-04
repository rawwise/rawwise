package io.github.rawwise.engine.txn;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class WriteBuffer {

    public enum BufferEntryType {
        NEW_VALUE,
        DELETED
    }

    public record BufferEntry(BufferEntryType type, byte[] value) {
        public BufferEntry {
            if (type == null) {
                throw new IllegalArgumentException("BufferEntryType must not be null");
            }
            value = value != null ? value.clone() : null;
        }

        public byte[] getValue() {
            return value != null ? value.clone() : null;
        }

        long memorySize() {
            return value != null ? value.length : 0;
        }
    }

    public record BufferResult(boolean exists, BufferEntry entry) {}

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;

    private final ConcurrentHashMap<String, ConcurrentHashMap<ByteBuffer, BufferEntry>> buffer;
    private final long maxBytes;
    private final AtomicLong currentBytes;

    public WriteBuffer() {
        this(DEFAULT_MAX_BYTES);
    }

    public WriteBuffer(long maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, got: " + maxBytes);
        }
        this.buffer = new ConcurrentHashMap<>();
        this.maxBytes = maxBytes;
        this.currentBytes = new AtomicLong(0);
    }

    public void put(String tableId, byte[] key, byte[] value) {
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("value must not be null for put operation");
        }

        ByteBuffer keyBuffer = ByteBuffer.wrap(key.clone());
        BufferEntry newEntry = new BufferEntry(BufferEntryType.NEW_VALUE, value);
        long entrySize = key.length + value.length;

        ConcurrentHashMap<ByteBuffer, BufferEntry> tableBuffer =
                buffer.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());

        boolean[] overflow = {false};
        tableBuffer.compute(
                keyBuffer,
                (k, oldEntry) -> {
                    long delta;
                    if (oldEntry != null) {
                        delta = entrySize - (key.length + oldEntry.memorySize());
                    } else {
                        delta = entrySize;
                    }

                    if (delta > 0) {
                        long newTotal = currentBytes.get() + delta;
                        if (newTotal > maxBytes) {
                            overflow[0] = true;
                            return oldEntry;
                        }
                    }

                    currentBytes.addAndGet(delta);
                    return newEntry;
                });

        if (overflow[0]) {
            throw new WriteBufferOverflowException(currentBytes.get(), maxBytes, entrySize);
        }
    }

    public void delete(String tableId, byte[] key) {
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }

        ByteBuffer keyBuffer = ByteBuffer.wrap(key.clone());
        BufferEntry newEntry = new BufferEntry(BufferEntryType.DELETED, null);
        long newSize = key.length;

        ConcurrentHashMap<ByteBuffer, BufferEntry> tableBuffer =
                buffer.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());

        boolean[] overflow = {false};
        tableBuffer.compute(
                keyBuffer,
                (k, oldEntry) -> {
                    long delta;
                    if (oldEntry != null) {
                        delta = newSize - (key.length + oldEntry.memorySize());
                    } else {
                        delta = newSize;
                    }

                    if (delta > 0) {
                        long newTotal = currentBytes.get() + delta;
                        if (newTotal > maxBytes) {
                            overflow[0] = true;
                            return oldEntry;
                        }
                    }

                    currentBytes.addAndGet(delta);
                    return newEntry;
                });

        if (overflow[0]) {
            throw new WriteBufferOverflowException(currentBytes.get(), maxBytes, newSize);
        }
    }

    public BufferResult get(String tableId, byte[] key) {
        if (tableId == null || key == null) {
            return new BufferResult(false, null);
        }

        ConcurrentHashMap<ByteBuffer, BufferEntry> tableBuffer = buffer.get(tableId);
        if (tableBuffer == null) {
            return new BufferResult(false, null);
        }

        ByteBuffer keyBuffer = ByteBuffer.wrap(key.clone());
        BufferEntry entry = tableBuffer.get(keyBuffer);

        if (entry == null) {
            return new BufferResult(false, null);
        }

        return new BufferResult(true, entry);
    }

    public boolean containsKey(String tableId, byte[] key) {
        if (tableId == null || key == null) {
            return false;
        }

        ConcurrentHashMap<ByteBuffer, BufferEntry> tableBuffer = buffer.get(tableId);
        if (tableBuffer == null) {
            return false;
        }

        ByteBuffer keyBuffer = ByteBuffer.wrap(key.clone());
        return tableBuffer.containsKey(keyBuffer);
    }

    /**
     * returnspecified tableId all WriteBuffer immutable of entrysnapshot(notremove).
     *
     * <p>for rangeRead etcrequiresiterate WriteBuffer scenario (P8g Range Read Isolation). return
     * Map is entries shallowcopysnapshot, to WriteBuffer subsequentmodifiednotwillaffectreturn Map.
     *
     * @param tableId table ID
     * @return immutable key → BufferEntry mapping, if tableId noentrythenreturnempty Map
     */
    public Map<ByteBuffer, BufferEntry> snapshotEntries(String tableId) {
        if (tableId == null) {
            return Map.of();
        }
        ConcurrentHashMap<ByteBuffer, BufferEntry> tableBuffer = buffer.get(tableId);
        if (tableBuffer == null || tableBuffer.isEmpty()) {
            return Map.of();
        }
        Map<ByteBuffer, BufferEntry> snapshot = new HashMap<>(tableBuffer);
        return Collections.unmodifiableMap(snapshot);
    }

    public Map<ByteBuffer, BufferEntry> drain(String tableId) {
        if (tableId == null) {
            return Map.of();
        }

        ConcurrentHashMap<ByteBuffer, BufferEntry> tableBuffer = buffer.remove(tableId);
        if (tableBuffer == null) {
            return Map.of();
        }

        long drainedBytes = 0;
        for (Map.Entry<ByteBuffer, BufferEntry> entry : tableBuffer.entrySet()) {
            drainedBytes += entry.getKey().remaining();
            drainedBytes += entry.getValue().memorySize();
        }
        currentBytes.addAndGet(-drainedBytes);

        Map<ByteBuffer, BufferEntry> result = new HashMap<>(tableBuffer);
        return result;
    }

    public void clear() {
        buffer.clear();
        currentBytes.set(0);
    }

    public long currentBytes() {
        return currentBytes.get();
    }

    public long maxBytes() {
        return maxBytes;
    }
}
