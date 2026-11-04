package io.github.rawwise.engine.storage.block.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class SparseIndex {

    private static final int MAGIC = 0x53504958;
    private static final byte VERSION = 1;

    private final List<SparseIndexEntry> entries;

    public SparseIndex(List<SparseIndexEntry> entries) {
        this.entries = List.copyOf(entries);
    }

    public List<SparseIndexEntry> entries() {
        return entries;
    }

    public long floorRowIndex(byte[] key) {
        int lo = 0, hi = entries.size() - 1;
        long result = 0;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = compareBytes(entries.get(mid).key, key);
            if (cmp <= 0) {
                result = entries.get(mid).rowIndex;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result;
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }

    public record SparseIndexEntry(byte[] key, long rowIndex) {}

    public static byte[] serialize(SparseIndex index) {
        int capacity = 4 + 1 + 4;
        for (SparseIndexEntry entry : index.entries) {
            capacity += 2 + entry.key.length + 8;
        }
        ByteBuffer buf = ByteBuffer.allocate(capacity);
        buf.putInt(MAGIC);
        buf.put(VERSION);
        buf.putInt(index.entries.size());
        for (SparseIndexEntry entry : index.entries) {
            buf.putShort((short) entry.key.length);
            buf.put(entry.key);
            buf.putLong(entry.rowIndex);
        }
        return buf.array();
    }

    public static SparseIndex deserialize(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        if (buf.remaining() < 9) {
            return null;
        }
        if (buf.getInt() != MAGIC) {
            return null;
        }
        if (buf.get() != VERSION) {
            return null;
        }
        int entryCount = buf.getInt();
        if (entryCount <= 0) {
            return null;
        }
        List<SparseIndexEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            if (buf.remaining() < 2) {
                return null;
            }
            int keyLen = Short.toUnsignedInt(buf.getShort());
            if (buf.remaining() < keyLen + 8) {
                return null;
            }
            byte[] key = new byte[keyLen];
            buf.get(key);
            long rowIndex = buf.getLong();
            entries.add(new SparseIndexEntry(key, rowIndex));
        }
        return new SparseIndex(entries);
    }
}
