package io.github.rawwise.engine.storage.block.index;

import io.github.rawwise.engine.storage.ByteArrays;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RowGroupIndex {

    private final List<RowGroupEntry> entries;

    public RowGroupIndex(List<RowGroupEntry> entries) {
        this.entries = Collections.unmodifiableList(entries);
    }

    public List<Integer> candidateRowGroups(byte[] targetKey) {
        if (entries.isEmpty()) {
            return Collections.emptyList();
        }
        List<Integer> candidates = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            RowGroupEntry entry = entries.get(i);
            if (entry.mightContain(targetKey)) {
                candidates.add(i);
            }
        }
        return candidates;
    }

    public boolean anyRowGroupMightContain(byte[] targetKey) {
        for (RowGroupEntry entry : entries) {
            if (entry.mightContain(targetKey)) {
                return true;
            }
        }
        return false;
    }

    public List<RowGroupEntry> entries() {
        return entries;
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public byte[] serialize() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(entries.size());
            for (RowGroupEntry entry : entries) {
                dos.writeInt(entry.minKey.length);
                dos.write(entry.minKey);
                dos.writeInt(entry.maxKey.length);
                dos.write(entry.maxKey);
                dos.writeLong(entry.rowCount);
                dos.writeLong(entry.startPos);
            }
            return bos.toByteArray();
        }
    }

    public static RowGroupIndex deserialize(byte[] data) throws IOException {
        if (data == null || data.length < 4) {
            return new RowGroupIndex(Collections.emptyList());
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                DataInputStream dis = new DataInputStream(bis)) {
            int count = dis.readInt();
            List<RowGroupEntry> entries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int minLen = dis.readInt();
                byte[] minKey = new byte[minLen];
                dis.readFully(minKey);
                int maxLen = dis.readInt();
                byte[] maxKey = new byte[maxLen];
                dis.readFully(maxKey);
                long rowCount = dis.readLong();
                long startPos = dis.readLong();
                entries.add(new RowGroupEntry(minKey, maxKey, rowCount, startPos));
            }
            return new RowGroupIndex(entries);
        }
    }

    public static RowGroupIndex empty() {
        return new RowGroupIndex(Collections.emptyList());
    }

    public record RowGroupEntry(byte[] minKey, byte[] maxKey, long rowCount, long startPos) {
        public RowGroupEntry {
            minKey = minKey == null ? new byte[0] : minKey.clone();
            maxKey = maxKey == null ? new byte[0] : maxKey.clone();
        }

        public boolean mightContain(byte[] targetKey) {
            if (minKey.length == 0 || maxKey.length == 0) {
                return true;
            }
            if (ByteArrays.compareUnsigned(targetKey, minKey) < 0) {
                return false;
            }
            return ByteArrays.compareUnsigned(targetKey, maxKey) <= 0;
        }
    }
}
