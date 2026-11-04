package io.github.rawwise.engine.storage.block.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrimaryKeyIndex {

    private final ConcurrentHashMap<ByteBuffer, List<PKLocation>> index = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<ByteBuffer>> blockToKeys =
            new ConcurrentHashMap<>();

    public void put(byte[] encodedKey, String blockId, int rowGroupIndex, long snapshotId) {
        ByteBuffer keyBuf = ByteBuffer.wrap(encodedKey.clone());
        PKLocation loc = new PKLocation(blockId, rowGroupIndex, snapshotId);
        index.compute(
                keyBuf,
                (k, existing) -> {
                    if (existing == null) {
                        List<PKLocation> list = new ArrayList<>();
                        list.add(loc);
                        return list;
                    }
                    List<PKLocation> merged = new ArrayList<>(existing);
                    merged.add(loc);
                    return merged;
                });
        blockToKeys.computeIfAbsent(blockId, k -> ConcurrentHashMap.newKeySet()).add(keyBuf);
    }

    public List<PKLocation> lookup(byte[] encodedKey) {
        ByteBuffer keyBuf = ByteBuffer.wrap(encodedKey);
        List<PKLocation> result = index.get(keyBuf);
        return result != null ? Collections.unmodifiableList(result) : Collections.emptyList();
    }

    public boolean mightContain(byte[] encodedKey) {
        return index.containsKey(ByteBuffer.wrap(encodedKey));
    }

    public void removeBlock(String blockId) {
        Set<ByteBuffer> affectedKeys = blockToKeys.remove(blockId);
        if (affectedKeys == null) {
            return;
        }
        for (ByteBuffer key : affectedKeys) {
            index.computeIfPresent(
                    key,
                    (k, list) -> {
                        List<PKLocation> filtered = new ArrayList<>();
                        for (PKLocation loc : list) {
                            if (!loc.blockId().equals(blockId)) {
                                filtered.add(loc);
                            }
                        }
                        return filtered.isEmpty() ? null : filtered;
                    });
        }
    }

    public int size() {
        return index.size();
    }

    public boolean isEmpty() {
        return index.isEmpty();
    }

    public void clear() {
        index.clear();
        blockToKeys.clear();
    }

    public byte[] serialize() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(index.size());
            for (var entry : index.entrySet()) {
                ByteBuffer keyBuf = entry.getKey();
                byte[] keyBytes = new byte[keyBuf.remaining()];
                keyBuf.duplicate().get(keyBytes);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);

                List<PKLocation> locations = entry.getValue();
                dos.writeInt(locations.size());
                for (PKLocation loc : locations) {
                    byte[] blockIdBytes =
                            loc.blockId().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    dos.writeInt(blockIdBytes.length);
                    dos.write(blockIdBytes);
                    dos.writeInt(loc.rowGroupIndex());
                    dos.writeLong(loc.snapshotId());
                }
            }
            return bos.toByteArray();
        }
    }

    public int deserializeAndMerge(byte[] data) throws IOException {
        if (data == null || data.length < 4) {
            return 0;
        }
        int loaded = 0;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                DataInputStream dis = new DataInputStream(bis)) {
            int entryCount = dis.readInt();
            for (int i = 0; i < entryCount; i++) {
                int keyLen = dis.readInt();
                byte[] keyBytes = new byte[keyLen];
                dis.readFully(keyBytes);

                int locCount = dis.readInt();
                List<PKLocation> locations = new ArrayList<>(locCount);
                for (int j = 0; j < locCount; j++) {
                    int blockIdLen = dis.readInt();
                    byte[] blockIdBytes = new byte[blockIdLen];
                    dis.readFully(blockIdBytes);
                    String blockId =
                            new String(blockIdBytes, java.nio.charset.StandardCharsets.UTF_8);
                    int rowGroupIndex = dis.readInt();
                    long snapshotId = dis.readLong();
                    locations.add(new PKLocation(blockId, rowGroupIndex, snapshotId));
                }
                ByteBuffer keyBuf = ByteBuffer.wrap(keyBytes);
                index.compute(
                        keyBuf,
                        (k, existing) -> {
                            if (existing == null) {
                                return Collections.unmodifiableList(locations);
                            }
                            List<PKLocation> merged = new ArrayList<>(existing);
                            merged.addAll(locations);
                            return Collections.unmodifiableList(merged);
                        });
                for (PKLocation loc : locations) {
                    blockToKeys
                            .computeIfAbsent(loc.blockId(), k -> ConcurrentHashMap.newKeySet())
                            .add(keyBuf);
                }
                loaded++;
            }
        }
        return loaded;
    }

    public enum IndexCoverage {
        EMPTY,
        PARTIAL,
        UNKNOWN
    }

    public IndexCoverage coverage() {
        if (index.isEmpty()) return IndexCoverage.EMPTY;
        return IndexCoverage.UNKNOWN;
    }

    public record PKLocation(String blockId, int rowGroupIndex, long snapshotId) {}
}
