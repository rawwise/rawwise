package io.github.rawwise.engine.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommitMarker {

    private static final String KEY_PREFIX = "__commit_marker__";

    private final LmdbInstance markerInstance;

    public CommitMarker(LmdbInstance markerInstance) {
        this.markerInstance = markerInstance;
    }

    public void writeCommitMarker(long txnId, List<String> instanceIds, long commitTs) {
        byte[] markerKey = (KEY_PREFIX + txnId).getBytes(StandardCharsets.UTF_8);
        byte[] markerValue = encodeInstanceIds(instanceIds, commitTs);
        LmdbInstance.PutStatus status = markerInstance.put(markerKey, markerValue);
        if (status != LmdbInstance.PutStatus.SUCCESS) {
            throw new RuntimeException(
                    "Failed to write commit marker for txnId=" + txnId + ": " + status);
        }
        log.debug("Written commit marker for txnId={}, instanceIds={}", txnId, instanceIds);
    }

    public void deleteCommitMarker(long txnId) {
        byte[] markerKey = (KEY_PREFIX + txnId).getBytes(StandardCharsets.UTF_8);
        markerInstance.batchDelete(List.of(markerKey));
        log.debug("Deleted commit marker for txnId={}", txnId);
    }

    public List<CommitRecord> scanCommitMarkers() {
        List<CommitRecord> records = new ArrayList<>();
        try (var iterator = markerInstance.closeableIterator()) {
            while (iterator.hasNext()) {
                var pair = iterator.next();
                byte[] keyBytes = pair.getLeft();
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                if (key.startsWith(KEY_PREFIX)) {
                    try {
                        long txnId = Long.parseLong(key.substring(KEY_PREFIX.length()));
                        CommitRecord record = decodeInstanceIds(pair.getRight(), txnId);
                        records.add(record);
                    } catch (Exception e) {
                        log.warn("Failed to parse commit marker key: {}", key, e);
                    }
                }
            }
        } catch (java.io.IOException e) {
            log.error("Failed to scan commit markers", e);
        }
        return records;
    }

    private byte[] encodeInstanceIds(List<String> instanceIds, long commitTs) {
        int totalSize = 8;
        List<byte[]> encodedIds = new ArrayList<>(instanceIds.size());
        for (String id : instanceIds) {
            byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);
            encodedIds.add(idBytes);
            totalSize += 4 + idBytes.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putLong(commitTs);
        for (byte[] idBytes : encodedIds) {
            buf.putInt(idBytes.length);
            buf.put(idBytes);
        }
        return buf.array();
    }

    private CommitRecord decodeInstanceIds(byte[] value, long txnId) {
        ByteBuffer buf = ByteBuffer.wrap(value);
        long commitTs = buf.getLong();
        List<String> instanceIds = new ArrayList<>();
        while (buf.hasRemaining()) {
            int len = buf.getInt();
            byte[] idBytes = new byte[len];
            buf.get(idBytes);
            instanceIds.add(new String(idBytes, StandardCharsets.UTF_8));
        }
        return new CommitRecord(txnId, instanceIds, commitTs);
    }

    public record CommitRecord(long txnId, List<String> instanceIds, long commitTs) {}
}
