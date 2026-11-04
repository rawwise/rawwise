package io.github.rawwise.engine.vector;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public non-sealed class RemoteShardIndex implements ShardInternalIndex {

    @Getter private final String tableId;
    @Getter private final String shardId;
    private final ShardClient client;
    private final ConcurrentHashMap<String, VectorLocation> keyToLocation =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> keyToStorageRef = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, float[]> cachedVectors = new ConcurrentHashMap<>();
    private final Set<String> tombstones = ConcurrentHashMap.newKeySet();
    @Getter private volatile boolean initialized = true;

    public RemoteShardIndex(String tableId, String shardId, ShardClient client) {
        this.tableId = tableId;
        this.shardId = shardId;
        this.client = client;
    }

    public boolean isAvailable() {
        return client.isAvailable(shardId);
    }

    public void cacheVector(String entryKey, float[] vector) {
        if (entryKey == null || vector == null) return;
        cachedVectors.put(entryKey, vector.clone());
        tombstones.remove(entryKey);
    }

    @Override
    public void initialize(List<float[]> vectors, List<String> keys) {
        throw new UnsupportedOperationException(
                "RemoteShardIndex does not support local initialization");
    }

    @Override
    public void insert(String entryKey, float[] vector) {
        throw new UnsupportedOperationException("RemoteShardIndex does not support local insert");
    }

    @Override
    public List<SearchCandidate> search(float[] query, int topK) {
        if (!client.isAvailable(shardId)) {
            log.warn("Remote shard {} unavailable, returning empty", shardId);
            return List.of();
        }
        return client.search(tableId, shardId, query, topK);
    }

    @Override
    public void updateLocation(String entryKey, VectorLocation location, String storageRef) {
        if (entryKey == null) return;
        if (location == null) {
            keyToLocation.remove(entryKey);
        } else {
            keyToLocation.put(entryKey, location);
        }
        if (storageRef == null) {
            keyToStorageRef.remove(entryKey);
        } else {
            keyToStorageRef.put(entryKey, storageRef);
        }
        tombstones.remove(entryKey);
    }

    @Override
    public VectorLocation getLocation(String entryKey) {
        if (entryKey == null || tombstones.contains(entryKey)) return null;
        return keyToLocation.get(entryKey);
    }

    @Override
    public String getStorageRef(String entryKey) {
        if (entryKey == null || tombstones.contains(entryKey)) return null;
        return keyToStorageRef.get(entryKey);
    }

    @Override
    public void markTombstone(String entryKey) {
        if (entryKey != null) {
            tombstones.add(entryKey);
            cachedVectors.remove(entryKey);
        }
    }

    @Override
    public int vectorCount() {
        int count = 0;
        for (String key : keyToLocation.keySet()) {
            if (!tombstones.contains(key)) count++;
        }
        return count;
    }

    @Override
    public Set<String> entryKeys() {
        Set<String> result = new HashSet<>(keyToLocation.keySet());
        result.removeAll(tombstones);
        return result;
    }

    @Override
    public float[] getVector(String entryKey) {
        if (entryKey == null || tombstones.contains(entryKey)) return null;
        float[] cached = cachedVectors.get(entryKey);
        return cached == null ? null : cached.clone();
    }

    @Override
    public void close() {
        keyToLocation.clear();
        keyToStorageRef.clear();
        cachedVectors.clear();
        tombstones.clear();
    }
}
