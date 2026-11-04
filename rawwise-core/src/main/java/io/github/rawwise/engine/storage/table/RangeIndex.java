package io.github.rawwise.engine.storage.table;

import io.github.rawwise.engine.storage.LmdbInstance;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RangeIndex {

    private static final Comparator<ByteBuffer> UNSIGNED_COMPARATOR =
            (a, b) -> {
                int aPos = a.position();
                int bPos = b.position();
                int aLen = a.remaining();
                int bLen = b.remaining();
                int max = Math.min(aLen, bLen);
                for (int i = 0; i < max; i++) {
                    int l = Byte.toUnsignedInt(a.get(aPos + i));
                    int r = Byte.toUnsignedInt(b.get(bPos + i));
                    if (l != r) return l - r;
                }
                return aLen - bLen;
            };

    private final ConcurrentSkipListMap<ByteBuffer, Set<String>> index =
            new ConcurrentSkipListMap<>(UNSIGNED_COMPARATOR);
    private final ConcurrentHashMap<String, ByteBuffer> reverseIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ByteBuffer> maxKeyIndex = new ConcurrentHashMap<>();

    public static RangeIndex rebuild(
            List<LmdbInstance> readOnlyInstances, List<LmdbInstance> writableInstances) {
        RangeIndex newIndex = new RangeIndex();
        for (LmdbInstance instance : readOnlyInstances) {
            newIndex.register(instance);
        }
        for (LmdbInstance instance : writableInstances) {
            newIndex.register(instance);
        }
        return newIndex;
    }

    public void register(LmdbInstance instance) {
        byte[] minKey = instance.minKey();
        byte[] maxKey = instance.maxKey();
        if (minKey == null || maxKey == null || minKey.length == 0 || maxKey.length == 0) {
            log.warn(
                    "Skipping RangeIndex registration for instance {}: empty key range",
                    instance.instanceId());
            return;
        }
        ByteBuffer keyBuf = ByteBuffer.wrap(minKey.clone());
        String instanceId = instance.instanceId();
        index.compute(
                keyBuf,
                (k, existing) -> {
                    Set<String> set = existing != null ? existing : ConcurrentHashMap.newKeySet();
                    set.add(instanceId);
                    return set;
                });
        reverseIndex.put(instanceId, keyBuf);
        maxKeyIndex.put(instanceId, ByteBuffer.wrap(maxKey.clone()));
    }

    public void unregister(String instanceId) {
        ByteBuffer keyBuf = reverseIndex.remove(instanceId);
        maxKeyIndex.remove(instanceId);
        if (keyBuf == null) return;
        index.computeIfPresent(
                keyBuf,
                (k, set) -> {
                    set.remove(instanceId);
                    return set.isEmpty() ? null : set;
                });
    }

    public List<String> lookup(byte[] targetKey) {
        if (index.isEmpty()) {
            return Collections.emptyList();
        }
        ByteBuffer targetBuf = ByteBuffer.wrap(targetKey);
        List<String> candidates = new ArrayList<>();
        NavigableMap<ByteBuffer, Set<String>> head = index.headMap(targetBuf, true);
        for (var entry : head.entrySet()) {
            for (String instanceId : entry.getValue()) {
                ByteBuffer maxKeyBuf = maxKeyIndex.get(instanceId);
                if (maxKeyBuf != null && UNSIGNED_COMPARATOR.compare(targetBuf, maxKeyBuf) <= 0) {
                    candidates.add(instanceId);
                }
            }
        }
        return candidates;
    }

    public List<String> lookupRange(byte[] startKey, byte[] endKey) {
        if (index.isEmpty()) {
            return Collections.emptyList();
        }

        ByteBuffer endBuf = endKey != null ? ByteBuffer.wrap(endKey) : null;
        ByteBuffer startBuf = startKey != null ? ByteBuffer.wrap(startKey) : null;

        NavigableMap<ByteBuffer, Set<String>> candidateMap;
        if (endBuf != null) {
            candidateMap = index.headMap(endBuf, true);
        } else {
            candidateMap = index;
        }

        List<String> candidates = new ArrayList<>();
        for (var entry : candidateMap.entrySet()) {
            for (String instanceId : entry.getValue()) {
                ByteBuffer maxKeyBuf = maxKeyIndex.get(instanceId);
                if (maxKeyBuf == null) continue;
                if (startBuf != null && UNSIGNED_COMPARATOR.compare(maxKeyBuf, startBuf) < 0) {
                    continue;
                }
                candidates.add(instanceId);
            }
        }
        return candidates;
    }

    public void clear() {
        index.clear();
        reverseIndex.clear();
        maxKeyIndex.clear();
    }

    public int size() {
        return index.size();
    }

    public boolean isEmpty() {
        return index.isEmpty();
    }
}
