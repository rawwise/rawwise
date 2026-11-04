package io.github.rawwise.backend.spark.routing;

import io.github.rawwise.backend.routing.ShardLookupResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public final class ShardOwnerCache {
    private final Function<Set<Integer>, Map<Integer, ShardLookupResult>> batchLookup;
    private final HashMap<Integer, ShardLookupResult> cache = new HashMap<>();

    public ShardOwnerCache(Function<Set<Integer>, Map<Integer, ShardLookupResult>> batchLookup) {
        this.batchLookup = Objects.requireNonNull(batchLookup, "batchLookup must not be null");
    }

    public Map<Integer, ShardLookupResult> get(Set<Integer> shardIds) {
        return getInternal(shardIds, false);
    }

    public Map<Integer, ShardLookupResult> refresh(Set<Integer> shardIds) {
        return getInternal(shardIds, true);
    }

    private Map<Integer, ShardLookupResult> getInternal(
            Set<Integer> shardIds, boolean forceRefresh) {
        Objects.requireNonNull(shardIds, "shardIds must not be null");
        if (shardIds.isEmpty()) {
            return Map.of();
        }

        HashSet<Integer> missing = new HashSet<>();
        if (forceRefresh) {
            missing.addAll(shardIds);
        } else {
            for (Integer sid : shardIds) {
                if (!cache.containsKey(sid)) {
                    missing.add(sid);
                }
            }
        }

        if (!missing.isEmpty()) {
            Map<Integer, ShardLookupResult> looked = batchLookup.apply(Set.copyOf(missing));
            if (looked != null) {
                cache.putAll(looked);
            }
        }

        HashMap<Integer, ShardLookupResult> out = new HashMap<>(Math.max(16, shardIds.size() * 2));
        for (Integer sid : shardIds) {
            out.put(sid, cache.get(sid));
        }
        return out;
    }
}
