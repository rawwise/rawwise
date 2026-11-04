package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.backend.spark.routing.SparkMurmur3ShardResolver;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class BatchPointUpdateRouter {

    private final int shardCount;
    private final SparkMurmur3ShardResolver shardResolver;
    private final KeyCodec keyCodec;

    public BatchPointUpdateRouter(int shardCount) {
        this(shardCount, new DefaultKeyCodec());
    }

    public BatchPointUpdateRouter(int shardCount, KeyCodec keyCodec) {
        if (shardCount <= 0) throw new IllegalArgumentException("shardCount must be positive");
        this.shardCount = shardCount;
        this.shardResolver = new SparkMurmur3ShardResolver(shardCount);
        this.keyCodec = Objects.requireNonNull(keyCodec);
    }

    public record PointUpdate(
            Map<String, Comparable<?>> primaryKeyValues, Map<String, Comparable<?>> updates) {
        public PointUpdate {
            Objects.requireNonNull(primaryKeyValues, "primaryKeyValues");
            primaryKeyValues = Map.copyOf(primaryKeyValues);
            updates = updates != null ? Map.copyOf(updates) : Map.of();
        }
    }

    public Map<Integer, List<PointUpdate>> route(List<PointUpdate> updates) {
        if (updates == null || updates.isEmpty()) return Map.of();

        Map<Integer, List<PointUpdate>> grouped = new LinkedHashMap<>();
        for (PointUpdate update : updates) {
            int shardId = resolveShardFromCompositeKey(update.primaryKeyValues());
            grouped.computeIfAbsent(shardId, k -> new ArrayList<>()).add(update);
        }
        log.debug(
                "BatchPointUpdateRouter: routed {} updates into {} shards",
                updates.size(),
                grouped.size());
        return grouped;
    }

    @SuppressWarnings("rawtypes")
    int resolveShardFromCompositeKey(Map<String, Comparable<?>> primaryKeyValues) {
        try {
            byte[] keyBytes = keyCodec.encodePrimaryKey((Map) primaryKeyValues);
            return shardResolver.resolve(keyBytes).value();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to encode primary key for shard resolution", e);
        }
    }
}
