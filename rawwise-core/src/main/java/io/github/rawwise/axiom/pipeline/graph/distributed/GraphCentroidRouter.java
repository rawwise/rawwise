package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.axiom.pipeline.entity.Entity;
import io.github.rawwise.axiom.pipeline.entity.EntityType;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphCentroidRouter {

    public static final int DEFAULT_SHARD_COUNT = 8;

    private final EntityCentroidStore centroidStore;
    private final NameIndexResolver nameResolver;
    private final int shardCount;
    private final boolean fallbackOnError;
    private volatile Map<String, EntityCentroid> centroidMap = new ConcurrentHashMap<>();
    private volatile Map<ShardId, EntityBloomFilter> bloomFilters = new ConcurrentHashMap<>();
    private final AtomicLong lastRefreshMs = new AtomicLong(0L);
    private volatile boolean initialized = false;

    public GraphCentroidRouter(
            EntityCentroidStore centroidStore, NameIndexResolver nameResolver, int shardCount) {
        this(
                centroidStore,
                nameResolver,
                shardCount,
                Boolean.parseBoolean(
                        System.getProperty(
                                "rawwise.graph.centroid-router.fallback-on-error", "true")));
    }

    public GraphCentroidRouter(
            EntityCentroidStore centroidStore,
            NameIndexResolver nameResolver,
            int shardCount,
            boolean fallbackOnError) {
        this.centroidStore = Objects.requireNonNull(centroidStore, "centroidStore");
        this.nameResolver = nameResolver == null ? NameIndexResolver.empty() : nameResolver;
        this.shardCount = shardCount <= 0 ? DEFAULT_SHARD_COUNT : shardCount;
        this.fallbackOnError = fallbackOnError;
        for (int i = 0; i < this.shardCount; i++) {
            bloomFilters.put(new ShardId(i), new EntityBloomFilter(1_000_000L, 0.001));
        }
    }

    public GraphCentroidRouter(EntityCentroidStore centroidStore, NameIndexResolver nameResolver) {
        this(centroidStore, nameResolver, DEFAULT_SHARD_COUNT);
    }

    public ShardId routeByEntityId(String entityId) {
        EntityCentroid c = centroidMap.get(entityId);
        if (c != null && c.shardId() != null) {
            return c.shardId();
        }
        Optional<EntityCentroid> looked = centroidStore.lookup(entityId);
        if (looked.isPresent() && looked.get().shardId() != null) {
            centroidMap.put(entityId, looked.get());
            return looked.get().shardId();
        }
        return computeShard(extractSampleId(entityId), EntityType.OTHER);
    }

    public Optional<ShardId> routeByName(String sampleId, String name) {
        String canon = Entity.canonicalize(name);
        Optional<String> entityId = nameResolver.resolveEntityId(sampleId, canon);
        return entityId.map(this::routeByEntityId);
    }

    public Optional<String> resolveEntityId(String sampleId, String name) {
        if (name == null || name.isBlank()) return Optional.empty();
        String canon = Entity.canonicalize(name);
        return nameResolver.resolveEntityId(sampleId, canon);
    }

    public Set<ShardId> routeBroadcast(String entityId) {
        Map<ShardId, EntityBloomFilter> bfs = bloomFilters;
        Set<ShardId> candidates = new HashSet<>();
        for (Map.Entry<ShardId, EntityBloomFilter> e : bfs.entrySet()) {
            if (e.getValue().mightContain(entityId)) {
                candidates.add(e.getKey());
            }
        }
        if (candidates.isEmpty()) {
            if (!initialized) {
                for (int i = 0; i < shardCount; i++) candidates.add(new ShardId(i));
            }
        }
        return candidates;
    }

    public synchronized void refreshFromMetastore() {
        List<EntityCentroid> all;
        try {
            all = centroidStore.scanAll();
        } catch (RuntimeException e) {
            if (fallbackOnError) {
                log.warn(
                        "GraphCentroidRouter refresh failed (centroidStore.scanAll error), "
                                + "retaining {} cached centroids (fallback-on-error=true): {}",
                        centroidMap.size(),
                        e.getMessage());
                return;
            }
            throw e;
        }
        if (all.isEmpty() && initialized && fallbackOnError) {
            log.warn(
                    "GraphCentroidRouter refresh: scanAll returned empty while {} centroids cached "
                            + "(possible Metastore degradation), retaining cache (fallback-on-error=true)",
                    centroidMap.size());
            return;
        }
        Map<String, EntityCentroid> rebuiltCentroids = new ConcurrentHashMap<>();
        Map<ShardId, EntityBloomFilter> rebuilt = new ConcurrentHashMap<>();
        for (int i = 0; i < shardCount; i++) {
            rebuilt.put(new ShardId(i), new EntityBloomFilter(1_000_000L, 0.001));
        }
        for (EntityCentroid c : all) {
            if (c.entityId() == null) continue;
            rebuiltCentroids.put(c.entityId(), c);
            ShardId sid =
                    c.shardId() != null ? c.shardId() : computeShard(c.sampleId(), c.entityType());
            EntityBloomFilter bf = rebuilt.get(sid);
            if (bf != null) bf.put(c.entityId());
        }
        this.centroidMap = rebuiltCentroids;
        this.bloomFilters = rebuilt;
        lastRefreshMs.set(System.currentTimeMillis());
        initialized = true;
        log.info(
                "GraphCentroidRouter refreshed: centroids={}, shardCount={}, source=metastore, fallbackOnError={}",
                all.size(),
                shardCount,
                fallbackOnError);
    }

    public long lastRefreshMs() {
        return lastRefreshMs.get();
    }

    public int cachedCentroidCount() {
        return centroidMap.size();
    }

    public boolean isFallbackOnError() {
        return fallbackOnError;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public Map<ShardId, EntityBloomFilter> bloomFilters() {
        return Map.copyOf(bloomFilters);
    }

    public int shardCount() {
        return shardCount;
    }

    public ShardId computeShard(String sampleId, EntityType entityType) {
        int sampleHash = sampleId == null ? 0 : sampleId.hashCode();
        int typeHash = entityType == null ? 0 : entityType.hashCode();
        int hash = (sampleHash * 31) + typeHash;
        int idx = Math.floorMod(hash, shardCount);
        return new ShardId(idx);
    }

    private static String extractSampleId(String entityId) {
        if (entityId == null) return "";
        int colon = entityId.indexOf(':');
        if (colon <= 0) return entityId;
        return entityId.substring(0, colon);
    }
}
