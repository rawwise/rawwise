package io.github.rawwise.engine.storage.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.time.Duration;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RowCache {

    @Getter private final Cache<CacheKey, CachedValue> cache;
    private final boolean enabled;

    public RowCache(long maxSize, long ttlMinutes, boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            this.cache =
                    Caffeine.newBuilder()
                            .maximumSize(maxSize)
                            .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                            .recordStats()
                            .removalListener(
                                    (CacheKey key, CachedValue value, RemovalCause cause) -> {
                                        if (cause.wasEvicted()) {
                                            log.debug(
                                                    "RowCache evicted: tableId={}, cause={}",
                                                    key != null ? key.tableId() : "null",
                                                    cause);
                                        }
                                    })
                            .build();
            log.info(
                    "RowCache initialized: maxSize={}, ttlMinutes={}, enabled=true",
                    maxSize,
                    ttlMinutes);
        } else {
            this.cache = Caffeine.newBuilder().maximumSize(0).build();
            log.info("RowCache initialized: enabled=false");
        }
    }

    public Optional<byte[]> get(String tableId, byte[] key, long snapshotTs) {
        if (!enabled) return Optional.empty();
        CacheKey cacheKey = CacheKey.of(tableId, key);
        CachedValue cached = cache.getIfPresent(cacheKey);
        if (cached == null) return Optional.empty();
        if (!cached.isVisibleTo(snapshotTs)) return Optional.empty();
        return Optional.of(cached.getData());
    }

    public void put(String tableId, byte[] key, byte[] data, long commitTs) {
        if (!enabled) return;
        CacheKey cacheKey = CacheKey.of(tableId, key);
        cache.put(cacheKey, new CachedValue(data, commitTs));
    }

    public void invalidate(String tableId, byte[] key) {
        if (!enabled) return;
        CacheKey cacheKey = CacheKey.of(tableId, key);
        cache.invalidate(cacheKey);
    }

    public void invalidateTable(String tableId) {
        if (!enabled) return;
        cache.asMap().keySet().removeIf(k -> k.tableId().equals(tableId));
        log.info("RowCache invalidated table: tableId={}", tableId);
    }

    public CacheStats stats() {
        com.github.benmanes.caffeine.cache.stats.CacheStats caffeineStats = cache.stats();
        return new CacheStats(
                caffeineStats.hitCount(),
                caffeineStats.missCount(),
                caffeineStats.evictionCount(),
                caffeineStats.hitRate());
    }

    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
        log.info("RowCache closed: final stats={}", stats());
    }

    public boolean isEnabled() {
        return enabled;
    }

    public record CacheStats(long hitCount, long missCount, long evictionCount, double hitRate) {}
}
