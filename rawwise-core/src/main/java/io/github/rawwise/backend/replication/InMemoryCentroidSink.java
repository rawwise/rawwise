package io.github.rawwise.backend.replication;

import io.github.rawwise.engine.vector.ShardCentroids;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process centroid cache (for development and testing).
 *
 * <p>Production environments should use {@code MetastoreCentroidSink} to write to Metastore,
 * IvfRouter reads automatically on 30s refresh.
 */
public class InMemoryCentroidSink implements CentroidSink {
    private final Map<String, ShardCentroids> cache = new ConcurrentHashMap<>();

    @Override
    public void save(ShardCentroids centroids) {
        cache.put(centroids.shardId(), centroids);
    }

    @Override
    public Optional<ShardCentroids> lookup(String shardId) {
        return Optional.ofNullable(cache.get(shardId));
    }

    @Override
    public Map<String, ShardCentroids> listAll() {
        return Map.copyOf(cache);
    }
}
