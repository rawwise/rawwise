package io.github.rawwise.backend.replication;

import io.github.rawwise.engine.vector.ShardCentroids;
import java.util.Map;
import java.util.Optional;

/**
 * Centroid persistence sink (BE Server side).
 *
 * <p>Architecture: BE Server receives centroid reports from BE Node -> CentroidSink persists ->
 * IvfRouter reads.
 *
 * <p>Implementation:
 *
 * <ul>
 *   <li>{@link InMemoryCentroidSink} - in-process cache, for development and testing
 *   <li>{@code MetastoreCentroidSink} - production implementation, writes to Metastore (via
 *       ShardCentroids.metastoreKey)
 * </ul>
 */
public interface CentroidSink {
    /**
     * Save shard centroid data.
     *
     * @param centroids centroid data
     */
    void save(ShardCentroids centroids);

    /**
     * Query shard centroid data.
     *
     * @param shardId shard identifier
     * @return centroid data, or empty (not reported)
     */
    Optional<ShardCentroids> lookup(String shardId);

    /**
     * Returns all reported centroids.
     *
     * @return shardId -> ShardCentroids mapping
     */
    Map<String, ShardCentroids> listAll();
}
