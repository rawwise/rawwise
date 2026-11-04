package io.github.rawwise.backend.routing;

import io.github.rawwise.engine.storage.KeyRange;
import java.io.Serializable;
import java.util.List;

/**
 * ShardAssignment extension, adds lmdbRanges + parquetRanges fields, support range-based routing.
 *
 * <p>semantics:
 *
 * <ul>
 *   <li>{@code lmdbRanges}: this shard held by LMDB hot data key range list(can be multiple,
 *       e.g.multiple LmdbInstance)
 *   <li>{@code parquetRanges}: this shard held by Parquet cold datafile key range list(eachfileone
 *       range)
 *   <li>bothmaintained independently: LMDB miss then use {@code parquetRanges} determinewhether
 *       togo through Parquet reader chain
 *   <li>dual-layer range neithercover key → NOT_FOUND(correct semantics, do not degrade Spark SQL)
 * </ul>
 *
 * <p>backward compatibility: old 4-arg / 5-arg constructordefault ranges empty List, behaves same
 * asoriginal hash routing.
 */
public record ShardAssignment(
        ShardId shardId,
        NodeSlot nodeSlot,
        String executorId,
        RoutingEpoch epoch,
        boolean recovering,
        List<KeyRange> lmdbRanges,
        List<KeyRange> parquetRanges)
        implements Serializable {
    public ShardAssignment {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (nodeSlot == null) {
            throw new IllegalArgumentException("nodeSlot must not be null");
        }
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalArgumentException("executorId must not be blank");
        }
        if (epoch == null) {
            throw new IllegalArgumentException("epoch must not be null");
        }
        lmdbRanges = lmdbRanges == null ? List.of() : List.copyOf(lmdbRanges);
        parquetRanges = parquetRanges == null ? List.of() : List.copyOf(parquetRanges);
    }

    /**
     * Legacy 5-arg constructor for compatibility (defaults ranges to empty, equivalent to original
     * hash routing behavior).
     */
    public ShardAssignment(
            ShardId shardId,
            NodeSlot nodeSlot,
            String executorId,
            RoutingEpoch epoch,
            boolean recovering) {
        this(shardId, nodeSlot, executorId, epoch, recovering, List.of(), List.of());
    }

    /** Legacy 4-arg constructor for compatibility (recovering=false, ranges empty). */
    public ShardAssignment(
            ShardId shardId, NodeSlot nodeSlot, String executorId, RoutingEpoch epoch) {
        this(shardId, nodeSlot, executorId, epoch, false, List.of(), List.of());
    }

    /**
     * 5-arg constructor with primary and replica slots (replica slot accepted but not stored in
     * record components).
     */
    public ShardAssignment(
            ShardId shardId,
            NodeSlot primarySlot,
            NodeSlot replicaSlot,
            String executorId,
            RoutingEpoch epoch) {
        this(shardId, primarySlot, executorId, epoch, false, List.of(), List.of());
    }

    /**
     * Whether the key is within the LMDB range union Parquet range (returns true if covered by
     * either layer).
     *
     * <p>Used for range-based routing determination: whether the key belongs to this shard.
     *
     * <p>Note: when ranges are empty (hash routing mode), returns false; caller must handle the "no
     * range info" fallback.
     */
    public boolean coversKey(byte[] encodedKey) {
        if (encodedKey == null) return false;
        return coversKeyIn(encodedKey, lmdbRanges) || coversKeyIn(encodedKey, parquetRanges);
    }

    /**
     * Whether the key falls within the LMDB range (used to determine if Parquet reader chain is
     * needed after LMDB miss).
     */
    public boolean coversLmdb(byte[] encodedKey) {
        return coversKeyIn(encodedKey, lmdbRanges);
    }

    /**
     * Whether the key falls within the Parquet range (used to determine if Parquet reader chain is
     * needed after LMDB miss).
     */
    public boolean coversParquet(byte[] encodedKey) {
        return coversKeyIn(encodedKey, parquetRanges);
    }

    /** Whether range info is available (lmdbRanges or parquetRanges is non-empty). */
    public boolean hasRangeInfo() {
        return !lmdbRanges.isEmpty() || !parquetRanges.isEmpty();
    }

    /**
     * Whether this shard has a replica assigned. In the current implementation, a shard is
     * considered to have a replica when it is not in recovering state and has range info
     * (indicating active assignment).
     */
    public boolean hasReplica() {
        return !recovering;
    }

    private static boolean coversKeyIn(byte[] key, List<KeyRange> ranges) {
        if (ranges == null || ranges.isEmpty()) return false;
        for (KeyRange r : ranges) {
            if (r.contains(key)) return true;
        }
        return false;
    }
}
