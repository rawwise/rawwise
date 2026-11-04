package io.github.rawwise.backend.spark.routing;

import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardResolver;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;

public record SparkMurmur3ShardResolver(int shardCount) implements ShardResolver {
    // Spark's default hash seed used by Murmur3Hash / HashPartitioning.
    private static final int SPARK_HASH_SEED = 42;

    public SparkMurmur3ShardResolver {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be positive");
        }
    }

    @Override
    public ShardId resolve(byte[] primaryKey) {
        if (primaryKey == null) {
            throw new IllegalArgumentException("primaryKey must not be null");
        }
        int hash =
                Murmur3_x86_32.hashUnsafeBytes(
                        primaryKey, Platform.BYTE_ARRAY_OFFSET, primaryKey.length, SPARK_HASH_SEED);
        return new ShardId(Math.floorMod(hash, shardCount));
    }
}
