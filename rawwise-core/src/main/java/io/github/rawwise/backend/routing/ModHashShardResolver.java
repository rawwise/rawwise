package io.github.rawwise.backend.routing;

import java.util.Arrays;

public record ModHashShardResolver(int shardCount) implements ShardResolver {
    public ModHashShardResolver {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be positive");
        }
    }

    @Override
    public ShardId resolve(byte[] primaryKey) {
        if (primaryKey == null) {
            throw new IllegalArgumentException("primaryKey must not be null");
        }
        return new ShardId(Math.floorMod(Arrays.hashCode(primaryKey), shardCount));
    }
}
