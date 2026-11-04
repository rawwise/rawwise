package io.github.rawwise.backend.routing;

public interface ShardResolver {
    ShardId resolve(byte[] primaryKey);

    int shardCount();

    default int keyToShard(byte[] primaryKey) {
        return resolve(primaryKey).value();
    }
}
