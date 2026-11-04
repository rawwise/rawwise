package io.github.rawwise.frontend.ir.routing;

import java.util.List;

public record ShardBatch(int shardId, String executorId, List<byte[]> keys, long epoch) {
    public ShardBatch {
        keys = List.copyOf(keys);
    }
}
