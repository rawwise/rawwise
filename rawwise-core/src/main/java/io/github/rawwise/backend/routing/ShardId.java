package io.github.rawwise.backend.routing;

import java.io.Serializable;

public record ShardId(int value) implements Serializable {
    public ShardId {
        if (value < 0) {
            throw new IllegalArgumentException("shardId must be non-negative");
        }
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
