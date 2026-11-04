package io.github.rawwise.axiom.pipeline.graph.distributed;

public record ShardId(int value) implements Comparable<ShardId> {

    public ShardId {
        if (value < 0) throw new IllegalArgumentException("shardId must be non-negative: " + value);
    }

    @Override
    public int compareTo(ShardId o) {
        return Integer.compare(this.value, o.value);
    }

    @Override
    public String toString() {
        return "shard-" + value;
    }
}
