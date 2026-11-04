package io.github.rawwise.backend.routing;

import java.io.Serializable;

public record RoutingEpoch(long value) implements Comparable<RoutingEpoch>, Serializable {
    public RoutingEpoch {
        if (value < 0) {
            throw new IllegalArgumentException("routingEpoch must be non-negative");
        }
    }

    public static RoutingEpoch initial() {
        return new RoutingEpoch(0);
    }

    public RoutingEpoch next() {
        return new RoutingEpoch(value + 1);
    }

    @Override
    public int compareTo(RoutingEpoch other) {
        return Long.compare(value, other.value);
    }
}
