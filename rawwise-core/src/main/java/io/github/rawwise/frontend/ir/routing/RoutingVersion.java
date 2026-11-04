package io.github.rawwise.frontend.ir.routing;

import java.io.Serializable;

public record RoutingVersion(long snapshotId, long indexVersion, long routingTableEpoch)
        implements Serializable {
    private static final long serialVersionUID = 1L;

    public boolean isCompatibleWith(RoutingVersion other) {
        if (other == null) return false;
        return this.snapshotId == other.snapshotId
                && this.indexVersion == other.indexVersion
                && this.routingTableEpoch == other.routingTableEpoch;
    }

    public boolean isStaleComparedTo(RoutingVersion newer) {
        if (newer == null) return false;
        if (newer.routingTableEpoch > this.routingTableEpoch) return true;
        if (newer.indexVersion > this.indexVersion) return true;
        return false;
    }
}
