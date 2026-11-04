package io.github.rawwise.backend.routing;

import java.io.Serializable;

public record NodeSlot(String nodeId, int slotIndex) implements Comparable<NodeSlot>, Serializable {
    public NodeSlot {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        if (slotIndex < 0) {
            throw new IllegalArgumentException("slotIndex must be non-negative");
        }
    }

    @Override
    public int compareTo(NodeSlot other) {
        int byNode = nodeId.compareTo(other.nodeId);
        if (byNode != 0) {
            return byNode;
        }
        return Integer.compare(slotIndex, other.slotIndex);
    }

    @Override
    public String toString() {
        return nodeId + ":" + slotIndex;
    }
}
