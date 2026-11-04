package io.github.rawwise.axiom.memory.semantic;

import java.util.Map;

public record GraphEdge(
        String edgeId,
        String graphId,
        String srcNodeId,
        String label,
        String dstNodeId,
        Map<String, String> properties,
        float weight,
        long version,
        long createdAt,
        long updatedAt) {
    public GraphEdge {
        properties = Map.copyOf(properties);
    }
}
