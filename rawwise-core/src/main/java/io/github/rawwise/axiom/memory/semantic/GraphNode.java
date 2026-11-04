package io.github.rawwise.axiom.memory.semantic;

import java.util.Map;

public record GraphNode(
        String nodeId,
        String graphId,
        String type,
        Map<String, String> properties,
        int importance,
        float decayFactor,
        String embeddingRef,
        String embeddingModel,
        String embeddingText,
        long version,
        long createdAt,
        long updatedAt) {
    public GraphNode {
        properties = Map.copyOf(properties);
    }
}
