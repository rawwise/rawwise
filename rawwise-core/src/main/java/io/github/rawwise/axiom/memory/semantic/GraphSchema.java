package io.github.rawwise.axiom.memory.semantic;

import java.util.List;
import java.util.Map;

public record GraphSchema(
        String graphId,
        String agentId,
        String name,
        String description,
        List<String> nodeTypes,
        List<String> edgeLabels,
        Map<String, String> metadata,
        int nodeCount,
        int edgeCount,
        int maxNodes,
        int maxEdges,
        long createdAt,
        long updatedAt) {
    public GraphSchema {
        nodeTypes = List.copyOf(nodeTypes);
        edgeLabels = List.copyOf(edgeLabels);
        metadata = Map.copyOf(metadata);
    }
}
