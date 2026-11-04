package io.github.rawwise.axiom.memory.semantic;

import java.util.List;

public record PathResult(
        String graphId,
        String srcNodeId,
        String dstNodeId,
        List<String> nodeIds,
        List<String> edgeIds,
        List<String> edgeLabels,
        float totalWeight,
        int hopCount) {
    public PathResult {
        nodeIds = List.copyOf(nodeIds);
        edgeIds = List.copyOf(edgeIds);
        edgeLabels = List.copyOf(edgeLabels);
    }
}
