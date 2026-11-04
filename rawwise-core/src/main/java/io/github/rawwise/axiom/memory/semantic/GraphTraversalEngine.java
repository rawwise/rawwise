package io.github.rawwise.axiom.memory.semantic;

import java.util.List;

public interface GraphTraversalEngine {
    List<GraphNode> neighbors(
            String graphId,
            String nodeId,
            String label,
            int maxHops,
            int limit,
            TraversalDirection direction)
            throws GraphException;

    List<PathResult> findPaths(String graphId, String srcNodeId, String dstNodeId, int maxHops)
            throws GraphException;
}
