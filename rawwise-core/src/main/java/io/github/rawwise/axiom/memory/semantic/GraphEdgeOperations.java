package io.github.rawwise.axiom.memory.semantic;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface GraphEdgeOperations {
    GraphEdge addEdge(
            String graphId,
            String srcNodeId,
            String label,
            String dstNodeId,
            Map<String, String> properties,
            float weight)
            throws GraphException;

    Optional<GraphEdge> getEdge(String graphId, String edgeId) throws GraphException;

    GraphEdge updateEdge(String graphId, GraphEdge edge) throws GraphException;

    boolean removeEdge(String graphId, String edgeId) throws GraphException;

    List<GraphEdge> getOutEdges(String graphId, String nodeId, String label) throws GraphException;

    List<GraphEdge> getInEdges(String graphId, String nodeId, String label) throws GraphException;
}
