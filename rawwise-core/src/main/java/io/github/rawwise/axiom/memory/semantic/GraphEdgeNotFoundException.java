package io.github.rawwise.axiom.memory.semantic;

public final class GraphEdgeNotFoundException extends GraphException {
    public GraphEdgeNotFoundException(String graphId, String edgeId) {
        super(graphId, "AXIOM_GRAPH_EDGE_NOT_FOUND", 404, "Edge not found: " + edgeId);
    }

    public GraphEdgeNotFoundException(String graphId, String edgeId, Throwable cause) {
        super(graphId, "AXIOM_GRAPH_EDGE_NOT_FOUND", 404, "Edge not found: " + edgeId, cause);
    }
}
