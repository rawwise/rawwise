package io.github.rawwise.axiom.memory.semantic;

public final class GraphNotFoundException extends GraphException {
    public GraphNotFoundException(String graphId) {
        super(graphId, "AXIOM_GRAPH_NOT_FOUND", 404, "Graph not found: " + graphId);
    }

    public GraphNotFoundException(String graphId, String detail) {
        super(
                graphId,
                "AXIOM_GRAPH_NOT_FOUND",
                404,
                "Graph not found: " + graphId + " - " + detail);
    }

    public GraphNotFoundException(String graphId, String detail, Throwable cause) {
        super(
                graphId,
                "AXIOM_GRAPH_NOT_FOUND",
                404,
                "Graph not found: " + graphId + " - " + detail,
                cause);
    }
}
