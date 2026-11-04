package io.github.rawwise.axiom.memory.semantic;

public final class GraphIntegrityException extends GraphException {
    public GraphIntegrityException(String graphId, String detail) {
        super(graphId, "AXIOM_GRAPH_INTEGRITY_VIOLATION", 422, "Integrity violation: " + detail);
    }

    public GraphIntegrityException(String graphId, String detail, Throwable cause) {
        super(
                graphId,
                "AXIOM_GRAPH_INTEGRITY_VIOLATION",
                422,
                "Integrity violation: " + detail,
                cause);
    }
}
