package io.github.rawwise.axiom.memory.semantic;

public final class GraphCasConflictException extends GraphException {
    public GraphCasConflictException(String graphId, String detail) {
        super(graphId, "AXIOM_GRAPH_CAS_CONFLICT", 409, "CAS conflict: " + detail);
    }

    public GraphCasConflictException(String graphId, String detail, Throwable cause) {
        super(graphId, "AXIOM_GRAPH_CAS_CONFLICT", 409, "CAS conflict: " + detail, cause);
    }
}
