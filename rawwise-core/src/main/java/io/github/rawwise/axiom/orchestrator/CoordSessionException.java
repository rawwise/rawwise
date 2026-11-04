package io.github.rawwise.axiom.orchestrator;

public final class CoordSessionException extends CoordException {
    public CoordSessionException(String resourceId, String detail) {
        super(resourceId, "AXIOM_COORD_SESSION_ERROR", 403, "Session error: " + detail);
    }
}
