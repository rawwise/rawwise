package io.github.rawwise.axiom.orchestrator;

public final class CoordConflictException extends CoordException {
    public CoordConflictException(String resourceId, String detail) {
        super(resourceId, "AXIOM_COORD_CONFLICT", 409, "Coord conflict: " + detail);
    }

    public CoordConflictException(String resourceId, String detail, Throwable cause) {
        super(resourceId, "AXIOM_COORD_CONFLICT", 409, "Coord conflict: " + detail, cause);
    }
}
