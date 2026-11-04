package io.github.rawwise.axiom.orchestrator;

public final class CoordExpiredException extends CoordException {
    public CoordExpiredException(String resourceId, String detail) {
        super(resourceId, "AXIOM_COORD_EXPIRED", 410, "Coord resource expired: " + detail);
    }
}
