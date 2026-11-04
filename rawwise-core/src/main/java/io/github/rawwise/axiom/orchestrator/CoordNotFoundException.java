package io.github.rawwise.axiom.orchestrator;

public final class CoordNotFoundException extends CoordException {
    public CoordNotFoundException(String resourceId) {
        super(resourceId, "AXIOM_COORD_NOT_FOUND", 404, "Coord resource not found: " + resourceId);
    }
}
