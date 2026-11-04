package io.github.rawwise.axiom.orchestrator;

public final class CoordAcquireTimeoutException extends CoordException {
    public CoordAcquireTimeoutException(String resourceId) {
        super(
                resourceId,
                "AXIOM_COORD_ACQUIRE_TIMEOUT",
                423,
                "Acquire timeout for resource: " + resourceId);
    }

    public CoordAcquireTimeoutException(String resourceId, String detail) {
        super(
                resourceId,
                "AXIOM_COORD_ACQUIRE_TIMEOUT",
                423,
                "Acquire timeout for resource: " + resourceId + " - " + detail);
    }
}
