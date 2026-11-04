package io.github.rawwise.engine.storage;

/**
 * Context for routing owner lease management. Carries the executor ID and epoch for lease
 * validation.
 *
 * <p>Extracted as a top-level record to avoid reverse dependency from InstancePool on
 * DynamicMmapStore.
 *
 * @param executorId the executor identifier (must not be blank)
 * @param epoch the epoch number (must be non-negative)
 */
public record RoutingOwnerContext(String executorId, long epoch) {
    public RoutingOwnerContext {
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalArgumentException("executorId must not be blank");
        }
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must be non-negative");
        }
    }

    public String leaseOwnerId() {
        return LmdbInstance.routingOwnerId(executorId, epoch);
    }
}
