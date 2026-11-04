package io.github.rawwise.backend.spark.routing;

import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public final class SparkRoutingBridge {
    private static final AtomicReference<RoutingCoordinator> DRIVER_COORDINATOR =
            new AtomicReference<>();

    private SparkRoutingBridge() {}

    public static void install(RoutingCoordinator coordinator) {
        DRIVER_COORDINATOR.set(Objects.requireNonNull(coordinator, "coordinator must not be null"));
    }

    public static void clear() {
        DRIVER_COORDINATOR.set(null);
    }

    public static RoutingCoordinator requireCoordinator() {
        RoutingCoordinator coordinator = DRIVER_COORDINATOR.get();
        if (coordinator == null) {
            throw new IllegalStateException(
                    "Spark routing coordinator is not installed on the driver");
        }
        return coordinator;
    }

    public static ShardLookupResult lookupOwner(int shardId) {
        return requireCoordinator().lookupOwner(new ShardId(shardId));
    }

    public static Map<ShardId, ShardLookupResult> lookupOwners(Set<Integer> shardIds) {
        Objects.requireNonNull(shardIds, "shardIds must not be null");
        Set<ShardId> resolvedShardIds =
                shardIds.stream()
                        .map(ShardId::new)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        return requireCoordinator().lookupOwners(resolvedShardIds);
    }
}
