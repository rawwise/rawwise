package io.github.rawwise.backend.replication;

import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingEpoch;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import lombok.extern.slf4j.Slf4j;

/**
 * Coordinates failover when a primary node becomes unavailable.
 *
 * <p>When the primary for a shard fails, {@link #promoteReplica} promotes the replica to primary by
 * creating a new {@link ShardAssignment} with an advanced epoch and no replica (the rebalancer will
 * assign a new replica later).
 */
@Slf4j
public class FailoverCoordinator {

    private final RoutingCoordinator routingCoordinator;

    public FailoverCoordinator(RoutingCoordinator routingCoordinator) {
        this.routingCoordinator = routingCoordinator;
    }

    /**
     * Promote the replica for a shard to become the new primary.
     *
     * <p>This reads the current primary and replica assignments, creates a new assignment with the
     * replica as primary (no replica), and updates the routing table via {@link
     * RoutingCoordinator#assignShard}.
     *
     * @param shardId shard identifier (int)
     * @return promote result indicating success/failure
     */
    public PromoteResult promoteReplica(int shardId) {
        ShardId sid = new ShardId(shardId);

        ShardLookupResult ownerResult = routingCoordinator.lookupOwner(sid);
        if (ownerResult == null || !ownerResult.hasAssignment()) {
            String reason = "No primary assignment found for shard " + shardId;
            log.warn("promoteReplica failed: {}", reason);
            return PromoteResult.failed(reason);
        }

        ShardLookupResult replicaResult = routingCoordinator.lookupReplica(sid);
        if (replicaResult == null || !replicaResult.hasAssignment()) {
            String reason = "No replica found for shard " + shardId;
            log.warn("promoteReplica failed: {}", reason);
            return PromoteResult.failed(reason);
        }

        ShardAssignment replicaAssignment = replicaResult.assignment();
        String newPrimaryExecutorId = replicaAssignment.executorId();
        NodeSlot newPrimarySlot = replicaAssignment.nodeSlot();
        RoutingEpoch currentEpoch = ownerResult.assignment().epoch();
        RoutingEpoch newEpoch = currentEpoch.next();

        ShardAssignment newAssignment =
                new ShardAssignment(sid, newPrimarySlot, newPrimaryExecutorId, newEpoch, true);
        routingCoordinator.assignShard(newAssignment);

        log.info(
                "Failover completed: shard={} newPrimary={} newEpoch={}",
                shardId,
                newPrimaryExecutorId,
                newEpoch.value());
        return PromoteResult.success(newPrimaryExecutorId, newEpoch.value());
    }

    /**
     * Result of a replica promotion attempt.
     *
     * @param success whether the promotion succeeded
     * @param failReason failure reason (empty if success)
     * @param newPrimaryExecutorId the new primary executor ID (null if failed)
     * @param newEpoch the new routing epoch (0 if failed)
     */
    public record PromoteResult(
            boolean success, String failReason, String newPrimaryExecutorId, long newEpoch) {
        public static PromoteResult success(String newPrimaryExecutorId, long newEpoch) {
            return new PromoteResult(true, "", newPrimaryExecutorId, newEpoch);
        }

        public static PromoteResult failed(String reason) {
            return new PromoteResult(false, reason, null, 0L);
        }
    }
}
