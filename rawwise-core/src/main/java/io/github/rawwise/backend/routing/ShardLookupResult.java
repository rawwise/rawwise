package io.github.rawwise.backend.routing;

import java.io.Serializable;

public record ShardLookupResult(ShardId shardId, RoutingStatus status, ShardAssignment assignment)
        implements Serializable {
    public ShardLookupResult {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
        if (status == RoutingStatus.ASSIGNED && assignment == null) {
            throw new IllegalArgumentException(
                    "assignment must not be null when status is ASSIGNED");
        }
        if (status == RoutingStatus.PENDING_READY && assignment == null) {
            throw new IllegalArgumentException(
                    "assignment must not be null when status is PENDING_READY");
        }
        if (status == RoutingStatus.RECOVERING && assignment == null) {
            throw new IllegalArgumentException(
                    "assignment must not be null when status is RECOVERING");
        }
        if (status == RoutingStatus.UNASSIGNED && assignment != null) {
            throw new IllegalArgumentException("assignment must be null when status is UNASSIGNED");
        }
        if (status == RoutingStatus.REASSIGNING && assignment != null) {
            throw new IllegalArgumentException(
                    "assignment must be null when status is REASSIGNING");
        }
    }

    public static ShardLookupResult assigned(ShardAssignment assignment) {
        return new ShardLookupResult(assignment.shardId(), RoutingStatus.ASSIGNED, assignment);
    }

    public static ShardLookupResult pendingReady(ShardAssignment assignment) {
        return new ShardLookupResult(assignment.shardId(), RoutingStatus.PENDING_READY, assignment);
    }

    public static ShardLookupResult recovering(ShardAssignment assignment) {
        return new ShardLookupResult(assignment.shardId(), RoutingStatus.RECOVERING, assignment);
    }

    public static ShardLookupResult reassigning(ShardId shardId) {
        return new ShardLookupResult(shardId, RoutingStatus.REASSIGNING, null);
    }

    public static ShardLookupResult unassigned(ShardId shardId) {
        return new ShardLookupResult(shardId, RoutingStatus.UNASSIGNED, null);
    }

    public boolean assigned() {
        return status == RoutingStatus.ASSIGNED;
    }

    public boolean hasAssignment() {
        return assignment != null;
    }
}
