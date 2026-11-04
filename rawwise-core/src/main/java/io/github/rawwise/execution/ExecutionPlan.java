package io.github.rawwise.execution;

import java.util.List;
import java.util.Map;

public record ExecutionPlan(
        String planId,
        String agentId,
        String userId,
        String sessionId,
        String description,
        PlanStatus status,
        List<ExecutionStep> steps,
        Map<String, String> metadata,
        RetryPolicy defaultRetryPolicy,
        long createdAt,
        long updatedAt,
        long finishedAt) {

    public ExecutionPlan {
        if (planId == null || planId.isEmpty()) {
            throw new IllegalArgumentException("planId must not be null or empty");
        }
        if (agentId == null || agentId.isEmpty()) {
            throw new IllegalArgumentException("agentId must not be null or empty");
        }
        if (userId == null || userId.isEmpty()) {
            throw new IllegalArgumentException("userId must not be null or empty");
        }
        if (status == null) {
            status = PlanStatus.PENDING;
        }
        if (steps == null) {
            steps = List.of();
        } else {
            steps = List.copyOf(steps);
        }
        if (metadata == null) {
            metadata = Map.of();
        } else {
            metadata = Map.copyOf(metadata);
        }
    }

    @Override
    public List<ExecutionStep> steps() {
        return steps;
    }

    @Override
    public Map<String, String> metadata() {
        return metadata;
    }

    public enum PlanStatus {
        PENDING,
        RUNNING,
        SUCCEEDED,
        FAILED,
        CANCELLED,
        COMPENSATING,
        COMPENSATED
    }
}
