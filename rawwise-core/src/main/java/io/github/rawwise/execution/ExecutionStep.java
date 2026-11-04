package io.github.rawwise.execution;

import java.util.List;
import java.util.Map;

public record ExecutionStep(
        String stepId,
        String planId,
        int stepIndex,
        String description,
        StepType type,
        StepStatus status,
        String toolName,
        Map<String, String> toolInput,
        StepResult result,
        RetryPolicy retryPolicy,
        CompensationAction compensation,
        String contextRef,
        List<Integer> dependsOn,
        int attemptCount,
        int maxAttempts,
        long startedAt,
        long finishedAt,
        List<StepLog> logs) {

    public ExecutionStep {
        if (stepId == null || stepId.isEmpty()) {
            throw new IllegalArgumentException("stepId must not be null or empty");
        }
        if (planId == null || planId.isEmpty()) {
            throw new IllegalArgumentException("planId must not be null or empty");
        }
        if (stepIndex < 0) {
            throw new IllegalArgumentException("stepIndex must be >= 0, got: " + stepIndex);
        }
        if (type == null) {
            type = StepType.TOOL_CALL;
        }
        if (status == null) {
            status = StepStatus.PENDING;
        }
        if (toolInput == null) {
            toolInput = Map.of();
        } else {
            toolInput = Map.copyOf(toolInput);
        }
        if (dependsOn == null) {
            dependsOn = List.of();
        } else {
            dependsOn = List.copyOf(dependsOn);
        }
        if (logs == null) {
            logs = List.of();
        } else {
            logs = List.copyOf(logs);
        }
    }

    @Override
    public Map<String, String> toolInput() {
        return toolInput;
    }

    @Override
    public List<Integer> dependsOn() {
        return dependsOn;
    }

    @Override
    public List<StepLog> logs() {
        return logs;
    }

    public enum StepType {
        TOOL_CALL,
        DECISION,
        WAIT
    }

    public enum StepStatus {
        PENDING,
        RUNNING,
        SUCCEEDED,
        FAILED,
        RETRYING,
        SKIPPED,
        COMPENSATING,
        COMPENSATED,
        COMPENSATION_FAILED
    }
}
