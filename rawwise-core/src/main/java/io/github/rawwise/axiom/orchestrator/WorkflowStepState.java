package io.github.rawwise.axiom.orchestrator;

import java.util.Map;

public record WorkflowStepState(
        String stepId,
        StepStatus status,
        Map<String, Object> input,
        Map<String, Object> output,
        String error,
        int retryCount,
        Long nextRetryAt,
        long startedAt,
        long completedAt) {
    public WorkflowStepState {
        input = input == null ? Map.of() : Map.copyOf(input);
        output = output == null ? Map.of() : Map.copyOf(output);
    }
}
