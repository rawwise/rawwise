package io.github.rawwise.axiom.orchestrator;

import java.util.List;
import java.util.Map;

public record WorkflowInstance(
        String instanceId,
        String workflowId,
        int workflowVersion,
        String agentId,
        Map<String, Object> input,
        Map<String, Object> output,
        WorkflowInstanceStatus status,
        String error,
        String cancelReason,
        String compensateReason,
        List<String> stepIds,
        List<String> coordinationResourceIds,
        long timeoutMs,
        long startedAt,
        long completedAt) {
    public WorkflowInstance {
        input = input == null ? Map.of() : Map.copyOf(input);
        output = output == null ? Map.of() : Map.copyOf(output);
        stepIds = List.copyOf(stepIds);
        coordinationResourceIds =
                coordinationResourceIds == null ? List.of() : List.copyOf(coordinationResourceIds);
    }
}
