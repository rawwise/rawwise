package io.github.rawwise.axiom.orchestrator;

import java.util.List;
import java.util.Map;

public record WorkflowDefinition(
        String workflowId,
        String name,
        String agentId,
        List<WorkflowStepDef> steps,
        List<WorkflowEdgeDef> edges,
        WorkflowStatus status,
        int version,
        Map<String, String> extensions,
        long createdAt,
        long updatedAt) {
    public WorkflowDefinition {
        steps = List.copyOf(steps);
        edges = List.copyOf(edges);
        extensions = extensions == null ? Map.of() : Map.copyOf(extensions);
    }
}
