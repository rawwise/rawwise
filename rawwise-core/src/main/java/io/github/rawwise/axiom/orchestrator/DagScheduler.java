package io.github.rawwise.axiom.orchestrator;

import java.util.List;

public interface DagScheduler {
    void evaluateDownstream(String instanceId, String completedStepId);

    List<String> topologicalSort(List<WorkflowStepDef> steps, List<WorkflowEdgeDef> edges);

    List<String> reverseTopologicalSort(List<WorkflowStepDef> steps, List<WorkflowEdgeDef> edges);
}
