package io.github.rawwise.axiom.orchestrator;

import java.util.List;

public interface WorkflowDefinitionManager {
    WorkflowValidationResult defineWorkflow(
            String name, String agentId, List<WorkflowStepDef> steps, List<WorkflowEdgeDef> edges);

    WorkflowDefinition getWorkflow(String workflowId);

    List<WorkflowDefinition> listWorkflows(String agentId, int offset, int limit);

    boolean deleteWorkflow(String workflowId);
}
