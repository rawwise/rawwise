package io.github.rawwise.axiom.orchestrator;

import java.util.List;

public interface AgentOrchestrator
        extends WorkflowDefinitionManager, WorkflowExecutionEngine, WorkflowStepCallback {

    List<WorkflowStepState> getStepStates(String instanceId);

    void triggerStepRetry(String instanceId, String stepId);

    boolean timeoutWorkflow(String instanceId);

    void markStepReady(String instanceId, String stepId);

    void markStepSkipped(String instanceId, String stepId);
}
