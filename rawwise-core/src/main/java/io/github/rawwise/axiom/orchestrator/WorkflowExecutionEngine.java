package io.github.rawwise.axiom.orchestrator;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public interface WorkflowExecutionEngine {
    WorkflowInstance startWorkflow(
            String workflowId, String agentId, Map<String, Object> input, Duration timeout);

    WorkflowInstance getWorkflowInstance(String instanceId);

    List<WorkflowInstance> listInstances(String agentId, int offset, int limit);

    boolean cancelWorkflow(String instanceId, String reason);

    boolean compensateWorkflow(String instanceId, String reason);

    boolean retryCompensation(String instanceId);

    WorkflowInstance resumeWorkflow(
            String instanceId, String fromStepId, Map<String, Object> resumeInput);
}
