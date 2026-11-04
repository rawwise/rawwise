package io.github.rawwise.axiom.orchestrator;

import java.util.Map;

public interface WorkflowStepCallback {
    WorkflowStepState completeStep(
            String instanceId, String stepId, Map<String, Object> output, String requestToken);

    WorkflowStepState failStep(
            String instanceId, String stepId, StepError error, String requestToken);

    WorkflowStepState compensateStep(
            String instanceId,
            String stepId,
            Map<String, Object> compensateInput,
            String requestToken);
}
