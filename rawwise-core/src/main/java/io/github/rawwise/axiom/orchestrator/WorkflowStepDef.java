package io.github.rawwise.axiom.orchestrator;

import io.github.rawwise.governance.auth.Principal;
import java.util.Map;

public record WorkflowStepDef(
        String stepId,
        String name,
        Principal.AgentPrincipal executor,
        StepType type,
        StepConfig config,
        long timeoutMs,
        RetryPolicy retryPolicy,
        CompensationAction compensation,
        Map<String, String> inputMapping) {
    public WorkflowStepDef {
        retryPolicy = retryPolicy == null ? RetryPolicy.NO_RETRY : retryPolicy;
        inputMapping = inputMapping == null ? Map.of() : Map.copyOf(inputMapping);
    }
}
