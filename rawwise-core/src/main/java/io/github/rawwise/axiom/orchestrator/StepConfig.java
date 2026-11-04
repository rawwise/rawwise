package io.github.rawwise.axiom.orchestrator;

import java.util.Map;

public sealed interface StepConfig
        permits StepConfig.ToolCallConfig,
                StepConfig.SubWorkflowConfig,
                StepConfig.HumanApprovalConfig {
    record ToolCallConfig(String toolName, Map<String, Object> params) implements StepConfig {
        public ToolCallConfig {
            params = params == null ? Map.of() : Map.copyOf(params);
        }
    }

    record SubWorkflowConfig(String workflowId, Map<String, String> inputMapping)
            implements StepConfig {
        public SubWorkflowConfig {
            inputMapping = inputMapping == null ? Map.of() : Map.copyOf(inputMapping);
        }
    }

    record HumanApprovalConfig(String approverAgentId, String message) implements StepConfig {}
}
