package io.github.rawwise.axiom.orchestrator;

public record AxiomOrchestratorOptions(
        int maxConcurrentInstances,
        int maxConcurrentInstancesPerAgent,
        int maxWorkflowSteps,
        int maxWorkflowDepth,
        int maxWorkflowFanOut,
        long timeoutCheckIntervalMs) {}
