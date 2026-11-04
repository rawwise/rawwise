package io.github.rawwise.axiom.orchestrator;

public record WorkflowEdgeDef(String fromStepId, String toStepId, EdgeCondition condition) {}
