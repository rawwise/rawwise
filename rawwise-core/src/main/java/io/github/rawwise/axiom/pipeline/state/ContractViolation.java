package io.github.rawwise.axiom.pipeline.state;

public record ContractViolation(
        String spaceId,
        String entryKey,
        String fromState,
        String toState,
        String guardExpression,
        String reason) {}
