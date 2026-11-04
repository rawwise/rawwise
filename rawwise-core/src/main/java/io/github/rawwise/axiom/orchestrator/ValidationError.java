package io.github.rawwise.axiom.orchestrator;

public record ValidationError(ValidationErrorType type, String stepId, String message) {}
