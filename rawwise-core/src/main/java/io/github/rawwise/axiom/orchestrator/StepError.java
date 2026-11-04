package io.github.rawwise.axiom.orchestrator;

public record StepError(String code, String message, boolean retryable, String category) {}
