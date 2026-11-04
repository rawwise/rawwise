package io.github.rawwise.execution;

import java.util.Map;

public record StepResult(
        boolean success, String output, String error, Map<String, String> metrics) {

    public StepResult {
        if (metrics == null) {
            metrics = Map.of();
        } else {
            metrics = Map.copyOf(metrics);
        }
        if (output == null) {
            output = "";
        }
        if (error == null) {
            error = "";
        }
    }

    @Override
    public Map<String, String> metrics() {
        return metrics;
    }

    public static StepResult ofSuccess(String output) {
        return new StepResult(true, output, "", Map.of());
    }

    public static StepResult ofSuccess(String output, Map<String, String> metrics) {
        return new StepResult(true, output, "", metrics);
    }

    public static StepResult ofFailure(String error) {
        return new StepResult(false, "", error, Map.of());
    }

    public static StepResult ofFailure(String error, Map<String, String> metrics) {
        return new StepResult(false, "", error, metrics);
    }
}
