package io.github.rawwise.axiom.orchestrator;

import java.util.Map;

public record CompensationAction(
        CompensationType type, Map<String, Object> params, Map<String, String> inputMapping) {
    public CompensationAction {
        params = params == null ? Map.of() : Map.copyOf(params);
        inputMapping = inputMapping == null ? Map.of() : Map.copyOf(inputMapping);
    }
}
