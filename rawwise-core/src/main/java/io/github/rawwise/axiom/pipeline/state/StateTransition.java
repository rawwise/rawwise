package io.github.rawwise.axiom.pipeline.state;

import java.util.Map;

public record StateTransition(
        String fromState, String toState, String guardExpression, Map<String, String> metadata) {
    public StateTransition {
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
