package io.github.rawwise.axiom.pipeline.cognition;

import java.util.List;
import java.util.Objects;

public record TextConditionEnhancement(
        String originalCondition,
        String enhancedCondition,
        List<String> constraints,
        List<String> sourceIds) {
    public TextConditionEnhancement {
        enhancedCondition = enhancedCondition == null ? originalCondition : enhancedCondition;
        constraints = constraints == null ? List.of() : List.copyOf(constraints);
        sourceIds = sourceIds == null ? List.of() : List.copyOf(sourceIds);
    }

    public boolean enhanced() {
        return !Objects.equals(originalCondition, enhancedCondition) || !constraints.isEmpty();
    }

    public static TextConditionEnhancement unchanged(String condition) {
        return new TextConditionEnhancement(condition, condition, List.of(), List.of());
    }
}
