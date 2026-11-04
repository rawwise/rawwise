package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.Action;

public record ConditionConstructionSpec(
        String baselineGraphId, String conditionType, Action.ActionType actionType) {
    public ConditionConstructionSpec {
        if (baselineGraphId == null || baselineGraphId.isBlank()) {
            throw new IllegalArgumentException("baselineGraphId is required");
        }
        conditionType = conditionType == null ? "hybrid" : conditionType;
        actionType = actionType == null ? Action.ActionType.CORRECT : actionType;
    }

    public static ConditionConstructionSpec defaults(String baselineGraphId) {
        return new ConditionConstructionSpec(baselineGraphId, "hybrid", Action.ActionType.CORRECT);
    }

    public static ConditionConstructionSpec textOnly(String baselineGraphId) {
        return new ConditionConstructionSpec(baselineGraphId, "text", Action.ActionType.CORRECT);
    }

    public static ConditionConstructionSpec embeddingOnly(String baselineGraphId) {
        return new ConditionConstructionSpec(
                baselineGraphId, "embedding", Action.ActionType.CORRECT);
    }
}
