package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.axiom.pipeline.state.ContractViolation;
import java.util.Map;

public record TextConditionEnhancementContext(
        String entityId,
        ContractViolation violation,
        ConditionConstructionSpec spec,
        Map<String, Object> conditionParams) {
    public TextConditionEnhancementContext {
        conditionParams = conditionParams == null ? Map.of() : Map.copyOf(conditionParams);
    }
}
