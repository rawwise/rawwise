package io.github.rawwise.axiom.pipeline.cognition;

import java.util.List;

/**
 * .
 *
 * @param original Condition condition
 * @param improved Condition condition
 * @param source Fact Ids ID
 * @param reasoning Method method("llm" / "template" / "noop")
 */
public record ConditionReasoningResult(
        String originalCondition,
        String improvedCondition,
        List<String> sourceFactIds,
        String reasoningMethod) {
    public ConditionReasoningResult {
        if (originalCondition == null) originalCondition = "";
        if (improvedCondition == null) improvedCondition = originalCondition;
        if (sourceFactIds == null) sourceFactIds = List.of();
        if (reasoningMethod == null) reasoningMethod = "noop";
    }

    public boolean isImproved() {
        return !improvedCondition.equals(originalCondition);
    }
}
