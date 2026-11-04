package io.github.rawwise.axiom.pipeline.cognition;

import java.util.List;
import java.util.Map;

/**
 * conditioncontext.
 *
 * <p>,. generation,generation,text.
 *
 * @param original Condition conditiontext(prompt)
 * @param retrieved Facts retrieval
 * @param entity Summary digest(→property)
 * @param violation Metadata information(→)
 * @param evaluation Scores (→)
 * @param previous Attempts (conditiontext→)
 */
public record ConditionReasoningContext(
        String originalCondition,
        List<String> retrievedFacts,
        Map<String, String> entitySummary,
        Map<String, Double> violationMetadata,
        Map<String, Double> evaluationScores,
        List<PreviousAttempt> previousAttempts) {
    public ConditionReasoningContext {
        if (originalCondition == null) originalCondition = "";
        if (retrievedFacts == null) retrievedFacts = List.of();
        if (entitySummary == null) entitySummary = Map.of();
        if (violationMetadata == null) violationMetadata = Map.of();
        if (evaluationScores == null) evaluationScores = Map.of();
        if (previousAttempts == null) previousAttempts = List.of();
    }

    /**
     * information.
     *
     * @param condition conditiontext
     * @param scores
     * @param outcome description("improved", "degraded", "neutral")
     */
    public record PreviousAttempt(String condition, Map<String, Double> scores, String outcome) {
        public PreviousAttempt {
            if (condition == null) condition = "";
            if (scores == null) scores = Map.of();
            if (outcome == null) outcome = "";
        }
    }
}
