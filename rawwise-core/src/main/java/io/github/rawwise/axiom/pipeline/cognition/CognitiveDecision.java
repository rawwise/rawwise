package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.StrategyAction;
import java.util.UUID;

/** cognition - Cognition Driver. */
public record CognitiveDecision(
        String decisionId,
        DecisionType type,
        StrategyAction strategy,
        String rationale,
        double expectedImprovement,
        double uncertainty,
        String sceneHash,
        long createdAt,
        boolean consumed) {
    public CognitiveDecision {
        if (decisionId == null || decisionId.isBlank()) {
            decisionId = "decision:" + UUID.randomUUID();
        }
    }

    public static CognitiveDecision explore(
            StrategyAction strategy,
            String rationale,
            double expectedImprovement,
            double uncertainty,
            String sceneHash) {
        return new CognitiveDecision(
                "decision:" + UUID.randomUUID(),
                DecisionType.EXPLORE,
                strategy,
                rationale,
                expectedImprovement,
                uncertainty,
                sceneHash,
                System.currentTimeMillis(),
                false);
    }

    public static CognitiveDecision exploit(
            StrategyAction strategy,
            String rationale,
            double expectedImprovement,
            double uncertainty,
            String sceneHash) {
        return new CognitiveDecision(
                "decision:" + UUID.randomUUID(),
                DecisionType.EXPLOIT,
                strategy,
                rationale,
                expectedImprovement,
                uncertainty,
                sceneHash,
                System.currentTimeMillis(),
                false);
    }

    public static CognitiveDecision terminate(String rationale, String sceneHash) {
        return new CognitiveDecision(
                "decision:" + UUID.randomUUID(),
                DecisionType.TERMINATE,
                null,
                rationale,
                0.0,
                0.0,
                sceneHash,
                System.currentTimeMillis(),
                false);
    }

    public CognitiveDecision markConsumed() {
        return new CognitiveDecision(
                decisionId,
                type,
                strategy,
                rationale,
                expectedImprovement,
                uncertainty,
                sceneHash,
                createdAt,
                true);
    }
}
