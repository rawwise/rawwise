package io.github.rawwise.model.record;

import java.util.List;
import java.util.Map;

/**
 * learned strategy model,belongs to M_t(active model set). does not belong to Semantic Record
 * five-layer semantic stack,persisted independently in Knowledge Store.
 *
 * <p>invariant: once Strategy Action is created,only updated by appending Effect Record, never
 * physically deleted.low confidence strategies only filtered from retrieval results,not removed
 * from storage.
 */
public record StrategyAction(
        String strategyId,
        String sceneCondition,
        Map<String, Double> conditionMetrics,
        String strategyContent,
        String expectedEffect,
        List<EffectRecord> historicalEffects,
        double confidence,
        List<String> sourceViewIds,
        List<String> sourceFeedbackIds,
        long createdAt,
        long updatedAt) {
    public record EffectRecord(
            String attemptId,
            String viewVersion,
            Map<String, Double> beforeMetrics,
            Map<String, Double> afterMetrics,
            double delta,
            double effectivenessMembership,
            String feedbackSource) {
        /**
         * Backward-compatible boolean effectiveness.
         *
         * @return true if effectiveness Membership >= 0.5
         */
        public boolean effective() {
            return effectivenessMembership >= 0.5;
        }
    }

    /**
     * compute weighted confidence from historical effects. weight: HUMAN=1.0, ENV=0.7,
     * SELF=0.5(conflictpriority human > env > self) needs at least 2 attempts to return non-zero
     * value.
     *
     * <p>note: confidence can decrease(when adding invalid attempts), N3-- N3 guarantees that
     * Strategy Action records are not deleted(|K_t| monotonically non-decreasing),not that
     * confidence is monotonic.
     */
    public static double computeConfidence(List<EffectRecord> effects) {
        if (effects == null || effects.size() < 2) return 0.0;
        double weightedEffective = 0;
        double weightedTotal = 0;
        for (EffectRecord e : effects) {
            double w =
                    switch (e.feedbackSource()) {
                        case "HUMAN" -> 1.0;
                        case "ENV" -> 0.7;
                        default -> 0.5;
                    };
            weightedTotal += w;
            weightedEffective += w * e.effectivenessMembership();
        }
        return weightedTotal == 0 ? 0.0 : weightedEffective / weightedTotal;
    }
}
