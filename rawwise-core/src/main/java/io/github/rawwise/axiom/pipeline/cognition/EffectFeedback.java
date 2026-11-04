package io.github.rawwise.axiom.pipeline.cognition;

import java.util.Map;

/**
 * - Effect Evaluator Execution Result.
 *
 * <p>():{@code reliable} field {@code boolean} {@code double reliability Membership}(0.0~1.0), S
 * function. {@link #reliable()} method(≥0.5),.
 */
public record EffectFeedback(
        double overallScore,
        Map<String, Double> dimensionScores,
        double noiseEstimate,
        double signalEstimate,
        double reliabilityMembership,
        String assessment) {
    /**
     * Signal-to-noise ratio. Returns 0.0 when noise Estimate is 0 (consistent with reliability
     * Membership=0.0 in that case). Note: {@link #reliability Membership()} is the authoritative
     * reliability metric; snr() is provided for logging/monitoring only.
     */
    public double snr() {
        return noiseEstimate == 0 ? 0.0 : signalEstimate / noiseEstimate;
    }

    /**
     * Backward-compatible boolean reliability.
     *
     * @return true if reliability Membership >= 0.5
     */
    public boolean reliable() {
        return reliabilityMembership >= 0.5;
    }

    /**
     * Factory for feedback below the SNR threshold. Sets reliability Membership to 0.0 (fully
     * unreliable). Use {@link #of(double, Map, double, double, double, String)} for partial
     * reliability.
     */
    public static EffectFeedback unreliable(
            double score, Map<String, Double> dimensions, double noise, double signal) {
        return new EffectFeedback(
                score, dimensions, noise, signal, 0.0, "SNR below threshold, feedback unreliable");
    }

    /**
     * Factory for feedback above the SNR threshold. Sets reliability Membership to 1.0 (fully
     * reliable).
     */
    public static EffectFeedback reliable(
            double score, Map<String, Double> dimensions, double noise, double signal) {
        return new EffectFeedback(
                score, dimensions, noise, signal, 1.0, "SNR above threshold, feedback reliable");
    }

    /**
     * Factory for unavailable evaluator (noop scenario). Distinct from {@link #unreliable}:
     * "evaluator unavailable" vs "feedback unreliable".
     */
    public static EffectFeedback unavailable() {
        return new EffectFeedback(
                0.0,
                Map.of(),
                1.0,
                0.0,
                0.0,
                "EffectEvaluator unavailable, no assessment performed");
    }

    /**
     * Create feedback with explicit reliability membership degree.
     *
     * @param score overall score
     * @param dimensions per-dimension scores
     * @param noise noise estimate
     * @param signal signal estimate
     * @param reliability Membership reliability membership in [0, 1]
     * @param assessment human-readable assessment
     * @return Effect Feedback with fuzzy reliability
     */
    public static EffectFeedback of(
            double score,
            Map<String, Double> dimensions,
            double noise,
            double signal,
            double reliabilityMembership,
            String assessment) {
        return new EffectFeedback(
                score, dimensions, noise, signal, reliabilityMembership, assessment);
    }
}
