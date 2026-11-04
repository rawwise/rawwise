package io.github.rawwise.axiom.pipeline.cognition;

/**
 * -. Raw Wise "",estimation.
 *
 * <p>reliability():{@code Effect Feedback.reliable} boolean {@code double reliability Membership},
 * S function μ(snr) = 1/(1+e^(-2*(snr-min Snr Ratio))). SNR = min Snr Ratio μ = 0.5(), SNR ≫ min
 * Snr Ratio μ → 1.0(), SNR ≪ min Snr Ratio μ → 0.0(). {@link Effect Feedback#reliable()} (μ ≥ 0.5).
 * MIN_SNR_RATIO via -Drawwise.cognition.min-snr-ratio config.
 */
public interface EffectEvaluator {

    EffectFeedback evaluate(ExecutionResult result, EvaluationCriteria criteria);

    double estimateSNR(EffectFeedback feedback);

    static EffectEvaluator noop() {
        return new EffectEvaluator() {
            @Override
            public EffectFeedback evaluate(ExecutionResult result, EvaluationCriteria criteria) {
                return EffectFeedback.unavailable();
            }

            @Override
            public double estimateSNR(EffectFeedback feedback) {
                return 0.0;
            }
        };
    }
}
