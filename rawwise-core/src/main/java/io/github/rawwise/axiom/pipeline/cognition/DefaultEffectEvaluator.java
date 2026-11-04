package io.github.rawwise.axiom.pipeline.cognition;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** defaultimplementation. Execution Result raw Metrics,. */
@Slf4j
public class DefaultEffectEvaluator implements EffectEvaluator {

    private final double minSnrRatio;

    public DefaultEffectEvaluator() {
        this.minSnrRatio =
                Double.parseDouble(System.getProperty("rawwise.cognition.min-snr-ratio", "2.0"));
    }

    @Override
    public EffectFeedback evaluate(ExecutionResult result, EvaluationCriteria criteria) {
        if (result == null || !result.success()) {
            return EffectFeedback.unreliable(0.0, Map.of(), 1.0, 0.0);
        }

        var metrics = result.rawMetrics() != null ? result.rawMetrics() : Map.<String, Double>of();
        double overall =
                metrics.values().stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

        double noiseEst = estimateNoise(metrics);
        double signalEst = estimateSignal(metrics, overall);
        double snr = noiseEst > 0 ? signalEst / noiseEst : 0.0;
        double reliabilityMembership =
                noiseEst > 0 ? MembershipFunctions.sigmoid(snr, minSnrRatio, 2.0) : 0.0;

        String assessment =
                reliabilityMembership >= 0.5
                        ? "SNR=%.2f (signal=%.4f, noise=%.4f), reliabilityMembership=%.3f"
                                .formatted(snr, signalEst, noiseEst, reliabilityMembership)
                        : "SNR=%.2f below threshold %.2f, reliabilityMembership=%.3f"
                                .formatted(snr, minSnrRatio, reliabilityMembership);

        log.debug(
                "Effect evaluation: score={} reliabilityMembership={} snr={} decisionId={}",
                overall,
                reliabilityMembership,
                snr,
                result.decisionId());

        return EffectFeedback.of(
                overall, metrics, noiseEst, signalEst, reliabilityMembership, assessment);
    }

    @Override
    public double estimateSNR(EffectFeedback feedback) {
        if (feedback == null || feedback.noiseEstimate() == 0) {
            return 0.0;
        }
        return feedback.signalEstimate() / feedback.noiseEstimate();
    }

    private double estimateNoise(Map<String, Double> metrics) {
        if (metrics.size() < 2) return 1.0;
        var values = metrics.values().stream().mapToDouble(Double::doubleValue).toArray();
        double mean = 0;
        for (var v : values) mean += v;
        mean /= values.length;
        double variance = 0;
        for (var v : values) variance += (v - mean) * (v - mean);
        variance /= values.length;
        return Math.sqrt(variance);
    }

    private double estimateSignal(Map<String, Double> metrics, double overall) {
        return Math.abs(overall);
    }
}
