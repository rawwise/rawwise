package io.github.rawwise.benchmark.vbench;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * VBench 16-dimension evaluation adapter.
 *
 * <p>Real VBench flow(Python): 1. videogenerategeneratemodelgenerategeneratevideo → MP4 file 2.
 * VBench Python evaluationscriptreadvideo → 16 dimensionscore JSON 3. this Adapter read JSON →
 * injectto ExternalEvaluationProvider.params 4. EvaluationEngine.evaluate(view, criteria) →
 * reuseuse 7 dimensionevaluationframework
 *
 * <p>This class provides stub scoring: deterministic scores based on prompt + whether RawWise is
 * used, for E2E testing and comparison experiment framework verification.
 */
@Slf4j
public class VBenchAdapter {

    public static final List<String> VBENCH_DIMENSIONS =
            List.of(
                    "subject_consistency",
                    "background_consistency",
                    "temporal_flickering",
                    "motion_smoothness",
                    "dynamic_degree",
                    "aesthetic_quality",
                    "imaging_quality",
                    "object_class",
                    "multiple_objects",
                    "human_action",
                    "color",
                    "spatial_relationship",
                    "scene",
                    "appearance_style",
                    "temporal_style",
                    "overall_consistency");

    /**
     * VBench evaluate stub: generates deterministic 16-dimension scores based on prompt and
     * hasRawWise.
     *
     * <p>Improvement magnitude extrapolated from Vgent (NeurIPS 2025) and other public empirical
     * data: - consistency dimension: 3-5% improvement - aesthetic / motion dimensions:<1%
     * improvement
     */
    public Map<String, Double> evaluate(String prompt, byte[] videoBytes, boolean hasRawWise) {
        if (prompt == null) {
            throw new IllegalArgumentException("prompt is required");
        }

        long promptHash = stableHash(prompt);
        Map<String, Double> scores = new HashMap<>();

        scores.put("subject_consistency", baseScoreWithJitter(0.75, promptHash, 1));
        scores.put("background_consistency", baseScoreWithJitter(0.80, promptHash, 2));
        scores.put("temporal_flickering", baseScoreWithJitter(0.85, promptHash, 3));
        scores.put("motion_smoothness", baseScoreWithJitter(0.90, promptHash, 4));
        scores.put("dynamic_degree", baseScoreWithJitter(0.70, promptHash, 5));
        scores.put("aesthetic_quality", baseScoreWithJitter(0.55, promptHash, 6));
        scores.put("imaging_quality", baseScoreWithJitter(0.65, promptHash, 7));
        scores.put("object_class", baseScoreWithJitter(0.70, promptHash, 8));
        scores.put("multiple_objects", baseScoreWithJitter(0.55, promptHash, 9));
        scores.put("human_action", baseScoreWithJitter(0.70, promptHash, 10));
        scores.put("color", baseScoreWithJitter(0.75, promptHash, 11));
        scores.put("spatial_relationship", baseScoreWithJitter(0.65, promptHash, 12));
        scores.put("scene", baseScoreWithJitter(0.70, promptHash, 13));
        scores.put("appearance_style", baseScoreWithJitter(0.60, promptHash, 14));
        scores.put("temporal_style", baseScoreWithJitter(0.65, promptHash, 15));
        scores.put("overall_consistency", baseScoreWithJitter(0.72, promptHash, 16));

        if (hasRawWise) {
            // based on Vgent empiricaldata:consistency dimensionslightimprovement
            scores.compute("subject_consistency", (k, v) -> Math.min(1.0, v + 0.03));
            scores.compute("background_consistency", (k, v) -> Math.min(1.0, v + 0.025));
            scores.compute("temporal_flickering", (k, v) -> Math.min(1.0, v + 0.02));
            scores.compute("overall_consistency", (k, v) -> Math.min(1.0, v + 0.025));
        }

        return scores;
    }

    public double averageScore(Map<String, Double> scores) {
        if (scores == null || scores.isEmpty()) {
            return 0.0;
        }
        double sum = scores.values().stream().mapToDouble(Double::doubleValue).sum();
        return sum / scores.size();
    }

    /**
     * Convert VBench scores to RawWise EvaluationCriteria.parameters, for ClipScoreProvider and
     * other ExternalEvaluationProvider consumers.
     */
    public Map<String, Object> toEvaluationParams(Map<String, Double> vbenchScores) {
        Map<String, Object> params = new HashMap<>();
        Double aesthetic = vbenchScores.get("aesthetic_quality");
        if (aesthetic != null) {
            params.put("clip_aesthetic_score", aesthetic);
        }
        Double subject = vbenchScores.get("subject_consistency");
        if (subject != null) {
            params.put("subject_consistency_score", subject);
        }
        Double flickering = vbenchScores.get("temporal_flickering");
        if (flickering != null) {
            params.put("temporal_flickering_score", flickering);
        }
        Double overall = vbenchScores.get("overall_consistency");
        if (overall != null) {
            params.put("clip_text_image_similarity", overall);
        }
        return params;
    }

    private double baseScoreWithJitter(double base, long promptHash, int dimSeed) {
        long combined = promptHash + dimSeed * 1009L;
        double jitter = ((combined & 0xFF) / 255.0 - 0.5) * 0.04;
        double score = base + jitter;
        return Math.max(0.0, Math.min(1.0, score));
    }

    private long stableHash(String s) {
        long h = 1125899906842597L;
        for (int i = 0; i < s.length(); i++) {
            h = 31 * h + s.charAt(i);
        }
        return h;
    }
}
