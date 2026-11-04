package io.github.rawwise.benchmark.vbench.providers;

import io.github.rawwise.axiom.pipeline.evaluation.ExternalEvaluationProvider;
import io.github.rawwise.model.record.View;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * CLIP score evaluation provider: consumes RawWise generic ExternalEvaluationProvider SPI.
 *
 * <p>Evaluation dimensions: - PERCEPTUAL_QUALITY: CLIP-based aesthetic score -
 * INSTRUCTION_FOLLOWING: CLIP image-text similarity(prompt adherence)
 *
 * <p>Two paths for actual algorithm integration: 1. Pre-computed CLIP scores passed directly via
 * params (computed externally by Python script and injected) 2. Call local CLIP inference service
 * via HTTP (e.g. OpenAI CLIP / SigLIP)
 *
 * <p>This implementation uses option 1: extract pre-computed scores from params, easy to integrate
 * with VBench offline evaluation flow. Option 2 (HTTP call) can be implemented via subclass
 * ClipHttpScoreProvider.
 */
@Slf4j
public class ClipScoreProvider implements ExternalEvaluationProvider {

    public static final String PARAM_CLIP_AESTHETIC = "clip_aesthetic_score";
    public static final String PARAM_CLIP_TEXT_IMAGE = "clip_text_image_similarity";
    public static final String PARAM_TEMPORAL_FLICKERING = "temporal_flickering_score";
    public static final String PARAM_SUBJECT_CONSISTENCY = "subject_consistency_score";

    private final String providerId;

    public ClipScoreProvider() {
        this("clip-vit-large-patch14");
    }

    public ClipScoreProvider(String providerId) {
        this.providerId = providerId;
    }

    @Override
    public Optional<Double> evaluate(View view, String dimension, Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return Optional.empty();
        }

        return switch (dimension) {
            case "PERCEPTUAL_QUALITY" -> extractScore(params, PARAM_CLIP_AESTHETIC);
            case "INSTRUCTION_FOLLOWING" -> extractScore(params, PARAM_CLIP_TEXT_IMAGE);
            case "SEQUENCE_CONSISTENCY" -> extractCompositeSequenceScore(params);
            case "DOMAIN_COHERENCE" -> extractScore(params, PARAM_SUBJECT_CONSISTENCY);
            default -> Optional.empty();
        };
    }

    @Override
    public String providerId() {
        return providerId;
    }

    private Optional<Double> extractScore(Map<String, Object> params, String key) {
        Object val = params.get(key);
        if (val == null) {
            return Optional.empty();
        }
        try {
            if (val instanceof Number n) {
                return Optional.of(n.doubleValue());
            }
            return Optional.of(Double.parseDouble(val.toString()));
        } catch (NumberFormatException e) {
            log.warn("Failed to parse score for key={}, value={}", key, val);
            return Optional.empty();
        }
    }

    private Optional<Double> extractCompositeSequenceScore(Map<String, Object> params) {
        Optional<Double> flickering = extractScore(params, PARAM_TEMPORAL_FLICKERING);
        Optional<Double> subjectConsistency = extractScore(params, PARAM_SUBJECT_CONSISTENCY);

        if (flickering.isEmpty() && subjectConsistency.isEmpty()) {
            return Optional.empty();
        }

        double composite = 0.0;
        int count = 0;
        if (flickering.isPresent()) {
            composite += flickering.get();
            count++;
        }
        if (subjectConsistency.isPresent()) {
            composite += subjectConsistency.get();
            count++;
        }
        return Optional.of(composite / count);
    }
}
