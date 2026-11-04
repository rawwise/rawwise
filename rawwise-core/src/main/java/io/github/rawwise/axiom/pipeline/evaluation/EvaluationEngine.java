package io.github.rawwise.axiom.pipeline.evaluation;

import io.github.rawwise.model.record.View;
import java.util.List;
import java.util.Map;

public interface EvaluationEngine {

    EvaluationResult evaluate(View view, EvaluationCriteria criteria);

    ComparisonResult compare(View v1, View v2, EvaluationCriteria criteria);

    List<EvaluationResult> evaluationHistory(String viewId);

    EvaluationCriteria getDefaultCriteria(View.ViewType viewType);

    record EvaluationResult(
            String viewId,
            double qualityScore,
            Map<EvaluationDimension, Double> dimensionScores,
            String explanation,
            long timestamp) {}

    record ComparisonResult(
            double scoreV1, double scoreV2, String diffReport, Map<String, Object> metrics) {}

    enum EvaluationDimension {
        COMPLETENESS,
        RELEVANCE,
        FRESHNESS,
        PERCEPTUAL_QUALITY,
        SEQUENCE_CONSISTENCY,
        INSTRUCTION_FOLLOWING,
        DOMAIN_COHERENCE
    }
}
