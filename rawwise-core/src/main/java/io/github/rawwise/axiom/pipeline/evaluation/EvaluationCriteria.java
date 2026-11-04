package io.github.rawwise.axiom.pipeline.evaluation;

import io.github.rawwise.model.record.View;
import java.util.HashMap;
import java.util.Map;

public record EvaluationCriteria(
        Map<EvaluationEngine.EvaluationDimension, Double> weights, Map<String, Object> parameters) {

    public EvaluationCriteria {
        if (weights == null) {
            weights = defaultWeights();
        }
        if (parameters == null) {
            parameters = new HashMap<>();
        }
    }

    public static EvaluationCriteria createDefault() {
        return new EvaluationCriteria(defaultWeights(), new HashMap<>());
    }

    public static EvaluationCriteria create(View.ViewType viewType) {
        Map<EvaluationEngine.EvaluationDimension, Double> w = defaultWeights();
        Map<String, Object> params = new HashMap<>();

        switch (viewType) {
            case RAW_IDENTITY -> {
                w.put(EvaluationEngine.EvaluationDimension.COMPLETENESS, 0.5);
                w.put(EvaluationEngine.EvaluationDimension.RELEVANCE, 0.2);
                w.put(EvaluationEngine.EvaluationDimension.FRESHNESS, 0.3);
            }
            case EMBEDDING_INDEX -> {
                w.put(EvaluationEngine.EvaluationDimension.COMPLETENESS, 0.4);
                w.put(EvaluationEngine.EvaluationDimension.RELEVANCE, 0.3);
                w.put(EvaluationEngine.EvaluationDimension.FRESHNESS, 0.3);
                params.put("minDimension", 128);
            }
            case SUMMARY -> {
                w.put(EvaluationEngine.EvaluationDimension.COMPLETENESS, 0.35);
                w.put(EvaluationEngine.EvaluationDimension.RELEVANCE, 0.40);
                w.put(EvaluationEngine.EvaluationDimension.FRESHNESS, 0.25);
            }
            case QUERY_RESULT -> {
                w.put(EvaluationEngine.EvaluationDimension.COMPLETENESS, 0.3);
                w.put(EvaluationEngine.EvaluationDimension.RELEVANCE, 0.45);
                w.put(EvaluationEngine.EvaluationDimension.FRESHNESS, 0.25);
            }
            case CLASSIFICATION -> {
                w.put(EvaluationEngine.EvaluationDimension.COMPLETENESS, 0.3);
                w.put(EvaluationEngine.EvaluationDimension.RELEVANCE, 0.45);
                w.put(EvaluationEngine.EvaluationDimension.FRESHNESS, 0.25);
            }
            case RETRIEVAL_TOPK -> {
                w.put(EvaluationEngine.EvaluationDimension.COMPLETENESS, 0.25);
                w.put(EvaluationEngine.EvaluationDimension.RELEVANCE, 0.45);
                w.put(EvaluationEngine.EvaluationDimension.FRESHNESS, 0.30);
            }
        }
        return new EvaluationCriteria(w, params);
    }

    private static Map<EvaluationEngine.EvaluationDimension, Double> defaultWeights() {
        Map<EvaluationEngine.EvaluationDimension, Double> w = new HashMap<>();
        w.put(EvaluationEngine.EvaluationDimension.COMPLETENESS, 0.4);
        w.put(EvaluationEngine.EvaluationDimension.RELEVANCE, 0.35);
        w.put(EvaluationEngine.EvaluationDimension.FRESHNESS, 0.25);
        w.put(EvaluationEngine.EvaluationDimension.PERCEPTUAL_QUALITY, 0.15);
        w.put(EvaluationEngine.EvaluationDimension.SEQUENCE_CONSISTENCY, 0.10);
        w.put(EvaluationEngine.EvaluationDimension.INSTRUCTION_FOLLOWING, 0.10);
        w.put(EvaluationEngine.EvaluationDimension.DOMAIN_COHERENCE, 0.10);
        return w;
    }
}
