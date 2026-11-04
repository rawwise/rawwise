package io.github.rawwise.axiom.pipeline.evaluation;

import io.github.rawwise.model.record.View;
import java.util.Map;
import java.util.Optional;

public interface ExternalEvaluationProvider {

    Optional<Double> evaluate(View view, String dimension, Map<String, Object> params);

    default String providerId() {
        return "anonymous";
    }
}
