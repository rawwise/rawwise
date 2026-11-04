package io.github.rawwise.axiom.pipeline.evaluation;

import io.github.rawwise.model.record.View;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultExternalEvaluationProvider implements ExternalEvaluationProvider {

    @Override
    public Optional<Double> evaluate(View view, String dimension, Map<String, Object> params) {
        log.debug("DefaultExternalEvaluationProvider: no-op for dimension={}", dimension);
        return Optional.empty();
    }

    @Override
    public String providerId() {
        return "default-noop";
    }
}
