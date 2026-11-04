package io.github.rawwise.frontend.evaluator;

import java.util.Map;

public class DefaultQueryEvaluator implements QueryEvaluator {

    @Override
    public EvaluationResult evaluate(String sql) {
        return new EvaluationResult(ExecutionMode.SYNC, Map.of());
    }
}
