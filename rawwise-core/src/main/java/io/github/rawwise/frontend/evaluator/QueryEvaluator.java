package io.github.rawwise.frontend.evaluator;

public interface QueryEvaluator {

    EvaluationResult evaluate(String sql);

    enum ExecutionMode {
        SYNC,
        ASYNC
    }

    record EvaluationResult(ExecutionMode mode, java.util.Map<String, Object> hints) {}
}
