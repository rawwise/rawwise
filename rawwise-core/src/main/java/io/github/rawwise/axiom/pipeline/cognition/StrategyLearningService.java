package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.StrategyAction;

/**
 * T function's Update Models componentimplementation. strategy, Strategy Action write M_t.
 *
 * <p>SPI interface, noop defaultimplementation LLM implementation.
 */
public interface StrategyLearningService {

    /**
     * strategy
     *
     * @param context context(,status,strategy)
     * @return strategy,return Optional.empty()
     */
    java.util.Optional<StrategyAction> learn(StrategyLearningContext context);

    /** (LLM endpoint configcorrectreturn true). */
    boolean isAvailable();

    /** default noop implementation:strategy. */
    static StrategyLearningService noop() {
        return new StrategyLearningService() {
            @Override
            public java.util.Optional<StrategyAction> learn(StrategyLearningContext context) {
                return java.util.Optional.empty();
            }

            @Override
            public boolean isAvailable() {
                return false;
            }
        };
    }
}
