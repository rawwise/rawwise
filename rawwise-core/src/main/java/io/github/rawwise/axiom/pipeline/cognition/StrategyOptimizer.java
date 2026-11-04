package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.StrategyAction;
import java.util.Optional;

/**
 * strategy optimization SPI - "strategy". Strategy Learning Service,recordstatusmanagement. Raw
 * Wise default 3 implementation,user.
 */
public interface StrategyOptimizer {

    Optional<StrategyAction> nextStrategy(OptimizationContext context);

    void recordEffect(String strategyId, StrategyAction.EffectRecord effect);

    ConvergenceStatus status();

    boolean isAvailable();

    static StrategyOptimizer noop() {
        return new StrategyOptimizer() {
            @Override
            public Optional<StrategyAction> nextStrategy(OptimizationContext context) {
                return Optional.empty();
            }

            @Override
            public void recordEffect(String strategyId, StrategyAction.EffectRecord effect) {}

            @Override
            public ConvergenceStatus status() {
                return ConvergenceStatus.disabled();
            }

            @Override
            public boolean isAvailable() {
                return false;
            }
        };
    }
}
