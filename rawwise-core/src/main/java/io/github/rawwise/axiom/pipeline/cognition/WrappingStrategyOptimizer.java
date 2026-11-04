package io.github.rawwise.axiom.pipeline.cognition;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.model.record.StrategyAction;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Strategy Optimizer implementation - learn() Strategy Learning Service, management record Effect()
 * status() status.
 *
 * <p>status persist Knowledge Store:
 *
 * <ul>
 *   <li>optimizer_status:<optimizer Id> - Convergence Status JSON
 *   <li>optimizer_effect:<strategy Id>:<attempt Id> - Effect Record index
 * </ul>
 */
@Slf4j
public class WrappingStrategyOptimizer implements StrategyOptimizer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final StrategyLearningService delegate;
    private final KnowledgeStore knowledgeStore;
    private final String optimizerId;
    private final ConcurrentHashMap<String, ConvergenceStatus> sceneStatusMap;

    public WrappingStrategyOptimizer(
            StrategyLearningService delegate, KnowledgeStore knowledgeStore) {
        this(delegate, knowledgeStore, "default");
    }

    public WrappingStrategyOptimizer(
            StrategyLearningService delegate, KnowledgeStore knowledgeStore, String optimizerId) {
        this.delegate = delegate;
        this.knowledgeStore = knowledgeStore;
        this.optimizerId = optimizerId;
        this.sceneStatusMap = new ConcurrentHashMap<>();
    }

    @Override
    public Optional<StrategyAction> nextStrategy(OptimizationContext context) {
        if (!isAvailable()) {
            return Optional.empty();
        }

        var learningContext =
                new StrategyLearningContext(
                        context.feedbackHistory(),
                        buildSystemStateSummary(context),
                        context.existingStrategies(),
                        context.tableId());

        var strategy = delegate.learn(learningContext);
        if (strategy.isPresent()) {
            log.info(
                    "StrategyOptimizer produced strategy={} scene={}",
                    strategy.get().strategyId(),
                    context.sceneDescription());
            var newStatus = ConvergenceStatus.exploring(0, 0.0);
            sceneStatusMap.put(context.sceneDescription(), newStatus);
            persistStatus(context.sceneDescription(), newStatus);
        }
        return strategy;
    }

    @Override
    public void recordEffect(String strategyId, StrategyAction.EffectRecord effect) {
        if (strategyId == null || effect == null) return;

        try {
            var effectKey =
                    ("optimizer_effect:" + strategyId + ":" + effect.attemptId())
                            .getBytes(StandardCharsets.UTF_8);
            var effectJson = MAPPER.writeValueAsBytes(effect);
            knowledgeStore.put(effectKey, effectJson);
            log.debug(
                    "Recorded effect for strategy={} attempt={} effective={}",
                    strategyId,
                    effect.attemptId(),
                    String.format("%.3f", effect.effectivenessMembership()));
        } catch (IOException e) {
            log.warn("Failed to record effect for strategy={}: {}", strategyId, e.getMessage());
        }
    }

    @Override
    public ConvergenceStatus status() {
        return sceneStatusMap.values().stream()
                .reduce(
                        ConvergenceStatus.disabled(),
                        (a, b) -> {
                            if (a.state() == ConvergenceStatus.State.DISABLED) return b;
                            if (b.state() == ConvergenceStatus.State.DISABLED) return a;
                            return a.roundsCompleted() >= b.roundsCompleted() ? a : b;
                        });
    }

    @Override
    public boolean isAvailable() {
        return delegate != null && delegate.isAvailable();
    }

    private String buildSystemStateSummary(OptimizationContext context) {
        var sb = new StringBuilder();
        sb.append("Scene: ").append(context.sceneDescription()).append("\n");
        sb.append("Current metrics: ").append(context.currentMetrics()).append("\n");
        sb.append("Feedback count: ")
                .append(context.feedbackHistory() != null ? context.feedbackHistory().size() : 0)
                .append("\n");
        sb.append("Existing strategies: ")
                .append(
                        context.existingStrategies() != null
                                ? context.existingStrategies().size()
                                : 0);
        return sb.toString();
    }

    private void persistStatus(String scene, ConvergenceStatus status) {
        try {
            var key =
                    ("optimizer_status:" + optimizerId + ":" + scene)
                            .getBytes(StandardCharsets.UTF_8);
            var value = MAPPER.writeValueAsBytes(status);
            knowledgeStore.put(key, value);
        } catch (IOException e) {
            log.warn("Failed to persist optimizer status for scene={}: {}", scene, e.getMessage());
        }
    }
}
