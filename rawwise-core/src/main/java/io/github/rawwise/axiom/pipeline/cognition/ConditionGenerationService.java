package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.axiom.pipeline.state.ContractViolation;
import io.github.rawwise.model.record.Action;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * conditiongenerationorchestration. strategyretrieval, fallback Condition Construction Pipeline.
 */
@Slf4j
public class ConditionGenerationService {

    private final StrategyRetriever strategyRetriever;
    private final ConditionConstructionPipeline conditionPipeline;
    private final ConditionReasoningService reasoningService;

    public ConditionGenerationService(
            StrategyRetriever strategyRetriever,
            ConditionConstructionPipeline conditionPipeline,
            ConditionReasoningService reasoningService) {
        this.strategyRetriever = strategyRetriever;
        this.conditionPipeline = conditionPipeline;
        this.reasoningService = reasoningService;
    }

    /**
     * generationcondition.
     *
     * @param prompt prompt
     * @param current Metrics
     * @param violation status(null)
     * @param spec condition(null)
     * @return conditiontext
     */
    public Optional<String> generateConditionForNextRound(
            String prompt,
            Map<String, Double> currentMetrics,
            ContractViolation violation,
            ConditionConstructionSpec spec) {

        // 1. strategyretrieval
        StrategyRetrievalContext sceneContext =
                new StrategyRetrievalContext(prompt, currentMetrics);
        List<io.github.rawwise.model.record.StrategyAction> strategies =
                strategyRetriever.retrieve(sceneContext, 3);

        if (!strategies.isEmpty()) {
            log.info("Retrieved {} strategies for scene: {}", strategies.size(), prompt);
            // strategy → LLM strategygenerationcondition
            String strategySummary = formatStrategies(strategies);
            ConditionReasoningContext reasoningContext =
                    new ConditionReasoningContext(
                            prompt,
                            List.of(strategySummary),
                            Map.of(),
                            Map.of(),
                            currentMetrics,
                            List.of());
            ConditionReasoningResult result = reasoningService.reason(reasoningContext);
            if (result != null
                    && result.improvedCondition() != null
                    && !result.improvedCondition().isBlank()) {
                return Optional.of(result.improvedCondition());
            }
        }

        // 2. strategy → fallback Condition Construction Pipeline
        if (violation != null && spec != null) {
            ConditionReasoningContext fallbackContext =
                    new ConditionReasoningContext(
                            prompt, List.of(prompt), Map.of(), Map.of(), currentMetrics, List.of());
            Optional<Action> action =
                    conditionPipeline.constructConditionWithReasoning(
                            violation, spec, fallbackContext);
            return action.map(a -> new String(a.content()));
        }

        // 3. → return prompt
        return Optional.of(prompt);
    }

    private String formatStrategies(
            List<io.github.rawwise.model.record.StrategyAction> strategies) {
        StringBuilder sb = new StringBuilder();
        for (var s : strategies) {
            sb.append("Strategy: ").append(s.strategyContent()).append("\n");
            sb.append("  Scene: ").append(s.sceneCondition()).append("\n");
            sb.append("  Expected: ").append(s.expectedEffect()).append("\n");
            sb.append("  Confidence: ").append(s.confidence()).append("\n");
            if (!s.historicalEffects().isEmpty()) {
                long effective =
                        s.historicalEffects().stream()
                                .filter(
                                        io.github.rawwise.model.record.StrategyAction.EffectRecord
                                                ::effective)
                                .count();
                sb.append("  History: ")
                        .append(effective)
                        .append("/")
                        .append(s.historicalEffects().size())
                        .append(" effective\n");
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
