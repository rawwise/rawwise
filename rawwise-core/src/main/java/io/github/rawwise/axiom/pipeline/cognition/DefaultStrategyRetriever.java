package io.github.rawwise.axiom.pipeline.cognition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.record.StrategyAction;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultStrategyRetriever implements StrategyRetriever {

    private static final String STRATEGY_PREFIX = "strategy:";
    private static final List<String> SKIP_SUB_PREFIXES =
            List.of(
                    "strategy:scene:",
                    "strategy:embedding:",
                    "strategy:view:",
                    "strategy:effect:",
                    "strategy:lineage:");
    private static final int CAS_MAX_RETRIES = 8;
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

    private final KnowledgeStore knowledgeStore;

    public DefaultStrategyRetriever(KnowledgeStore knowledgeStore) {
        this.knowledgeStore = knowledgeStore;
    }

    @Override
    public List<StrategyAction> retrieve(StrategyRetrievalContext sceneContext, int topK) {
        Map<String, Double> currentMetrics = sceneContext.currentMetrics();

        List<StrategyAction> candidates = new ArrayList<>();
        try {
            byte[] prefix = STRATEGY_PREFIX.getBytes(StandardCharsets.UTF_8);
            List<KeyValue> entries = knowledgeStore.scanAll(prefix);

            for (KeyValue kv : entries) {
                String keyStr = new String(kv.key(), StandardCharsets.UTF_8);

                boolean skip = false;
                for (String subPrefix : SKIP_SUB_PREFIXES) {
                    if (keyStr.startsWith(subPrefix)) {
                        skip = true;
                        break;
                    }
                }
                if (skip) continue;

                try {
                    StrategyAction strategy = MAPPER.readValue(kv.value(), StrategyAction.class);
                    candidates.add(strategy);
                } catch (IOException e) {
                    log.warn(
                            "Failed to deserialize strategy at key={}: {}", keyStr, e.getMessage());
                }
            }
        } catch (IOException e) {
            log.warn("Failed to scan strategies: {}", e.getMessage());
            return List.of();
        }

        if (candidates.isEmpty()) {
            return List.of();
        }

        int totalStrategies = candidates.size();
        List<ScoredStrategy> scored = new ArrayList<>();

        for (StrategyAction strategy : candidates) {
            double similarity = computeSimilarity(strategy.conditionMetrics(), currentMetrics);

            double confidence = strategy.confidence();
            boolean passesConfidence =
                    confidence > 0.3 || (confidence == 0.0 && totalStrategies == 1);
            if (!passesConfidence) continue;

            List<StrategyAction.EffectRecord> effects = strategy.historicalEffects();
            if (effects != null && !effects.isEmpty()) {
                long effectiveCount =
                        effects.stream().filter(StrategyAction.EffectRecord::effective).count();
                double effectiveRatio = (double) effectiveCount / effects.size();
                if (effectiveRatio <= 0.5) continue;
            }

            scored.add(new ScoredStrategy(strategy, similarity));
        }

        scored.sort((a, b) -> Double.compare(b.similarity(), a.similarity()));

        return scored.stream().limit(topK).map(ScoredStrategy::strategy).toList();
    }

    @Override
    public void recordEffect(String strategyId, StrategyAction.EffectRecord effect) {
        byte[] key = (STRATEGY_PREFIX + strategyId).getBytes(StandardCharsets.UTF_8);

        for (int attempt = 0; attempt < CAS_MAX_RETRIES; attempt++) {
            try {
                Optional<byte[]> currentOpt = knowledgeStore.get(key);
                if (currentOpt.isEmpty()) {
                    log.warn(
                            "Cannot record effect: strategy not found for strategyId={}",
                            strategyId);
                    return;
                }
                byte[] expectedBytes = currentOpt.get();

                StrategyAction existing = MAPPER.readValue(expectedBytes, StrategyAction.class);

                List<StrategyAction.EffectRecord> newEffects =
                        new ArrayList<>(existing.historicalEffects());
                newEffects.add(effect);
                double newConfidence = StrategyAction.computeConfidence(newEffects);

                StrategyAction updated =
                        new StrategyAction(
                                existing.strategyId(),
                                existing.sceneCondition(),
                                existing.conditionMetrics(),
                                existing.strategyContent(),
                                existing.expectedEffect(),
                                newEffects,
                                newConfidence,
                                existing.sourceViewIds(),
                                existing.sourceFeedbackIds(),
                                existing.createdAt(),
                                System.currentTimeMillis());

                byte[] newValue = MAPPER.writeValueAsBytes(updated);

                boolean success =
                        knowledgeStore.compareAndSwap(
                                List.of(new Precondition.ValueEquals(key, expectedBytes)),
                                List.of(new Mutation.Put(key, newValue)));

                if (success) {
                    log.debug(
                            "CAS success for strategyId={}, confidence={}, attempt={}",
                            strategyId,
                            newConfidence,
                            attempt + 1);
                    return;
                }

                log.debug(
                        "CAS conflict for strategyId={}, retrying (attempt={}/{})",
                        strategyId,
                        attempt + 1,
                        CAS_MAX_RETRIES);
            } catch (IOException e) {
                log.warn(
                        "CAS failed for strategyId={}, attempt={}: {}",
                        strategyId,
                        attempt + 1,
                        e.getMessage());
                return;
            }
        }

        log.warn(
                "CAS exhausted {} retries for strategyId={}, falling back to direct put",
                CAS_MAX_RETRIES,
                strategyId);
        try {
            Optional<byte[]> currentOpt = knowledgeStore.get(key);
            if (currentOpt.isPresent()) {
                StrategyAction existing = MAPPER.readValue(currentOpt.get(), StrategyAction.class);
                List<StrategyAction.EffectRecord> newEffects =
                        new ArrayList<>(existing.historicalEffects());
                if (newEffects.stream().noneMatch(e -> e.attemptId().equals(effect.attemptId()))) {
                    newEffects.add(effect);
                }
                double newConfidence = StrategyAction.computeConfidence(newEffects);
                StrategyAction finalUpdated =
                        new StrategyAction(
                                existing.strategyId(),
                                existing.sceneCondition(),
                                existing.conditionMetrics(),
                                existing.strategyContent(),
                                existing.expectedEffect(),
                                newEffects,
                                newConfidence,
                                existing.sourceViewIds(),
                                existing.sourceFeedbackIds(),
                                existing.createdAt(),
                                System.currentTimeMillis());
                knowledgeStore.put(key, MAPPER.writeValueAsBytes(finalUpdated));
                log.info("Fallback put succeeded for strategyId={}", strategyId);
            }
        } catch (IOException e) {
            log.error("Fallback put failed for strategyId={}: {}", strategyId, e.getMessage());
        }
    }

    private double computeSimilarity(
            Map<String, Double> conditionMetrics, Map<String, Double> currentMetrics) {
        if (conditionMetrics == null
                || conditionMetrics.isEmpty()
                || currentMetrics == null
                || currentMetrics.isEmpty()) {
            return 0.0;
        }

        double sumSquaredDiff = 0.0;
        int intersectionCount = 0;

        for (Map.Entry<String, Double> entry : conditionMetrics.entrySet()) {
            String key = entry.getKey();
            if (currentMetrics.containsKey(key)) {
                double diff = entry.getValue() - currentMetrics.get(key);
                sumSquaredDiff += diff * diff;
                intersectionCount++;
            }
        }

        if (intersectionCount == 0) {
            return 0.0;
        }

        double distance = Math.sqrt(sumSquaredDiff);
        return 1.0 / (1.0 + distance);
    }

    private record ScoredStrategy(StrategyAction strategy, double similarity) {}
}
