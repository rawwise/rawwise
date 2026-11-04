package io.github.rawwise.axiom.pipeline.cognition;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.model.record.StrategyAction;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * ε-greedy strategy optimization - LLM rule-based fallback implementation.
 *
 * <p>config LLM endpoint,Wrapping Strategy Optimizer (Strategy Learning Service).
 * implementation-loop:probability ε strategy random, probability 1-ε strategy.ε 0.95(0.05), .
 *
 * <p>status persist Knowledge Store(Wrapping Strategy Optimizer persist):
 *
 * <ul>
 *   <li>{@code simple_opt_score:<strategy Id>} - strategy score Strategy Score JSON
 *   <li>{@code simple_opt_effect:<strategy Id>:<attempt Id>} - record Effect Record JSON
 *   <li>{@code simple_opt_status:default} - status Convergence Status JSON
 * </ul>
 *
 * <p>strategy derivation(chunk size,overlap,batch,rerank), LLM,cognition loop.
 */
@Slf4j
public class SimpleStrategyOptimizer implements StrategyOptimizer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final double INITIAL_EPSILON = 0.3;
    private static final double EPSILON_DECAY = 0.95;
    private static final double MIN_EPSILON = 0.05;

    private static final String SCORE_PREFIX = "simple_opt_score:";
    private static final String EFFECT_PREFIX = "simple_opt_effect:";
    private static final String STATUS_KEY = "simple_opt_status:default";

    private final KnowledgeStore knowledgeStore;
    private final Random random;
    private final AtomicReference<Double> epsilon = new AtomicReference<>(INITIAL_EPSILON);
    private final AtomicInteger roundsCompleted = new AtomicInteger(0);
    private final ConcurrentHashMap<String, StrategyScore> scoreCache = new ConcurrentHashMap<>();

    private static final List<StrategyAction> BUILTIN_STRATEGIES =
            List.of(
                    builtin(
                            "rule:chunk-size-up",
                            "increase chunk size by 25% for broader context window",
                            "Higher recall via broader context, potential precision tradeoff"),
                    builtin(
                            "rule:chunk-size-down",
                            "decrease chunk size by 25% for finer granularity",
                            "Higher precision via focused chunks, potential recall tradeoff"),
                    builtin(
                            "rule:overlap-increase",
                            "increase chunk overlap to 20% for better boundary recall",
                            "Reduces boundary information loss across chunk edges"),
                    builtin(
                            "rule:embedding-batch-up",
                            "increase embedding batch size for higher ingestion throughput",
                            "Higher throughput at constant embedding quality"),
                    builtin(
                            "rule:rerank-enable",
                            "enable cross-encoder reranking on top-20 candidates",
                            "Precision boost at cost of retrieval latency"));

    public SimpleStrategyOptimizer(KnowledgeStore knowledgeStore) {
        this(knowledgeStore, new Random());
    }

    SimpleStrategyOptimizer(KnowledgeStore knowledgeStore, Random random) {
        this.knowledgeStore = knowledgeStore;
        this.random = random;
        loadScores();
    }

    @Override
    public Optional<StrategyAction> nextStrategy(OptimizationContext context) {
        double currentEpsilon = epsilon.get();
        boolean explore = random.nextDouble() < currentEpsilon;

        int rounds = roundsCompleted.incrementAndGet();
        double newEpsilon =
                epsilon.updateAndGet(curr -> Math.max(MIN_EPSILON, curr * EPSILON_DECAY));

        StrategyAction chosen;
        if (explore || scoreCache.isEmpty()) {
            chosen = BUILTIN_STRATEGIES.get(random.nextInt(BUILTIN_STRATEGIES.size()));
            log.info(
                    "SimpleStrategyOptimizer EXPLORE: strategy={} epsilon={}->{} rounds={}",
                    chosen.strategyId(),
                    String.format("%.3f", currentEpsilon),
                    String.format("%.3f", newEpsilon),
                    rounds);
        } else {
            chosen = findBestStrategy();
            log.info(
                    "SimpleStrategyOptimizer EXPLOIT: strategy={} avgScore={} epsilon={} rounds={}",
                    chosen.strategyId(),
                    String.format("%.3f", scoreOf(chosen.strategyId()).averageScore()),
                    String.format("%.3f", newEpsilon),
                    rounds);
        }

        persistStatus();
        return Optional.of(chosen);
    }

    @Override
    public void recordEffect(String strategyId, StrategyAction.EffectRecord effect) {
        if (strategyId == null || effect == null) return;

        try {
            var effectKey =
                    (EFFECT_PREFIX + strategyId + ":" + effect.attemptId())
                            .getBytes(StandardCharsets.UTF_8);
            knowledgeStore.put(effectKey, MAPPER.writeValueAsBytes(effect));
        } catch (IOException e) {
            log.warn("Failed to persist effect for strategy={}: {}", strategyId, e.getMessage());
        }

        StrategyScore updated =
                scoreCache.compute(
                        strategyId,
                        (id, existing) ->
                                existing == null
                                        ? new StrategyScore(id, effect.effectivenessMembership(), 1)
                                        : existing.accumulate(effect.effectivenessMembership()));
        persistScore(updated);

        log.debug(
                "SimpleStrategyOptimizer recorded effect: strategy={} attempt={} effectiveness={}",
                strategyId,
                effect.attemptId(),
                String.format("%.3f", effect.effectivenessMembership()));
    }

    @Override
    public ConvergenceStatus status() {
        String bestId = findBestStrategyId();
        double bestAvg = bestId != null ? scoreOf(bestId).averageScore() : 0.0;
        double currentEpsilon = epsilon.get();
        double regret = Math.max(0.0, 1.0 - bestAvg);
        var state =
                currentEpsilon <= MIN_EPSILON
                        ? ConvergenceStatus.State.CONVERGING
                        : ConvergenceStatus.State.EXPLORING;
        double confidence = 1.0 - currentEpsilon;
        String assessment =
                "epsilon=%.3f bestStrategy=%s bestAvgScore=%.3f rounds=%d"
                        .formatted(
                                currentEpsilon,
                                bestId != null ? bestId : "none",
                                bestAvg,
                                roundsCompleted.get());
        return new ConvergenceStatus(state, roundsCompleted.get(), regret, confidence, assessment);
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    private StrategyAction findBestStrategy() {
        String bestId = findBestStrategyId();
        if (bestId == null) {
            return BUILTIN_STRATEGIES.get(0);
        }
        for (var s : BUILTIN_STRATEGIES) {
            if (s.strategyId().equals(bestId)) return s;
        }
        return BUILTIN_STRATEGIES.get(0);
    }

    private String findBestStrategyId() {
        String bestId = null;
        double bestAvg = Double.NEGATIVE_INFINITY;
        for (var entry : scoreCache.entrySet()) {
            double avg = entry.getValue().averageScore();
            if (avg > bestAvg) {
                bestAvg = avg;
                bestId = entry.getKey();
            }
        }
        return bestId;
    }

    private StrategyScore scoreOf(String strategyId) {
        return scoreCache.getOrDefault(strategyId, new StrategyScore(strategyId, 0.0, 0));
    }

    private void persistScore(StrategyScore score) {
        try {
            var key = (SCORE_PREFIX + score.strategyId()).getBytes(StandardCharsets.UTF_8);
            knowledgeStore.put(key, MAPPER.writeValueAsBytes(score));
        } catch (IOException e) {
            log.warn(
                    "Failed to persist score for strategy={}: {}",
                    score.strategyId(),
                    e.getMessage());
        }
    }

    private void persistStatus() {
        try {
            var status = status();
            knowledgeStore.put(
                    STATUS_KEY.getBytes(StandardCharsets.UTF_8), MAPPER.writeValueAsBytes(status));
        } catch (IOException e) {
            log.warn("Failed to persist optimizer status: {}", e.getMessage());
        }
    }

    private void loadScores() {
        try {
            var prefix = SCORE_PREFIX.getBytes(StandardCharsets.UTF_8);
            try (var it = knowledgeStore.scan(prefix)) {
                while (it.hasNext()) {
                    var kv = it.next();
                    try {
                        var score = MAPPER.readValue(kv.value(), StrategyScore.class);
                        scoreCache.put(score.strategyId(), score);
                    } catch (IOException e) {
                        log.debug("Skipping unparseable score entry: {}", e.getMessage());
                    }
                }
            }
            if (!scoreCache.isEmpty()) {
                log.info(
                        "SimpleStrategyOptimizer loaded {} strategy scores from KnowledgeStore",
                        scoreCache.size());
            }
        } catch (IOException e) {
            log.warn("Failed to load strategy scores: {}", e.getMessage());
        }
    }

    private static StrategyAction builtin(String id, String content, String expectedEffect) {
        long now = System.currentTimeMillis();
        return new StrategyAction(
                id,
                "rule-based-fallback",
                Map.of(),
                content,
                expectedEffect,
                List.of(),
                0.0,
                List.of(),
                List.of(),
                now,
                now);
    }

    private record StrategyScore(String strategyId, double totalScore, int sampleCount) {
        double averageScore() {
            return sampleCount == 0 ? 0.0 : totalScore / sampleCount;
        }

        StrategyScore accumulate(double score) {
            return new StrategyScore(strategyId, totalScore + score, sampleCount + 1);
        }
    }
}
