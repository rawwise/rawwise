package io.github.rawwise.axiom.pipeline.cognition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.model.record.StrategyAction;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * defaultcognitionimplementation. Feedback Loop Closer.on Annotation() async, Strategy Optimizer
 * Cognitive Decision,persist Knowledge Store.
 *
 * <p>Guardrails:
 *
 * <ul>
 *   <li>pending: 10
 *   <li>epoch: 20
 *   <li>limit: 5 LLM /
 *   <li>detection: 3 → TERMINATE
 * </ul>
 */
@Slf4j
public class DefaultCognitionDriver implements CognitionDriver {

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new Jdk8Module())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private static final int MAX_PENDING_DECISIONS = 10;
    private static final int MAX_EXPLORE_ROUNDS = 20;
    private static final int STAGNATION_THRESHOLD = 3;
    private static final double IMPROVEMENT_EPSILON = 0.01;

    private final StrategyOptimizer optimizer;
    private final EffectEvaluator effectEvaluator;
    private final KnowledgeStore knowledgeStore;
    private final String optimizerId;

    private final ConcurrentHashMap<String, CognitiveState> sceneStateCache =
            new ConcurrentHashMap<>();

    public DefaultCognitionDriver(
            StrategyOptimizer optimizer,
            EffectEvaluator effectEvaluator,
            KnowledgeStore knowledgeStore) {
        this.optimizer = optimizer;
        this.effectEvaluator = effectEvaluator;
        this.knowledgeStore = knowledgeStore;
        this.optimizerId = "default";
    }

    @Override
    public CognitiveDecision nextStep(CognitiveState state) {
        var sceneHash = state.sceneHash();

        if (state.exploreCount() >= MAX_EXPLORE_ROUNDS) {
            log.info(
                    "CognitionDriver: max explore rounds reached ({}) for scene={}, terminating",
                    MAX_EXPLORE_ROUNDS,
                    sceneHash);
            return CognitiveDecision.terminate("Max explore rounds reached", sceneHash);
        }

        if (state.consecutiveNoImprovement() >= STAGNATION_THRESHOLD) {
            log.info(
                    "CognitionDriver: stagnation detected ({} rounds no improvement) for scene={}, terminating",
                    state.consecutiveNoImprovement(),
                    sceneHash);
            return CognitiveDecision.terminate(
                    "Stagnation: " + STAGNATION_THRESHOLD + " rounds no improvement", sceneHash);
        }

        if (countPendingDecisions(sceneHash) >= MAX_PENDING_DECISIONS) {
            log.info(
                    "CognitionDriver: max pending decisions reached ({}) for scene={}, terminating",
                    MAX_PENDING_DECISIONS,
                    sceneHash);
            return CognitiveDecision.terminate("Max pending decisions reached", sceneHash);
        }

        var context =
                new OptimizationContext(
                        "scene:" + sceneHash,
                        state.latestMetrics(),
                        List.of(),
                        List.of(),
                        "default");

        var strategyOpt = optimizer.nextStrategy(context);
        if (strategyOpt.isEmpty()) {
            log.debug("CognitionDriver: optimizer returned no strategy for scene={}", sceneHash);
            return CognitiveDecision.terminate("No strategy from optimizer", sceneHash);
        }

        var strategy = strategyOpt.get();
        var convStatus = optimizer.status();
        double expectedImprovement =
                convStatus.averageRegret() > 0
                        ? Math.max(0, convStatus.averageRegret() * 0.5)
                        : 0.05;
        double uncertainty =
                switch (convStatus.state()) {
                    case EXPLORING -> 0.8;
                    case CONVERGING -> 0.4;
                    case CONVERGED -> 0.1;
                    case DIVERGED, DISABLED -> 1.0;
                };

        var decisionType =
                convStatus.state() == ConvergenceStatus.State.CONVERGED
                        ? DecisionType.EXPLOIT
                        : DecisionType.EXPLORE;

        var decision =
                decisionType == DecisionType.EXPLOIT
                        ? CognitiveDecision.exploit(
                                strategy,
                                "Converged state, exploiting best strategy",
                                expectedImprovement,
                                uncertainty,
                                sceneHash)
                        : CognitiveDecision.explore(
                                strategy,
                                "Exploring strategy space",
                                expectedImprovement,
                                uncertainty,
                                sceneHash);

        persistDecision(decision);
        updateState(state, decision);

        log.info(
                "CognitionDriver: produced {} decision for scene={} strategy={} expectedImprovement={}",
                decision.type(),
                sceneHash,
                strategy.strategyId(),
                expectedImprovement);

        return decision;
    }

    @Override
    public void observe(CognitiveDecision decision, ExecutionResult result) {
        if (decision == null || result == null) return;

        var feedback = effectEvaluator.evaluate(result, EvaluationCriteria.defaultCriteria());

        if (decision.strategy() != null) {
            var beforeMetrics = currentState().latestMetrics();
            var afterMetrics =
                    result.rawMetrics() != null ? result.rawMetrics() : Map.<String, Double>of();
            var effect =
                    new StrategyAction.EffectRecord(
                            decision.decisionId(),
                            null,
                            beforeMetrics,
                            afterMetrics,
                            feedback.overallScore(),
                            feedback.reliabilityMembership(),
                            "ENV");
            optimizer.recordEffect(decision.strategy().strategyId(), effect);
        }

        var state = currentState();
        if (state != null && decision.type() == DecisionType.EXPLORE) {
            var updated =
                    state.recordExplore(
                            decision.decisionId(),
                            feedback.overallScore(),
                            decision.strategy() != null ? decision.strategy().strategyId() : null);
            persistState(updated);
        }

        log.info(
                "CognitionDriver: observed decision={} score={} reliabilityMembership={} reliable={}",
                decision.decisionId(),
                feedback.overallScore(),
                String.format("%.3f", feedback.reliabilityMembership()),
                feedback.reliable());
    }

    @Override
    public CognitiveState currentState() {
        return sceneStateCache.values().stream()
                .reduce((a, b) -> a.lastUpdated() >= b.lastUpdated() ? a : b)
                .orElseGet(() -> CognitiveState.initial("unknown", Map.of()));
    }

    public CognitiveState getOrCreateState(String sceneHash, Map<String, Double> metrics) {
        var existing =
                sceneStateCache.computeIfAbsent(
                        sceneHash,
                        k -> {
                            var loaded = loadState(sceneHash);
                            return loaded != null
                                    ? loaded
                                    : CognitiveState.initial(sceneHash, metrics);
                        });
        var updated = existing.updateMetrics(metrics);
        persistState(updated);
        return updated;
    }

    @Override
    public boolean isAvailable() {
        return optimizer.isAvailable();
    }

    private int countPendingDecisions(String sceneHash) {
        try {
            var prefix = ("cognitive_decision:").getBytes(StandardCharsets.UTF_8);
            int count = 0;
            try (var it = knowledgeStore.scan(prefix)) {
                while (it.hasNext()) {
                    var kv = it.next();
                    var key = new String(kv.key(), StandardCharsets.UTF_8);
                    if (key.startsWith("cognitive_decision:") && !key.contains(":consumed")) {
                        count++;
                    }
                }
            }
            return count;
        } catch (IOException e) {
            log.warn(
                    "Failed to count pending decisions for scene={}: {}",
                    sceneHash,
                    e.getMessage());
            return 0;
        }
    }

    private void persistDecision(CognitiveDecision decision) {
        try {
            var key =
                    ("cognitive_decision:" + decision.decisionId())
                            .getBytes(StandardCharsets.UTF_8);
            var value = MAPPER.writeValueAsBytes(decision);
            knowledgeStore.put(key, value);
        } catch (IOException e) {
            log.warn(
                    "Failed to persist cognitive decision={}: {}",
                    decision.decisionId(),
                    e.getMessage());
        }
    }

    private void persistState(CognitiveState state) {
        try {
            var key = ("cognitive_state:" + state.sceneHash()).getBytes(StandardCharsets.UTF_8);
            var value = MAPPER.writeValueAsBytes(state);
            knowledgeStore.put(key, value);
            sceneStateCache.put(state.sceneHash(), state);
        } catch (IOException e) {
            log.warn(
                    "Failed to persist cognitive state for scene={}: {}",
                    state.sceneHash(),
                    e.getMessage());
        }
    }

    private CognitiveState loadState(String sceneHash) {
        try {
            var key = ("cognitive_state:" + sceneHash).getBytes(StandardCharsets.UTF_8);
            var raw = knowledgeStore.get(key);
            if (raw.isPresent()) {
                var state = MAPPER.readValue(raw.get(), CognitiveState.class);
                sceneStateCache.put(sceneHash, state);
                return state;
            }
        } catch (IOException e) {
            log.warn("Failed to load cognitive state for scene={}: {}", sceneHash, e.getMessage());
        }
        return null;
    }

    private void updateState(CognitiveState state, CognitiveDecision decision) {
        var updated =
                decision.type() == DecisionType.EXPLORE
                        ? state.recordExplore(
                                decision.decisionId(),
                                0.0,
                                decision.strategy() != null
                                        ? decision.strategy().strategyId()
                                        : null)
                        : state.recordExploit(decision.decisionId());
        persistState(updated);
    }

    public static String computeSceneHash(String sceneDescription, Map<String, Double> metrics) {
        try {
            var md = MessageDigest.getInstance("SHA-256");
            md.update(sceneDescription.getBytes(StandardCharsets.UTF_8));
            metrics.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(
                            e -> {
                                md.update(e.getKey().getBytes(StandardCharsets.UTF_8));
                                md.update(
                                        String.valueOf(e.getValue())
                                                .getBytes(StandardCharsets.UTF_8));
                            });
            return HexFormat.of().formatHex(md.digest()).substring(0, 16);
        } catch (NoSuchAlgorithmException e) {
            return Integer.toHexString((sceneDescription + metrics).hashCode());
        }
    }
}
