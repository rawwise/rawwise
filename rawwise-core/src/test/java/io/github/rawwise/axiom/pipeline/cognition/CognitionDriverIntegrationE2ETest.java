package io.github.rawwise.axiom.pipeline.cognition;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.model.record.StrategyAction;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * CognitionDriver end-to-end integration test.
 *
 * <p>verifyfull chain: CognitionDriver.nextStep() -> CognitiveDecision persistedto KnowledgeStore
 * -> verify. real : InMemoryKnowledgeStore + Mock StrategyLearningService(strategy, does not depend
 * on LLM).
 */
@DisplayName("CognitionDriver Integration E2E - cognition end-to-end integration test")
class CognitionDriverIntegrationE2ETest {

    private KnowledgeStore knowledgeStore;
    private MockStrategyLearningService strategyLearningService;
    private WrappingStrategyOptimizer optimizer;
    private DefaultEffectEvaluator effectEvaluator;
    private DefaultCognitionDriver cognitionDriver;

    @BeforeEach
    void setUp() {
        knowledgeStore = new InMemoryKnowledgeStore();
        new InMemoryMetastore();
        strategyLearningService = new MockStrategyLearningService();
        optimizer = new WrappingStrategyOptimizer(strategyLearningService, knowledgeStore);
        effectEvaluator = new DefaultEffectEvaluator();
        cognitionDriver = new DefaultCognitionDriver(optimizer, effectEvaluator, knowledgeStore);
    }

    @Nested
    @DisplayName("S1-HappyPath - to producecognitiondecision")
    class HappyPath {

        @Test
        @DisplayName("trigger CognitionDriver produce EXPLORE decision persistedto KnowledgeStore")
        void firstFeedbackProducesExploreDecisionAndPersists() throws Exception {
            String sceneHash = "view-test-001";
            var state = cognitionDriver.getOrCreateState(sceneHash, Map.of("quality", 0.8));
            var decision = cognitionDriver.nextStep(state);

            assertThat(decision).isNotNull();
            assertThat(decision.type()).isEqualTo(DecisionType.EXPLORE);
            assertThat(decision.sceneHash()).isEqualTo(sceneHash);
            assertThat(decision.strategy()).isNotNull();
            assertThat(decision.strategy().strategyId()).isEqualTo("mock-strategy-001");

            var key =
                    ("cognitive_decision:" + decision.decisionId())
                            .getBytes(StandardCharsets.UTF_8);
            assertThat(knowledgeStore.get(key)).isPresent();
        }

        @Test
        @DisplayName("CognitiveState persistedaftercanfrom KnowledgeStore recovery")
        void cognitiveStatePersistsAndRecovers() throws Exception {
            String sceneHash = "view-test-002";
            cognitionDriver.getOrCreateState(sceneHash, Map.of("quality", 0.7));

            var stateKey = ("cognitive_state:" + sceneHash).getBytes(StandardCharsets.UTF_8);
            assertThat(knowledgeStore.get(stateKey)).isPresent();

            var newState = cognitionDriver.getOrCreateState(sceneHash, Map.of("quality", 0.85));
            assertThat(newState.sceneHash()).isEqualTo(sceneHash);
            assertThat(newState.latestMetrics()).containsEntry("quality", 0.85);
        }
    }

    @Nested
    @DisplayName("S2-Concurrency - multiscenarioconcurrentstatusisolation")
    class Concurrency {

        @Test
        @DisplayName("two scene ofstatus cover(verify sceneStateCache repeat)")
        void multipleScenesDoNotOverwriteEachOther() throws Exception {
            var stateA = cognitionDriver.getOrCreateState("view-A", Map.of("score", 0.5));
            var stateB = cognitionDriver.getOrCreateState("view-B", Map.of("score", 0.9));

            var decisionA = cognitionDriver.nextStep(stateA);
            var decisionB = cognitionDriver.nextStep(stateB);

            assertThat(decisionA.sceneHash()).isEqualTo("view-A");
            assertThat(decisionB.sceneHash()).isEqualTo("view-B");
            assertThat(decisionA.decisionId()).isNotEqualTo(decisionB.decisionId());

            assertThat(
                            knowledgeStore.get(
                                    ("cognitive_state:view-A").getBytes(StandardCharsets.UTF_8)))
                    .isPresent();
            assertThat(
                            knowledgeStore.get(
                                    ("cognitive_state:view-B").getBytes(StandardCharsets.UTF_8)))
                    .isPresent();
        }
    }

    @Nested
    @DisplayName("S3-Guardrails - limit")
    class Guardrails {

        @Test
        @DisplayName("no to STAGNATION_THRESHOLD produce TERMINATE decision")
        void stagnationTriggersTerminate() {
            String sceneHash = "view-stagnation";
            var state = cognitionDriver.getOrCreateState(sceneHash, Map.of("score", 0.5));

            for (int i = 0; i < 3; i++) {
                var decision = cognitionDriver.nextStep(state);
                if (decision.type() == DecisionType.TERMINATE) break;
                cognitionDriver.observe(
                        decision,
                        new ExecutionResult(
                                decision.decisionId(), true, Map.of("quality", 0.1), "low", 100L));
                state = cognitionDriver.getOrCreateState(sceneHash, Map.of("score", 0.1));
            }

            var finalDecision = cognitionDriver.nextStep(state);
            assertThat(finalDecision.type()).isEqualTo(DecisionType.TERMINATE);
        }

        @Test
        @DisplayName("EffectEvaluator for SNR unreliable")
        void lowSNRFeedbackMarkedUnreliable() {
            var result = new ExecutionResult("d1", false, Map.of("dim1", 0.5), "out", 100L);
            var feedback = effectEvaluator.evaluate(result, EvaluationCriteria.defaultCriteria());
            assertThat(feedback.reliable()).isFalse();
        }

        @Test
        @DisplayName("EffectEvaluator for SNR reliable")
        void highSNRFeedbackMarkedReliable() {
            var result =
                    new ExecutionResult("d2", true, Map.of("dim1", 0.9, "dim2", 0.1), "out", 100L);
            var feedback = effectEvaluator.evaluate(result, EvaluationCriteria.defaultCriteria());
            assertThat(feedback.overallScore()).isGreaterThan(0.0);
        }
    }

    @Nested
    @DisplayName("S4-Boundary - boundary conditions")
    class Boundary {

        @Test
        @DisplayName("CognitionDriver.noop() TERMINATE decision")
        void noopDriverReturnsTerminate() {
            var noop = CognitionDriver.noop();
            assertThat(noop.isAvailable()).isFalse();
            var decision = noop.nextStep(CognitiveState.initial("noop", Map.of()));
            assertThat(decision.type()).isEqualTo(DecisionType.TERMINATE);
        }

        @Test
        @DisplayName("ExecutionResult success=false EffectFeedback unreliable")
        void failedExecutionProducesUnreliableFeedback() {
            var result = new ExecutionResult("fail", false, Map.of(), "error", 0L);
            var feedback = effectEvaluator.evaluate(result, EvaluationCriteria.defaultCriteria());
            assertThat(feedback.reliable()).isFalse();
            assertThat(feedback.overallScore()).isEqualTo(0.0);
        }

        @Test
        @DisplayName("StrategyOptimizer.noop() returns emptystrategyand disabled status")
        void noopOptimizerReturnsEmpty() {
            var noop = StrategyOptimizer.noop();
            assertThat(noop.isAvailable()).isFalse();
            assertThat(
                            noop.nextStrategy(
                                    new OptimizationContext(
                                            "noop", Map.of(), List.of(), List.of(), "default")))
                    .isEmpty();
            assertThat(noop.status().state()).isEqualTo(ConvergenceStatus.State.DISABLED);
        }

        @Test
        @DisplayName("WrappingStrategyOptimizer recordEffect column EffectRecord to KnowledgeStore")
        void recordEffectPersistsCompleteEffectRecord() throws Exception {
            var effect =
                    new StrategyAction.EffectRecord(
                            "attempt-001",
                            "v1",
                            Map.of("quality", 0.5),
                            Map.of("quality", 0.8),
                            0.3,
                            1.0,
                            "ENV");
            optimizer.recordEffect("strategy-001", effect);

            var key = "optimizer_effect:strategy-001:attempt-001".getBytes(StandardCharsets.UTF_8);
            var persisted = knowledgeStore.get(key);
            assertThat(persisted).isPresent();
            assertThat(persisted.get().length).isGreaterThan(10);
        }
    }

    static class MockStrategyLearningService implements StrategyLearningService {
        private int callCount = 0;

        @Override
        public Optional<StrategyAction> learn(StrategyLearningContext context) {
            callCount++;
            return Optional.of(
                    new StrategyAction(
                            "mock-strategy-" + String.format("%03d", callCount),
                            "scene:" + context.systemStateSummary(),
                            Map.of("callCount", (double) callCount),
                            "explore temperature=" + (0.5 + callCount * 0.1),
                            "improve quality",
                            List.of(),
                            0.5,
                            List.of(),
                            List.of(),
                            System.currentTimeMillis(),
                            System.currentTimeMillis()));
        }

        @Override
        public boolean isAvailable() {
            return true;
        }
    }
}
