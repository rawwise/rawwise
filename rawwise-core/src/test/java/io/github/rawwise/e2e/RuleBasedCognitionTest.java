package io.github.rawwise.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.cognition.CognitionDriver;
import io.github.rawwise.axiom.pipeline.cognition.CognitiveDecision;
import io.github.rawwise.axiom.pipeline.cognition.CognitiveState;
import io.github.rawwise.axiom.pipeline.cognition.DecisionType;
import io.github.rawwise.axiom.pipeline.cognition.DefaultCognitionDriver;
import io.github.rawwise.axiom.pipeline.cognition.DefaultEffectEvaluator;
import io.github.rawwise.axiom.pipeline.cognition.SimpleStrategyOptimizer;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.model.record.StrategyAction;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("RuleBasedCognition - rule cognition end-to-end test")
class RuleBasedCognitionTest {

    private KnowledgeStore knowledgeStore;
    private SimpleStrategyOptimizer optimizer;
    private DefaultEffectEvaluator effectEvaluator;
    private DefaultCognitionDriver cognitionDriver;

    @BeforeEach
    void setUp() {
        knowledgeStore = new InMemoryKnowledgeStore();
        optimizer = new SimpleStrategyOptimizer(knowledgeStore);
        effectEvaluator = new DefaultEffectEvaluator();
        cognitionDriver =
                (DefaultCognitionDriver)
                        CognitionDriver.ruleBased(optimizer, effectEvaluator, knowledgeStore);
    }

    @AfterEach
    void tearDown() {
        if (knowledgeStore != null) {
            try {
                knowledgeStore.close();
            } catch (Exception e) {
                /* tearDown best-effort */
            }
        }
    }

    @Nested
    @DisplayName("S1-HappyPath - no LLM environment rule cognition")
    class HappyPath {

        @Test
        @DisplayName("trigger CognitionDriver produce EXPLORE decision persistedto KnowledgeStore")
        void firstFeedbackProducesExploreDecisionAndPersists() throws Exception {
            String sceneHash = "view-rule-001";
            CognitiveState state =
                    cognitionDriver.getOrCreateState(sceneHash, Map.of("quality", 0.8));

            CognitiveDecision decision = cognitionDriver.nextStep(state);

            assertThat(decision).isNotNull();
            assertThat(decision.type()).isEqualTo(DecisionType.EXPLORE);
            assertThat(decision.sceneHash()).isEqualTo(sceneHash);
            assertThat(decision.strategy()).isNotNull();
            assertThat(decision.strategy().strategyId()).startsWith("rule:");

            byte[] key =
                    ("cognitive_decision:" + decision.decisionId())
                            .getBytes(StandardCharsets.UTF_8);
            assertThat(knowledgeStore.get(key)).isPresent();
        }

        @Test
        @DisplayName("after SimpleStrategyOptimizer record updatestrategyscore")
        void positiveFeedbackRecordsEffectAndUpdatesScore() throws Exception {
            String sceneHash = "view-rule-002";
            CognitiveState state =
                    cognitionDriver.getOrCreateState(sceneHash, Map.of("recall", 0.75));

            CognitiveDecision decision = cognitionDriver.nextStep(state);
            assertThat(decision.type()).isNotEqualTo(DecisionType.TERMINATE);

            StrategyAction strategy = decision.strategy();
            var effect =
                    new StrategyAction.EffectRecord(
                            "attempt-001",
                            "v1",
                            Map.of("recall", 0.75),
                            Map.of("recall", 0.85),
                            0.10,
                            0.85,
                            "HUMAN");
            optimizer.recordEffect(strategy.strategyId(), effect);

            var status = optimizer.status();
            assertThat(status.roundsCompleted()).isGreaterThanOrEqualTo(1);
            assertThat(status.averageRegret()).isLessThanOrEqualTo(1.0);

            byte[] effectKey =
                    ("simple_opt_effect:" + strategy.strategyId() + ":attempt-001")
                            .getBytes(StandardCharsets.UTF_8);
            assertThat(knowledgeStore.get(effectKey)).isPresent();
        }

        @Test
        @DisplayName("CognitionDriver.isAvailable inno LLM environment true")
        void isAvailableReturnsTrueWithoutLlm() {
            assertThat(cognitionDriver.isAvailable()).isTrue();
            assertThat(optimizer.isAvailable()).isTrue();
        }

        @Test
        @DisplayName(
                "multi after CognitionDriver producenon- TERMINATE decision to guardrail trigger")
        void multipleFeedbacksProduceNonTerminateUntilGuardrail() {
            String sceneHash = "view-rule-003";
            CognitiveState state =
                    cognitionDriver.getOrCreateState(sceneHash, Map.of("score", 0.5));

            int nonTerminateCount = 0;
            for (int i = 0; i < 5; i++) {
                CognitiveDecision decision = cognitionDriver.nextStep(state);
                if (decision.type() != DecisionType.TERMINATE) {
                    nonTerminateCount++;
                    state =
                            state.recordExplore(
                                    decision.decisionId(),
                                    0.6 + i * 0.05,
                                    decision.strategy().strategyId());
                    var effect =
                            new StrategyAction.EffectRecord(
                                    "attempt-" + i,
                                    "v" + i,
                                    Map.of("score", 0.5 + i * 0.05),
                                    Map.of("score", 0.6 + i * 0.05),
                                    0.05,
                                    0.6 + i * 0.05,
                                    "SELF");
                    optimizer.recordEffect(decision.strategy().strategyId(), effect);
                }
            }
            assertThat(nonTerminateCount).isGreaterThanOrEqualTo(3);
        }
    }

    @Nested
    @DisplayName("S4-Boundary - guardrail boundary conditions")
    class Boundary {

        @Test
        @DisplayName("to after CognitionDriver produce TERMINATE decision")
        void maxExploreReachedProducesTerminate() {
            String sceneHash = "view-rule-boundary-001";
            CognitiveState exhausted =
                    new CognitiveState(
                            sceneHash,
                            20,
                            0,
                            0,
                            0.9,
                            "rule:rerank-enable",
                            List.of(),
                            Map.of("recall", 0.9),
                            System.currentTimeMillis());

            CognitiveDecision decision = cognitionDriver.nextStep(exhausted);

            assertThat(decision).isNotNull();
            assertThat(decision.type()).isEqualTo(DecisionType.TERMINATE);
            assertThat(decision.strategy()).isNull();
            assertThat(decision.rationale()).contains("Max explore rounds");
        }

        @Test
        @DisplayName("tothresholdafter CognitionDriver produce TERMINATE decision")
        void stagnationThresholdProducesTerminate() {
            String sceneHash = "view-rule-boundary-002";
            CognitiveState stagnant =
                    new CognitiveState(
                            sceneHash,
                            5,
                            0,
                            3,
                            0.5,
                            "rule:chunk-size-up",
                            List.of(),
                            Map.of("recall", 0.5),
                            System.currentTimeMillis());

            CognitiveDecision decision = cognitionDriver.nextStep(stagnant);

            assertThat(decision).isNotNull();
            assertThat(decision.type()).isEqualTo(DecisionType.TERMINATE);
            assertThat(decision.rationale()).contains("Stagnation");
        }
    }
}
