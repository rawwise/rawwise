package io.github.rawwise.axiom.pipeline.cognition;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.reformulation.LlmClient;
import io.github.rawwise.axiom.pipeline.reformulation.LlmException;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.model.record.StrategyAction.EffectRecord;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ActionGenerationStrategy contract test - §6.2 Action generatestrategy abstraction")
class ActionGenerationStrategyContractTest {

    @Nested
    @DisplayName("RuleActionStrategy - rule-based(rule-based)")
    class RuleActionStrategyTests {

        @Test
        @DisplayName(
                "buildContent based onthenfrom semantic insidegenerate action,returns non-emptyinside")
        void buildContentGeneratesRuleBasedAction() {
            RuleActionStrategy strategy = new RuleActionStrategy();
            Map<String, byte[]> semantics =
                    Map.of(
                            "s1",
                            "{\"semanticType\":\"CONCEPT\",\"concepts\":[{\"term\":\"ml\"}]}"
                                    .getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(semantics);

            byte[] content = strategy.buildContent(List.of("s1"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
            String json = new String(content, StandardCharsets.UTF_8);
            assertThat(json)
                    .as("规thengeneratesucceed's action contentshould包contains action 类型标识")
                    .containsAnyOf(
                            "RETRIEVE",
                            "TRANSFORM",
                            "NOTIFY",
                            "CORRECT",
                            "retrieve",
                            "transform",
                            "notify",
                            "correct",
                            "actionType",
                            "action");
        }

        @Test
        @DisplayName(
                "buildContent when semantic not foundwhenstillreturns non-null inside(fault tolerance)")
        void buildContentWithMissingSemanticReturnsNonNull() {
            RuleActionStrategy strategy = new RuleActionStrategy();
            KnowledgeDataAccessor accessor = fakeAccessor(Map.of());

            byte[] content = strategy.buildContent(List.of("nonexistent"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("LlmPlanStrategy - LLM plan (llm-plan, mandatory CircuitBreaker)")
    class LlmPlanStrategyTests {

        @Test
        @DisplayName("CircuitBreaker OPEN whennotcall LLM,returnsdegradation fallback inside")
        void circuitBreakerOpenSkipsLlmAndReturnsFallback() {
            StubLlmClient llmClient = new StubLlmClient("LLM_PLAN_RESULT");
            AlwaysOpenBreaker openBreaker = new AlwaysOpenBreaker();
            LlmPlanStrategy strategy = new LlmPlanStrategy(llmClient, openBreaker);

            Map<String, byte[]> semantics =
                    Map.of("s1", "{\"semanticType\":\"CONCEPT\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(semantics);

            byte[] content = strategy.buildContent(List.of("s1"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
            assertThat(llmClient.callCount())
                    .as("CircuitBreaker OPEN whennotshould调use LLM")
                    .isEqualTo(0);
            String result = new String(content, StandardCharsets.UTF_8);
            assertThat(result)
                    .as("降级contentnotshould包contains LLM 计划结indeed")
                    .doesNotContain("LLM_PLAN_RESULT");
        }

        @Test
        @DisplayName("CircuitBreaker CLOSED and LLM success returns LLM plan content")
        void circuitBreakerClosedAndLlmSuccessReturnsLlmPlan() {
            StubLlmClient llmClient = new StubLlmClient("LLM_PLAN_RESULT");
            AlwaysClosedBreaker closedBreaker = new AlwaysClosedBreaker();
            LlmPlanStrategy strategy = new LlmPlanStrategy(llmClient, closedBreaker);

            Map<String, byte[]> semantics =
                    Map.of("s1", "{\"semanticType\":\"CONCEPT\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(semantics);

            byte[] content = strategy.buildContent(List.of("s1"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
            assertThat(llmClient.callCount())
                    .as("CircuitBreaker CLOSED whenshould调use LLM")
                    .isEqualTo(1);
            String result = new String(content, StandardCharsets.UTF_8);
            assertThat(result)
                    .as("should包contains LLM generatesucceed's计划content")
                    .contains("LLM_PLAN_RESULT");
        }

        @Test
        @DisplayName(
                "CircuitBreaker CLOSED but LLM exceptionwhen,returnsdegraded contentandrecordfailure")
        void circuitBreakerClosedButLlmThrowsReturnsFallback() {
            ThrowingLlmClient throwingClient = new ThrowingLlmClient();
            TrackingCircuitBreaker tracker = new TrackingCircuitBreaker(true);
            LlmPlanStrategy strategy = new LlmPlanStrategy(throwingClient, tracker);

            Map<String, byte[]> semantics =
                    Map.of("s1", "{\"semanticType\":\"CONCEPT\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(semantics);

            byte[] content = strategy.buildContent(List.of("s1"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
            assertThat(throwingClient.callCount()).as("LLM shouldbe调use过onetimes").isEqualTo(1);
            assertThat(tracker.failureCount())
                    .as("CircuitBreaker shouldrecord录onetimeslosefail")
                    .isGreaterThanOrEqualTo(1);
        }
    }

    @Nested
    @DisplayName("HistoryCalibratedStrategy - history(history-calibrated)")
    class HistoryCalibratedStrategyTests {

        @Test
        @DisplayName(
                "buildContentWithHistory usehistory EffectRecord output,andnohistorywhendifferent")
        void buildContentWithHistoryUsesHistory() {
            HistoryCalibratedStrategy strategy = new HistoryCalibratedStrategy();
            Map<String, byte[]> semantics =
                    Map.of("s1", "{\"semanticType\":\"CONCEPT\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(semantics);

            List<EffectRecord> effectiveHistory =
                    List.of(
                            new EffectRecord(
                                    "att1",
                                    "v1",
                                    Map.of("score", 0.2),
                                    Map.of("score", 0.9),
                                    0.7,
                                    0.95,
                                    "HUMAN"));

            byte[] withoutHistory = strategy.buildContent(List.of("s1"), Map.of(), accessor);
            byte[] withHistory =
                    strategy.buildContentWithHistory(
                            List.of("s1"), Map.of(), accessor, effectiveHistory);

            assertThat(withHistory).isNotNull();
            assertThat(withHistory).isNotEmpty();
            assertThat(withHistory)
                    .as("has历史whenshould调整输出，andno历史whennotsame")
                    .isNotEqualTo(withoutHistory);
        }

        @Test
        @DisplayName("buildContentWithHistory emptyhistorywhenretreatis buildContent rowis")
        void buildContentWithHistoryEmptyDegradesToBuildContent() {
            HistoryCalibratedStrategy strategy = new HistoryCalibratedStrategy();
            Map<String, byte[]> semantics =
                    Map.of("s1", "{\"semanticType\":\"CONCEPT\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(semantics);

            byte[] fromBuildContent = strategy.buildContent(List.of("s1"), Map.of(), accessor);
            byte[] fromWithHistory =
                    strategy.buildContentWithHistory(List.of("s1"), Map.of(), accessor, List.of());

            assertThat(fromWithHistory)
                    .as("empty历史whenshouldand buildContent executeisconsistent")
                    .isEqualTo(fromBuildContent);
        }

        @Test
        @DisplayName("buildContentWithHistory rootevidencehistoryvalidparameter")
        void buildContentWithHistoryAdjustsByEffectiveness() {
            HistoryCalibratedStrategy strategy = new HistoryCalibratedStrategy();
            Map<String, byte[]> semantics =
                    Map.of("s1", "{\"semanticType\":\"CONCEPT\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(semantics);

            List<EffectRecord> allEffective =
                    List.of(
                            new EffectRecord(
                                    "att1",
                                    "v1",
                                    Map.of("score", 0.1),
                                    Map.of("score", 0.9),
                                    0.8,
                                    0.95,
                                    "HUMAN"));
            List<EffectRecord> allIneffective =
                    List.of(
                            new EffectRecord(
                                    "att2",
                                    "v1",
                                    Map.of("score", 0.9),
                                    Map.of("score", 0.1),
                                    -0.8,
                                    0.05,
                                    "HUMAN"));

            byte[] effectiveResult =
                    strategy.buildContentWithHistory(
                            List.of("s1"), Map.of(), accessor, allEffective);
            byte[] ineffectiveResult =
                    strategy.buildContentWithHistory(
                            List.of("s1"), Map.of(), accessor, allIneffective);

            assertThat(effectiveResult).isNotEmpty();
            assertThat(ineffectiveResult).isNotEmpty();
            assertThat(effectiveResult)
                    .as("allhas效历史 vs allno效历史shouldproducenotsame调整结indeed")
                    .isNotEqualTo(ineffectiveResult);
        }
    }

    @Nested
    @DisplayName("MultiArmStrategy - many(multi-arm)")
    class MultiArmStrategyTests {

        @Test
        @DisplayName(
                "buildContent selects action arm from semantic content, returns non-empty content")
        void buildContentSelectsArm() {
            MultiArmStrategy strategy = new MultiArmStrategy();
            Map<String, byte[]> semantics =
                    Map.of("s1", "{\"semanticType\":\"CONCEPT\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(semantics);

            byte[] content = strategy.buildContent(List.of("s1"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
        }
    }

    private static KnowledgeDataAccessor fakeAccessor(Map<String, byte[]> semantics) {
        InMemoryMetastore metastore = new InMemoryMetastore();
        InMemoryKnowledgeStore knowledgeStore = new InMemoryKnowledgeStore();
        return new KnowledgeDataAccessor(metastore, knowledgeStore) {
            @Override
            public Optional<byte[]> getWithFallback(byte[] key) {
                String keyStr = new String(key, StandardCharsets.UTF_8);
                if (keyStr.startsWith("semantic:") && semantics != null) {
                    String semId = keyStr.substring("semantic:".length());
                    if (semantics.containsKey(semId)) {
                        return Optional.of(semantics.get(semId));
                    }
                }
                return Optional.empty();
            }
        };
    }

    private static final class StubLlmClient implements LlmClient {
        private final String response;
        private final AtomicInteger callCount = new AtomicInteger(0);

        StubLlmClient(String response) {
            this.response = response;
        }

        @Override
        public String name() {
            return "stub-llm";
        }

        @Override
        public String complete(String prompt, String modelName, int maxTokens) {
            callCount.incrementAndGet();
            return response;
        }

        int callCount() {
            return callCount.get();
        }
    }

    private static final class ThrowingLlmClient implements LlmClient {
        private final AtomicInteger callCount = new AtomicInteger(0);

        @Override
        public String name() {
            return "throwing-llm";
        }

        @Override
        public String complete(String prompt, String modelName, int maxTokens) throws LlmException {
            callCount.incrementAndGet();
            throw new LlmException("simulated LLM failure");
        }

        int callCount() {
            return callCount.get();
        }
    }

    private static final class AlwaysClosedBreaker implements CircuitBreaker {
        @Override
        public boolean allowRequest() {
            return true;
        }

        @Override
        public void recordSuccess() {}

        @Override
        public void recordFailure() {}

        @Override
        public State getState() {
            return State.CLOSED;
        }
    }

    private static final class AlwaysOpenBreaker implements CircuitBreaker {
        @Override
        public boolean allowRequest() {
            return false;
        }

        @Override
        public void recordSuccess() {}

        @Override
        public void recordFailure() {}

        @Override
        public State getState() {
            return State.OPEN;
        }
    }

    private static final class TrackingCircuitBreaker implements CircuitBreaker {
        private final boolean allow;
        private final AtomicInteger failureCount = new AtomicInteger(0);

        TrackingCircuitBreaker(boolean allow) {
            this.allow = allow;
        }

        @Override
        public boolean allowRequest() {
            return allow;
        }

        @Override
        public void recordSuccess() {}

        @Override
        public void recordFailure() {
            failureCount.incrementAndGet();
        }

        @Override
        public State getState() {
            return allow ? State.CLOSED : State.OPEN;
        }

        int failureCount() {
            return failureCount.get();
        }
    }
}
