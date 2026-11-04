package io.github.rawwise.axiom.pipeline.cognition;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.reformulation.LlmClient;
import io.github.rawwise.axiom.pipeline.reformulation.LlmException;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName(
        "SemanticGenerationStrategy contract test - Section 5.2 Semantic generation strategy abstraction")
class SemanticGenerationStrategyContractTest {

    @Nested
    @DisplayName("TfConceptStrategy - TF take(tf-concept)")
    class TfConceptStrategyTests {

        @Test
        @DisplayName(
                "buildContent extracts concepts by TF from view content, high-frequency terms should appear in output")
        void buildContentExtractsTermsByFrequency() {
            TfConceptStrategy strategy = new TfConceptStrategy();
            Map<String, byte[]> views =
                    Map.of(
                            "v1",
                                    "{\"text\":\"machine learning machine learning deep\"}"
                                            .getBytes(StandardCharsets.UTF_8),
                            "v2",
                                    "{\"text\":\"machine data data data\"}"
                                            .getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(views);

            byte[] content = strategy.buildContent(List.of("v1", "v2"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();

            String json = new String(content, StandardCharsets.UTF_8);
            assertThat(json)
                    .as("high频词 'machine' (3times) should出现in概念抽取结indeedin")
                    .contains("machine");
            assertThat(json).as("high频词 'data' (3times) should出现in概念抽取结indeedin").contains("data");
        }

        @Test
        @DisplayName(
                "buildContent when view not foundwhenstillreturns non-null inside(fault tolerance)")
        void buildContentWithMissingViewReturnsNonNull() {
            TfConceptStrategy strategy = new TfConceptStrategy();
            KnowledgeDataAccessor accessor = fakeAccessor(Map.of());

            byte[] content = strategy.buildContent(List.of("nonexistent"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("LlmExtractStrategy - LLM semantictake(llm-extract,strong CircuitBreaker)")
    class LlmExtractStrategyTests {

        @Test
        @DisplayName("CircuitBreaker OPEN whennotcall LLM,returnsdegradation fallback inside")
        void circuitBreakerOpenSkipsLlmAndReturnsFallback() {
            StubLlmClient llmClient = new StubLlmClient("LLM_EXTRACTED_RESULT");
            AlwaysOpenBreaker openBreaker = new AlwaysOpenBreaker();
            LlmExtractStrategy strategy = new LlmExtractStrategy(llmClient, openBreaker);

            Map<String, byte[]> views =
                    Map.of("v1", "{\"text\":\"sample content\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(views);

            byte[] content = strategy.buildContent(List.of("v1"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
            assertThat(llmClient.callCount())
                    .as("CircuitBreaker OPEN whennotshould调use LLM")
                    .isEqualTo(0);
            String result = new String(content, StandardCharsets.UTF_8);
            assertThat(result)
                    .as("降级contentnotshould包contains LLM 抽取结indeed")
                    .doesNotContain("LLM_EXTRACTED_RESULT");
        }

        @Test
        @DisplayName("CircuitBreaker CLOSED and LLM on success,returns LLM takeinside")
        void circuitBreakerClosedAndLlmSuccessReturnsLlmContent() {
            StubLlmClient llmClient = new StubLlmClient("LLM_EXTRACTED_RESULT");
            AlwaysClosedBreaker closedBreaker = new AlwaysClosedBreaker();
            LlmExtractStrategy strategy = new LlmExtractStrategy(llmClient, closedBreaker);

            Map<String, byte[]> views =
                    Map.of("v1", "{\"text\":\"sample content\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(views);

            byte[] content = strategy.buildContent(List.of("v1"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
            assertThat(llmClient.callCount())
                    .as("CircuitBreaker CLOSED whenshould调use LLM")
                    .isEqualTo(1);
            String result = new String(content, StandardCharsets.UTF_8);
            assertThat(result)
                    .as("should包contains LLM 抽取'scontent")
                    .contains("LLM_EXTRACTED_RESULT");
        }

        @Test
        @DisplayName(
                "CircuitBreaker CLOSED but LLM exceptionwhen,returnsdegraded contentandrecordfailure")
        void circuitBreakerClosedButLlmThrowsReturnsFallback() {
            ThrowingLlmClient throwingClient = new ThrowingLlmClient();
            TrackingCircuitBreaker tracker = new TrackingCircuitBreaker(true);
            LlmExtractStrategy strategy = new LlmExtractStrategy(throwingClient, tracker);

            Map<String, byte[]> views =
                    Map.of("v1", "{\"text\":\"content\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(views);

            byte[] content = strategy.buildContent(List.of("v1"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
            assertThat(throwingClient.callCount()).as("LLM shouldbe调use过onetimes").isEqualTo(1);
            assertThat(tracker.failureCount())
                    .as("CircuitBreaker shouldrecord录onetimeslosefail")
                    .isGreaterThanOrEqualTo(1);
        }
    }

    @Nested
    @DisplayName("GraphMergeStrategy - merge(graph-merge)")
    class GraphMergeStrategyTests {

        @Test
        @DisplayName("buildContent frommany view intakeclosesystem/schema,returns non-emptyinside")
        void buildContentExtractsRelations() {
            GraphMergeStrategy strategy = new GraphMergeStrategy();
            Map<String, byte[]> views =
                    Map.of(
                            "v1",
                                    "{\"entity\":\"A\",\"text\":\"A relates to B\"}"
                                            .getBytes(StandardCharsets.UTF_8),
                            "v2",
                                    "{\"entity\":\"B\",\"text\":\"B connects to C\"}"
                                            .getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(views);

            byte[] content = strategy.buildContent(List.of("v1", "v2"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("HybridSemanticStrategy - Hybrid strategy (hybrid, fusing LLM + graph + TF)")
    class HybridSemanticStrategyTests {

        @Test
        @DisplayName("buildContent fuses LLM and TF signals, output contains features from both")
        void buildContentFusesLlmAndTf() {
            StubLlmClient llmClient = new StubLlmClient("LLM_SIGNAL_MARKER");
            AlwaysClosedBreaker breaker = new AlwaysClosedBreaker();
            HybridSemanticStrategy strategy = new HybridSemanticStrategy(llmClient, breaker);

            Map<String, byte[]> views =
                    Map.of(
                            "v1",
                            "{\"text\":\"keyword keyword keyword\"}"
                                    .getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(views);

            byte[] content = strategy.buildContent(List.of("v1"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
            String result = new String(content, StandardCharsets.UTF_8);
            assertThat(result).as("should包contains LLM 路truly号").contains("LLM_SIGNAL_MARKER");
            assertThat(result).as("should包contains TF 路抽取'shigh频词 'keyword'").contains("keyword");
        }

        @Test
        @DisplayName("CircuitBreaker OPEN when LLM calldegradation,TF stillproduceinside")
        void circuitBreakerOpenDegradesLlmButTfStillWorks() {
            StubLlmClient llmClient = new StubLlmClient("LLM_SIGNAL");
            AlwaysOpenBreaker openBreaker = new AlwaysOpenBreaker();
            HybridSemanticStrategy strategy = new HybridSemanticStrategy(llmClient, openBreaker);

            Map<String, byte[]> views =
                    Map.of(
                            "v1",
                            "{\"text\":\"fallback fallback\"}".getBytes(StandardCharsets.UTF_8));
            KnowledgeDataAccessor accessor = fakeAccessor(views);

            byte[] content = strategy.buildContent(List.of("v1"), Map.of(), accessor);

            assertThat(content).isNotNull();
            assertThat(content).isNotEmpty();
            assertThat(llmClient.callCount())
                    .as("CB OPEN when LLM 子调useshould降级，not调use LLM")
                    .isEqualTo(0);
        }
    }

    private static KnowledgeDataAccessor fakeAccessor(Map<String, byte[]> views) {
        InMemoryMetastore metastore = new InMemoryMetastore();
        InMemoryKnowledgeStore knowledgeStore = new InMemoryKnowledgeStore();
        return new KnowledgeDataAccessor(metastore, knowledgeStore) {
            @Override
            public Optional<byte[]> readView(String viewId) {
                if (views != null && views.containsKey(viewId)) {
                    return Optional.of(views.get(viewId));
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
