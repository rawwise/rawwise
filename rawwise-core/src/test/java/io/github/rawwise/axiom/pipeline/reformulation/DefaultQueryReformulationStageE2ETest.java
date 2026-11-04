package io.github.rawwise.axiom.pipeline.reformulation;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("DefaultQueryReformulationStage E2E - LLM expand + Caffeine cache + degraded path")
class DefaultQueryReformulationStageE2ETest {

    private DefaultQueryReformulationStage stage;
    private CountingLlmClient countingClient;
    private ReformulationConfig config;

    @BeforeEach
    void setUp() {
        countingClient = new CountingLlmClient(new StubLlmClient());
        config =
                ReformulationConfig.builder()
                        .enabled(true)
                        .expandN(4)
                        .cacheTtl(Duration.ofMinutes(10))
                        .cacheSize(100)
                        .modelName("stub-model")
                        .maxTokens(200)
                        .build();
        stage = new DefaultQueryReformulationStage(countingClient, config);
    }

    @AfterEach
    void tearDown() {
        if (stage != null) stage.invalidateAll();
    }

    @Test
    @DisplayName("expand returns N paraphrases from stub LLM")
    void expandReturnsParaphrases() {
        List<String> result = stage.expand("test query", 4);
        assertThat(result).hasSize(4);
    }

    @Test
    @DisplayName("repeated expand with same args -> cache hit, no extra LLM calls")
    void cacheHitOnRepeat() {
        stage.expand("test query", 4);
        int firstCalls = countingClient.callCount();
        assertThat(firstCalls).isEqualTo(1);

        stage.expand("test query", 4);
        stage.expand("test query", 4);
        assertThat(countingClient.callCount()).isEqualTo(firstCalls);
    }

    @Test
    @DisplayName("invalidate evicts cache; next expand re-invokes LLM")
    void invalidateEvictsCache() {
        stage.expand("test query", 4);
        int before = countingClient.callCount();
        stage.invalidate("test query");
        stage.expand("test query", 4);
        assertThat(countingClient.callCount()).isEqualTo(before + 1);
    }

    @Test
    @DisplayName(
            "LlmException from client -> degraded path returns single-element list with original query")
    void llmExceptionFallsBackToOriginal() {
        LlmClient throwing =
                new LlmClient() {
                    @Override
                    public String name() {
                        return "throwing";
                    }

                    @Override
                    public String complete(String prompt, String modelName, int maxTokens)
                            throws LlmException {
                        throw new LlmException("simulated failure");
                    }
                };
        DefaultQueryReformulationStage degraded =
                new DefaultQueryReformulationStage(throwing, config);
        List<String> result = degraded.expand("q", 4);
        assertThat(result).containsExactly("q");
    }

    @Test
    @DisplayName("blank query -> empty list (no LLM call)")
    void blankQueryReturnsEmpty() {
        List<String> result = stage.expand("", 4);
        assertThat(result).isEmpty();
        assertThat(countingClient.callCount()).isEqualTo(0);
    }

    private static final class CountingLlmClient implements LlmClient {
        private final LlmClient delegate;
        private final AtomicInteger counter = new AtomicInteger();

        CountingLlmClient(LlmClient delegate) {
            this.delegate = delegate;
        }

        @Override
        public String name() {
            return "counting:" + delegate.name();
        }

        @Override
        public String complete(String prompt, String modelName, int maxTokens) throws LlmException {
            counter.incrementAndGet();
            return delegate.complete(prompt, modelName, maxTokens);
        }

        int callCount() {
            return counter.get();
        }
    }
}
