package io.github.rawwise.axiom.memory.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.github.rawwise.axiom.memory.NoOpSummarizationService;
import io.github.rawwise.axiom.memory.SummarizationService;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.InMemoryVectorIndex;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.governance.context.ContextEntry;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.governance.context.DefaultContextManager;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * E2E test for P1.3 — KnowledgeExtractorFactory.
 *
 * <p>Validates that system-property driven factory creates the correct {@link KnowledgeExtractor}
 * implementation, degrades safely on misconfiguration, and that the extractor is wired correctly to
 * the supplied GraphEngine + SummarizationService dependencies.
 */
class KnowledgeExtractorFactoryE2ETest {

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private ContextManager contextManager;
    private GraphEngine graphEngine;
    private VectorIndex vectorIndex;
    private EmbeddingProvider embeddingProvider;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        contextManager = new DefaultContextManager(metastore, 1, 1);
        embeddingProvider = text -> new float[1];
        vectorIndex = new InMemoryVectorIndex(embeddingProvider);
        GraphEngineConfig config =
                new GraphEngineConfig(
                        metastore,
                        knowledgeStore,
                        contextManager,
                        new NoOpSummarizationService(),
                        vectorIndex,
                        embeddingProvider,
                        new NoOpKnowledgeExtractor(),
                        null);
        graphEngine = new DefaultGraphEngine(config);
        clearProperties();
    }

    @AfterEach
    void tearDown() throws Exception {
        clearProperties();
        if (contextManager instanceof DefaultContextManager dcm) {
            dcm.close();
        }
        knowledgeStore.close();
        metastore.close();
    }

    private static void clearProperties() {
        System.clearProperty(KnowledgeExtractorFactory.KEY_TYPE);
        System.clearProperty(KnowledgeExtractorFactory.KEY_ENDPOINT);
        System.clearProperty(KnowledgeExtractorFactory.KEY_MODEL);
        System.clearProperty(KnowledgeExtractorFactory.KEY_API_KEY);
    }

    @Test
    void defaultType_IsNoOp_ExtractReturnsEmptyResult() throws Exception {
        SummarizationService summarization = new NoOpSummarizationService();
        KnowledgeExtractor extractor =
                KnowledgeExtractorFactory.createFromSystemProperties(graphEngine, summarization);

        assertNotNull(extractor);
        assertInstanceOf(NoOpKnowledgeExtractor.class, extractor);
        assertFalse(extractor.isAvailable(), "NoOp extractor must report unavailable");

        GraphSchema schema =
                graphEngine.createGraph(
                        "agent-noop",
                        "g-noop",
                        "noop test",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        ExtractionResult result =
                extractor.extract(
                        "agent-noop",
                        schema.graphId(),
                        List.of(buildEntry("e1", "hello world")),
                        ExtractionPolicy.DEFAULT);

        assertNotNull(result);
        assertEquals(0, result.nodesExtracted(), "NoOp must extract zero nodes");
        assertEquals(0, result.edgesExtracted(), "NoOp must extract zero edges");
    }

    @Test
    void typeLlm_WithDedicatedInvalidEndpoint_CreatesLlmExtractor() throws Exception {
        // type=llm + dedicated endpoint+model -> should construct LlmKnowledgeExtractor.
        // We use an unreachable endpoint so the construction succeeds but extract() will fail
        // through the circuit breaker / retry path; still a valid LlmKnowledgeExtractor instance.
        System.setProperty(KnowledgeExtractorFactory.KEY_TYPE, "llm");
        System.setProperty(
                KnowledgeExtractorFactory.KEY_ENDPOINT, "http://127.0.0.1:1/v1/chat/completions");
        System.setProperty(KnowledgeExtractorFactory.KEY_MODEL, "deepseek-chat-test");

        KnowledgeExtractor extractor =
                KnowledgeExtractorFactory.createFromSystemProperties(
                        graphEngine, new NoOpSummarizationService());

        assertNotNull(extractor);
        assertInstanceOf(
                LlmKnowledgeExtractor.class,
                extractor,
                "type=llm with valid endpoint+model must produce LlmKnowledgeExtractor");

        // Calling extract() against an unreachable server must not throw uncaught;
        // LlmKnowledgeExtractor's retry+circuit returns null response and produces
        // a zero-result ExtractionResult (filtered.isEmpty() path is not hit because
        // we provide a non-empty entry, so we go through the retry path which returns null).
        GraphSchema schema =
                graphEngine.createGraph(
                        "agent-llm",
                        "g-llm",
                        "llm test",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        ExtractionResult result =
                extractor.extract(
                        "agent-llm",
                        schema.graphId(),
                        List.of(buildEntry("e1", "the quick brown fox")),
                        ExtractionPolicy.DEFAULT);
        assertNotNull(
                result, "LlmKnowledgeExtractor must not return null even on transport failure");
        assertEquals(0, result.nodesExtracted());
        assertEquals(0, result.edgesExtracted());
    }

    @Test
    void typeLlm_WithoutAnyConfig_FallsBackToNoOp() {
        System.setProperty(KnowledgeExtractorFactory.KEY_TYPE, "llm");
        // No dedicated endpoint/model configured AND shared summarizationService is NoOp -> noop
        // fallback

        KnowledgeExtractor extractor =
                KnowledgeExtractorFactory.createFromSystemProperties(
                        graphEngine, new NoOpSummarizationService());

        assertNotNull(extractor);
        assertInstanceOf(
                NoOpKnowledgeExtractor.class,
                extractor,
                "type=llm with no usable LLM client must fall back to NoOp");
    }

    @Test
    void invalidType_FallsBackToNoOpAndWarns() {
        System.setProperty(KnowledgeExtractorFactory.KEY_TYPE, "invalid-type-xyz");

        KnowledgeExtractor extractor =
                KnowledgeExtractorFactory.createFromSystemProperties(
                        graphEngine, new NoOpSummarizationService());

        assertNotNull(extractor);
        assertInstanceOf(
                NoOpKnowledgeExtractor.class, extractor, "Unknown type must fall back to NoOp");
    }

    @Test
    void typeLlm_NullGraphEngine_FallsBackToNoOp() {
        System.setProperty(KnowledgeExtractorFactory.KEY_TYPE, "llm");
        System.setProperty(
                KnowledgeExtractorFactory.KEY_ENDPOINT, "http://127.0.0.1:1/v1/chat/completions");
        System.setProperty(KnowledgeExtractorFactory.KEY_MODEL, "deepseek-chat-test");

        KnowledgeExtractor extractor =
                KnowledgeExtractorFactory.createFromSystemProperties(
                        null, new NoOpSummarizationService());

        assertNotNull(extractor);
        assertInstanceOf(
                NoOpKnowledgeExtractor.class,
                extractor,
                "Null graphEngine must force fallback to NoOp");
    }

    private static ContextEntry buildEntry(String key, String body) {
        return ContextEntry.builder(key, body)
                .contentType("text/plain")
                .importance(8)
                .l0Abstract(body)
                .l1Overview(body)
                .build();
    }
}
