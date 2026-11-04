package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.axiom.memory.SummarizationService;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.metastore.Metastore;
import java.util.Objects;

public record GraphEngineConfig(
        Metastore metastore,
        KnowledgeStore knowledgeStore,
        ContextManager contextManager,
        SummarizationService summarizationService,
        VectorIndex vectorIndex,
        EmbeddingProvider embeddingProvider,
        KnowledgeExtractor knowledgeExtractor,
        GraphTrustAdapter trustAdapter,
        GraphStateAdapter stateAdapter) {
    public GraphEngineConfig {
        Objects.requireNonNull(metastore, "metastore must not be null");
        Objects.requireNonNull(knowledgeStore, "knowledgeStore must not be null");
        Objects.requireNonNull(contextManager, "contextManager must not be null");
        Objects.requireNonNull(vectorIndex, "vectorIndex must not be null");
        Objects.requireNonNull(embeddingProvider, "embeddingProvider must not be null");
        if (knowledgeExtractor == null) {
            knowledgeExtractor = new NoOpKnowledgeExtractor();
        }
    }

    public GraphEngineConfig(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            ContextManager contextManager,
            SummarizationService summarizationService,
            VectorIndex vectorIndex,
            EmbeddingProvider embeddingProvider,
            KnowledgeExtractor knowledgeExtractor,
            GraphTrustAdapter trustAdapter) {
        this(
                metastore,
                knowledgeStore,
                contextManager,
                summarizationService,
                vectorIndex,
                embeddingProvider,
                knowledgeExtractor,
                trustAdapter,
                null);
    }
}
