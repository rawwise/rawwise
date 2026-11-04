package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.axiom.memory.NoOpSummarizationService;
import io.github.rawwise.axiom.memory.SummarizationService;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.InMemoryVectorIndex;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory that creates a {@link Graph Engine} from system properties.
 *
 * <p>Configuration keys (system properties):
 *
 * <ul>
 *   <li>{@code rawwise.graph.engine.type} = {@code inmemory} | {@code default} (default {@code
 *       inmemory})
 *       <ul>
 *         <li>{@code inmemory}: backed by {@link In Memory Metastore} + {@link In Memory Knowledge
 *             Store} + {@link In Memory Vector Index}. Self-contained, no external dependencies.
 *             Suitable for dev / benchmark / tests.
 *         <li>{@code default}: backed by the supplied {@link Metastore} + {@link Knowledge Store},
 *             persistent across restarts. Suitable for production.
 *       </ul>
 * </ul>
 *
 * <p>On configuration error (e.g. type=default but metastore/knowledge Store is null) the factory
 * degrades to {@code inmemory} and logs a warning rather than throwing. Unknown {@code type} values
 * also degrade to inmemory.
 */
@Slf4j
public final class GraphEngineFactory {

    public static final String KEY_TYPE = "rawwise.graph.engine.type";

    public static final String DEFAULT_TYPE = "inmemory";

    private GraphEngineFactory() {}

    /**
     * Create a {@link Graph Engine} from system properties.
     *
     * @param metastore used when type=default. May be null when type=inmemory.
     * @param knowledge Store used when type=default. May be null when type=inmemory.
     * @param context Manager required (used by extract From Context / vector search node
     *     materialisation)
     * @param embedding Provider required for vector search
     * @param vector Index used when type=default. Ignored for type=inmemory (creates own).
     */
    public static GraphEngine createFromSystemProperties(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            ContextManager contextManager,
            EmbeddingProvider embeddingProvider,
            VectorIndex vectorIndex) {
        String type = System.getProperty(KEY_TYPE, DEFAULT_TYPE).trim().toLowerCase(Locale.ROOT);

        switch (type) {
            case "default":
                return createDefault(
                        metastore, knowledgeStore, contextManager, embeddingProvider, vectorIndex);
            case "inmemory":
                log.info("GraphEngine created: type=inmemory (self-contained)");
                return createInMemory(contextManager, embeddingProvider);
            default:
                log.warn("Unknown {}={}, falling back to inmemory", KEY_TYPE, type);
                return createInMemory(contextManager, embeddingProvider);
        }
    }

    private static GraphEngine createDefault(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            ContextManager contextManager,
            EmbeddingProvider embeddingProvider,
            VectorIndex vectorIndex) {
        if (metastore == null
                || knowledgeStore == null
                || contextManager == null
                || embeddingProvider == null
                || vectorIndex == null) {
            log.warn(
                    "type=default but required dependencies are null (metastore={}, knowledgeStore={}, contextManager={}, embeddingProvider={}, vectorIndex={}); falling back to inmemory",
                    metastore != null,
                    knowledgeStore != null,
                    contextManager != null,
                    embeddingProvider != null,
                    vectorIndex != null);
            return createInMemory(contextManager, embeddingProvider);
        }
        SummarizationService summarizationService = new NoOpSummarizationService();
        GraphEngineConfig config =
                new GraphEngineConfig(
                        metastore,
                        knowledgeStore,
                        contextManager,
                        summarizationService,
                        vectorIndex,
                        embeddingProvider,
                        new NoOpKnowledgeExtractor(),
                        null);
        log.info("GraphEngine created: type=default (metastore-backed)");
        return new DefaultGraphEngine(config);
    }

    private static GraphEngine createInMemory(
            ContextManager contextManager, EmbeddingProvider embeddingProvider) {
        if (contextManager == null || embeddingProvider == null) {
            throw new IllegalStateException(
                    "inmemory GraphEngine requires non-null contextManager and embeddingProvider");
        }
        Metastore mem = new InMemoryMetastore();
        KnowledgeStore ks = new InMemoryKnowledgeStore();
        VectorIndex vi = new InMemoryVectorIndex(embeddingProvider);
        SummarizationService summarizationService = new NoOpSummarizationService();
        GraphEngineConfig config =
                new GraphEngineConfig(
                        mem,
                        ks,
                        contextManager,
                        summarizationService,
                        vi,
                        embeddingProvider,
                        new NoOpKnowledgeExtractor(),
                        new GraphTrustAdapter(mem));
        return new DefaultGraphEngine(config);
    }
}
