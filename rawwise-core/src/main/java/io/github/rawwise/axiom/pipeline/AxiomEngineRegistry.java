package io.github.rawwise.axiom.pipeline;

import io.github.rawwise.axiom.memory.ViewMetadataStore;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.derivation.AsyncDerivationQueue;
import io.github.rawwise.axiom.pipeline.derivation.AutoDerivationHook;
import io.github.rawwise.axiom.pipeline.derivation.DefaultDerivationEngine;
import io.github.rawwise.axiom.pipeline.derivation.DerivationEngine;
import io.github.rawwise.axiom.pipeline.derivation.KeywordIndex;
import io.github.rawwise.axiom.pipeline.entity.EntityDerivationStage;
import io.github.rawwise.axiom.pipeline.evaluation.DefaultEvaluationEngine;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.axiom.pipeline.feedback.FeedbackLoopCloser;
import io.github.rawwise.axiom.pipeline.ingestion.DefaultImplicitViewGenerator;
import io.github.rawwise.axiom.pipeline.ingestion.DefaultIngestionEngine;
import io.github.rawwise.axiom.pipeline.ingestion.IngestionEngine;
import io.github.rawwise.engine.storage.ImmutablePromotionService;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingProviderFactory;
import io.github.rawwise.engine.vector.FlatVectorColdStore;
import io.github.rawwise.engine.vector.IvfPqConfig;
import io.github.rawwise.engine.vector.JVectorConfig;
import io.github.rawwise.engine.vector.JVectorVectorIndex;
import io.github.rawwise.engine.vector.LanceVectorColdStore;
import io.github.rawwise.engine.vector.LmdbIoBackend;
import io.github.rawwise.engine.vector.VectorColdStore;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.engine.vector.VectorIndexManager;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.metastore.Metastore;
import java.util.Map;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AxiomEngineRegistry {

    private static volatile VectorIndexManager sharedVectorIndexManager;

    public static void setVectorIndexManager(VectorIndexManager manager) {
        sharedVectorIndexManager = manager;
        log.info(
                "AxiomEngineRegistry: shared VectorIndexManager set (tableFormatMode={})",
                manager != null && manager.isTableFormatMode());
    }

    private final String basePath;
    private final StorageWriteFacade storageFacade;
    private final Supplier<Metastore> metastoreSupplier;
    private final ImmutablePromotionService immutablePromotionService;

    @Getter private volatile VectorIndex vectorIndex;
    @Getter private volatile KeywordIndex keywordIndex;

    private volatile IngestionEngine ingestionEngine;
    private volatile DerivationEngine derivationEngine;
    private volatile EmbeddingProvider embeddingProvider;
    private volatile KnowledgeStore knowledgeStore;
    @Setter @Getter private volatile FeedbackLoopCloser feedbackLoopCloser;
    @Getter @Setter private volatile EntityDerivationStage entityDerivationStage;
    @Setter private volatile EvaluationEngine evaluationEngine;
    @Getter private volatile CognitionEngine cognitionEngine;
    private volatile AsyncDerivationQueue asyncDerivationQueue;

    public void setCognitionEngine(CognitionEngine engine) {
        this.cognitionEngine = engine;
        log.info(
                "AxiomEngineRegistry: CognitionEngine set, propagating to registered AutoDerivationHook instances");
        for (TableStore ts : storageFacade.allTableStores()) {
            for (io.github.rawwise.engine.storage.PostWriteHook hook : ts.getPostWriteHooks()) {
                if (hook instanceof AutoDerivationHook adh) {
                    wireCognitionToHook(adh);
                }
            }
        }
    }

    private void wireCognitionToHook(AutoDerivationHook hook) {
        if (cognitionEngine != null) {
            hook.setCognitionEngine(cognitionEngine);
        }
        hook.setKnowledgeStore(knowledgeStore());
    }

    private void wireViewMetadataToHook(AutoDerivationHook hook) {
        Metastore ms = metastoreSupplier.get();
        if (ms != null) {
            hook.setViewMetadataStore(new ViewMetadataStore(ms));
            log.info("AxiomEngineRegistry: ViewMetadataStore wired into AutoDerivationHook");
        } else {
            log.warn(
                    "AxiomEngineRegistry: metastore unavailable, ViewMetadataStore NOT wired into AutoDerivationHook");
        }
    }

    public AxiomEngineRegistry(
            String basePath,
            StorageWriteFacade storageFacade,
            Supplier<Metastore> metastoreSupplier,
            ImmutablePromotionService immutablePromotionService) {
        this.basePath = basePath;
        this.storageFacade = storageFacade;
        this.metastoreSupplier = metastoreSupplier;
        this.immutablePromotionService = immutablePromotionService;
    }

    public void setVectorIndex(VectorIndex vectorIndex, VectorColdStore coldStore) {
        this.vectorIndex = vectorIndex;
        if (vectorIndex != null && coldStore != null) {
            vectorIndex.setColdStore(coldStore);
        }
    }

    public EvaluationEngine getEvaluationEngine() {
        if (evaluationEngine == null) {
            Metastore ms = metastoreSupplier.get();
            if (ms != null) {
                evaluationEngine = new DefaultEvaluationEngine(ms, knowledgeStore());
            }
        }
        return evaluationEngine;
    }

    public void initAxiomEngines(RawWiseConfig config) {
        if (this.embeddingProvider == null) {
            this.embeddingProvider = EmbeddingProviderFactory.createFromSystemProperties();
            log.info(
                    "EmbeddingProvider initialized: model={}",
                    this.embeddingProvider.modelIdentifier());
        }
        if (this.vectorIndex == null) {
            Map<String, Object> cfg = config.asMap();
            int dimension = resolveAxiomEmbeddingDimension(cfg);
            boolean indexManagerEnabled =
                    Boolean.parseBoolean(
                            System.getProperty("rawwise.vector.index-manager.enabled", "false"));
            if (indexManagerEnabled) {
                VectorIndexManager manager = sharedVectorIndexManager;
                if (manager == null) {
                    manager =
                            new VectorIndexManager(
                                    new IvfPqConfig(dimension, 1024, 64, 32, 10, 20, 10000, 42, 3),
                                    false,
                                    java.nio.file.Path.of(basePath));
                }
                this.vectorIndex = manager.createVectorIndex("default", "axiom_vector", dimension);
                log.info(
                        "VectorIndex created via VectorIndexManager (adapter mode, dimension={})",
                        dimension);
            } else {
                int graphDegree = parseIntOrDefault(cfg, "rawwise.axiom.vector.graph-degree", 32);
                int searchTopK = parseIntOrDefault(cfg, "rawwise.axiom.vector.search-top-k", 10);
                int efSearch =
                        parseIntOrDefault(
                                cfg,
                                "rawwise.axiom.vector.ef-search",
                                Integer.getInteger("rawwise.vector.hnsw.ef-search", 50));
                int buildThreshold =
                        parseIntOrDefault(cfg, "rawwise.axiom.vector.build-threshold", 10000);
                int mergeThreshold =
                        parseIntOrDefault(cfg, "rawwise.axiom.vector.merge-threshold", 10000);
                int maxVectors = parseIntOrDefault(cfg, "rawwise.axiom.vector.max-vectors", 500000);
                java.nio.file.Path vectorBasePath = java.nio.file.Path.of(basePath, "vector-index");
                JVectorConfig jVectorConfig =
                        new JVectorConfig(
                                "disk",
                                dimension,
                                graphDegree,
                                searchTopK,
                                efSearch,
                                vectorBasePath.toString(),
                                buildThreshold,
                                mergeThreshold,
                                maxVectors);
                boolean lmdbIoEnabled =
                        Boolean.parseBoolean(
                                System.getProperty("rawwise.vector.lmdb-io.enabled", "false"));
                if (lmdbIoEnabled) {
                    LmdbIoBackend lmdbBackend = new LmdbIoBackend(vectorBasePath.resolve("lmdb"));
                    this.vectorIndex =
                            new JVectorVectorIndex(
                                    jVectorConfig, this.embeddingProvider, false, lmdbBackend);
                    log.info(
                            "VectorIndex created: JVectorVectorIndex with LMDB I/O at {}",
                            vectorBasePath);
                } else {
                    this.vectorIndex =
                            new JVectorVectorIndex(jVectorConfig, this.embeddingProvider, false);
                    log.info("VectorIndex created: JVectorVectorIndex at {}", vectorBasePath);
                }
            }
        }
        if (this.vectorIndex != null) {
            java.nio.file.Path vectorPath = java.nio.file.Path.of(basePath, "vectors");
            int dimension = resolveAxiomEmbeddingDimension(config.asMap());
            VectorColdStore coldStore = createColdStore(vectorPath, dimension);
            this.vectorIndex.setColdStore(coldStore);
            log.info(
                    "VectorIndex coldStore initialized at {} (type={})",
                    vectorPath,
                    System.getProperty("rawwise.vector.cold-store.type", "flat"));
        }
        if (this.keywordIndex == null) {
            this.keywordIndex = new KeywordIndex();
            log.info("KeywordIndex created");
        }
        if (this.embeddingProvider != null && this.vectorIndex != null) {
            DerivationEngine de = getDerivationEngine();
            if (de != null) {
                AutoDerivationHook hook =
                        new AutoDerivationHook(
                                de,
                                this.vectorIndex,
                                this.embeddingProvider,
                                this.keywordIndex,
                                this.feedbackLoopCloser,
                                storageFacade);
                wireCognitionToHook(hook);
                wireViewMetadataToHook(hook);
                hook.setAsyncDerivationQueue(getAsyncDerivationQueue());
                for (TableStore ts : storageFacade.allTableStores()) {
                    ts.registerPostWriteHook(hook);
                }
                log.info("AutoDerivationHook registered on all TableStores");
            } else {
                log.info("DerivationEngine not available, AutoDerivationHook skipped");
            }
        }
    }

    private static int parseIntOrDefault(Map<String, Object> cfg, String key, int defaultValue) {
        Object v = cfg.get(key);
        if (v instanceof Number n) return n.intValue();
        if (v instanceof String s) {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }

    private static int resolveAxiomEmbeddingDimension(Map<String, Object> configMap) {
        Object dim = configMap.get("rawwise.axiom.embedding.dimension");
        if (dim == null) {
            dim = configMap.get("rawwise.embedding.provider.dimension");
        }
        if (dim == null) {
            dim = configMap.get("rawwise.embedding.dimension");
        }
        if (dim == null) {
            dim = System.getProperty("rawwise.embedding.provider.dimension");
        }
        if (dim == null) {
            dim = System.getProperty("rawwise.embedding.dimension");
        }
        if (dim == null) {
            dim = System.getProperty("RAWWISE_EMBEDDING_DIMENSION");
        }
        if (dim instanceof Number n) {
            return n.intValue();
        }
        if (dim instanceof String s) {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException ignored) {
            }
        }
        return 768;
    }

    private static VectorColdStore createColdStore(java.nio.file.Path vectorPath, int dimension) {
        String type = System.getProperty("rawwise.vector.cold-store.type", "flat");
        return switch (type) {
            case "lance" -> new LanceVectorColdStore(vectorPath, dimension);
            default -> new FlatVectorColdStore(vectorPath, dimension);
        };
    }

    public void registerAutoDeriveHook(String tableIdentifier) {
        if (embeddingProvider == null || vectorIndex == null) {
            log.info(
                    "registerAutoDeriveHook: engines not initialized, attempting lazy init for table {}",
                    tableIdentifier);
            ensureAxiomEngines();
        }
        if (embeddingProvider == null || vectorIndex == null) {
            log.warn(
                    "registerAutoDeriveHook SKIP for table {}: embeddingProvider={} vectorIndex={} (lazy init failed)",
                    tableIdentifier,
                    embeddingProvider != null ? embeddingProvider.modelIdentifier() : "null",
                    vectorIndex != null ? vectorIndex.getClass().getSimpleName() : "null");
            return;
        }
        TableStore ts = storageFacade.tableStore(tableIdentifier);
        for (io.github.rawwise.engine.storage.PostWriteHook existing : ts.getPostWriteHooks()) {
            if (existing instanceof AutoDerivationHook) {
                log.debug(
                        "registerAutoDeriveHook: already registered for table {}", tableIdentifier);
                return;
            }
        }
        DerivationEngine de = getDerivationEngine();
        if (de == null) {
            log.warn(
                    "registerAutoDeriveHook SKIP for table {}: DerivationEngine is null (metastoreSupplier={} metastore={})",
                    tableIdentifier,
                    metastoreSupplier,
                    metastoreSupplier != null ? metastoreSupplier.get() : "null");
            return;
        }
        try {
            AutoDerivationHook hook =
                    new AutoDerivationHook(
                            de,
                            vectorIndex,
                            embeddingProvider,
                            keywordIndex,
                            this.feedbackLoopCloser,
                            storageFacade,
                            this.entityDerivationStage);
            wireCognitionToHook(hook);
            wireViewMetadataToHook(hook);
            hook.setAsyncDerivationQueue(getAsyncDerivationQueue());
            ts.registerPostWriteHook(hook);
            log.info(
                    "AutoDerivationHook registered for table {} (entityStage={})",
                    tableIdentifier,
                    this.entityDerivationStage != null ? "enabled" : "disabled");
        } catch (Exception e) {
            log.error(
                    "registerAutoDeriveHook FAILED for table {}: {}",
                    tableIdentifier,
                    e.getMessage(),
                    e);
        }
    }

    /**
     * Lazy-init embedding Provider, vector Index, keyword Index from system properties. Used when
     * init Axiom Engines(Raw Wise Config) failed at startup due to transient g RPC errors (e.g.
     * metastore not yet bound). Reads dimension and vector params from -Drawwise.* system
     * properties as fallback.
     */
    private synchronized void ensureAxiomEngines() {
        if (this.embeddingProvider == null) {
            try {
                this.embeddingProvider = EmbeddingProviderFactory.createFromSystemProperties();
                log.info(
                        "Lazy init EmbeddingProvider: model={}",
                        this.embeddingProvider.modelIdentifier());
            } catch (Exception e) {
                log.warn("Lazy init EmbeddingProvider FAILED: {}", e.getMessage(), e);
                return;
            }
        }
        if (this.keywordIndex == null) {
            try {
                this.keywordIndex = new KeywordIndex();
                log.info("Lazy init KeywordIndex created");
            } catch (Exception e) {
                log.warn("Lazy init KeywordIndex FAILED: {}", e.getMessage(), e);
            }
        }
        if (this.vectorIndex == null) {
            try {
                int dimension =
                        Integer.parseInt(
                                System.getProperty(
                                        "rawwise.embedding.provider.dimension",
                                        System.getProperty("rawwise.embedding.dimension", "1024")));
                int graphDegree = Integer.getInteger("rawwise.axiom.vector.graph-degree", 32);
                int searchTopK = Integer.getInteger("rawwise.axiom.vector.search-top-k", 10);
                int efSearch =
                        Integer.getInteger(
                                "rawwise.vector.hnsw.ef-search",
                                Integer.getInteger("rawwise.axiom.vector.ef-search", 50));
                int buildThreshold =
                        Integer.getInteger("rawwise.axiom.vector.build-threshold", 10000);
                int mergeThreshold =
                        Integer.getInteger("rawwise.axiom.vector.merge-threshold", 10000);
                int maxVectors = Integer.getInteger("rawwise.axiom.vector.max-vectors", 500000);
                java.nio.file.Path vectorBasePath = java.nio.file.Path.of(basePath, "vector-index");
                JVectorConfig jVectorConfig =
                        new JVectorConfig(
                                "disk",
                                dimension,
                                graphDegree,
                                searchTopK,
                                efSearch,
                                vectorBasePath.toString(),
                                buildThreshold,
                                mergeThreshold,
                                maxVectors);
                this.vectorIndex =
                        new JVectorVectorIndex(jVectorConfig, this.embeddingProvider, false);
                log.info(
                        "Lazy init VectorIndex: JVectorVectorIndex at {} dimension={}",
                        vectorBasePath,
                        dimension);

                java.nio.file.Path vectorPath = java.nio.file.Path.of(basePath, "vectors");
                VectorColdStore coldStore = createColdStore(vectorPath, dimension);
                this.vectorIndex.setColdStore(coldStore);
                log.info("Lazy init VectorIndex coldStore at {}", vectorPath);
            } catch (Exception e) {
                log.warn("Lazy init VectorIndex FAILED: {}", e.getMessage(), e);
            }
        }
    }

    public IngestionEngine getIngestionEngine() {
        IngestionEngine local = ingestionEngine;
        if (local == null) {
            synchronized (this) {
                local = ingestionEngine;
                if (local == null) {
                    Metastore ms = metastoreSupplier.get();
                    if (ms == null) {
                        log.warn("Metastore not configured, IngestionEngine unavailable");
                        return null;
                    }
                    DefaultImplicitViewGenerator viewGen =
                            new DefaultImplicitViewGenerator(ms, knowledgeStore());
                    local =
                            new DefaultIngestionEngine(
                                    storageFacade, immutablePromotionService, viewGen);
                    this.ingestionEngine = local;
                }
            }
        }
        return local;
    }

    public DerivationEngine getDerivationEngine() {
        DerivationEngine local = derivationEngine;
        if (local == null) {
            synchronized (this) {
                local = derivationEngine;
                if (local == null) {
                    try {
                        Metastore ms = metastoreSupplier.get();
                        if (ms == null) {
                            log.warn("Metastore not configured, DerivationEngine unavailable");
                            return null;
                        }
                        local = new DefaultDerivationEngine(storageFacade, ms, knowledgeStore());
                        this.derivationEngine = local;
                    } catch (Exception e) {
                        log.warn(
                                "getDerivationEngine FAILED: metastoreSupplier threw exception: {}",
                                e.getMessage(),
                                e);
                        return null;
                    }
                }
            }
        }
        return local;
    }

    public EmbeddingProvider getEmbeddingProvider() {
        return embeddingProvider;
    }

    public KnowledgeStore knowledgeStore() {
        if (knowledgeStore == null) {
            synchronized (this) {
                if (knowledgeStore == null) {
                    knowledgeStore = new InMemoryKnowledgeStore();
                    log.info("KnowledgeStore lazily initialized as InMemoryKnowledgeStore");
                }
            }
        }
        return knowledgeStore;
    }

    /**
     * Lazily creates the shared {@link Async Derivation Queue} backed by virtual threads and the
     * {@link Knowledge Store} (for watermark persistence). The same instance is wired into every
     * registered {@link Auto Derivation Hook} so that derivation backpressure is global per store.
     */
    public AsyncDerivationQueue getAsyncDerivationQueue() {
        if (asyncDerivationQueue == null) {
            synchronized (this) {
                if (asyncDerivationQueue == null) {
                    int queueSize =
                            Integer.parseInt(
                                    System.getProperty(
                                            "rawwise.axiom.derivation.async.queue-size", "10000"));
                    int concurrency =
                            Integer.parseInt(
                                    System.getProperty(
                                            "rawwise.axiom.derivation.async.concurrency", "8"));
                    asyncDerivationQueue =
                            new AsyncDerivationQueue(
                                    queueSize, concurrency, knowledgeStore(), null);
                    log.info(
                            "AsyncDerivationQueue created: queueSize={}, concurrency={}",
                            queueSize,
                            concurrency);
                }
            }
        }
        return asyncDerivationQueue;
    }

    /**
     * Gracefully drains in-flight async derivation tasks. Should be called during store shutdown
     * before closing the underlying vector/keyword indices so that pending derivations can finish.
     */
    public void shutdownAsyncDerivation() {
        AsyncDerivationQueue q = asyncDerivationQueue;
        if (q != null) {
            try {
                q.close();
            } catch (Exception e) {
                log.warn("Failed to close AsyncDerivationQueue: {}", e.getMessage());
            }
        }
    }

    /**
     * Closes the vector Index (which flushes hot Buffers to disk). Must be called AFTER {@link
     * #shutdown Async Derivation()} so that pending async derivation tasks finish before the index
     * is closed.
     */
    public void close() {
        shutdownAsyncDerivation();
        VectorIndex vi = vectorIndex;
        if (vi != null) {
            try {
                vi.close();
                log.info("VectorIndex closed (hotBuffers flushed to disk)");
            } catch (Exception e) {
                log.warn("Failed to close VectorIndex: {}", e.getMessage(), e);
            }
        }
    }
}
