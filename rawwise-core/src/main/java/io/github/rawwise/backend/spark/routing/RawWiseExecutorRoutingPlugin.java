package io.github.rawwise.backend.spark.routing;

import io.github.rawwise.axiom.orchestrator.InProcessCoordEngine;
import io.github.rawwise.axiom.pipeline.cognition.ActionExecutionLogger;
import io.github.rawwise.axiom.pipeline.cognition.ActionExecutorRegistry;
import io.github.rawwise.axiom.pipeline.cognition.ConditionConstructionPipeline;
import io.github.rawwise.axiom.pipeline.cognition.CorrectionCallback;
import io.github.rawwise.axiom.pipeline.cognition.DefaultActionExecutor;
import io.github.rawwise.axiom.pipeline.cognition.DefaultCognitionEngine;
import io.github.rawwise.axiom.pipeline.cognition.DerivationFeedbackCallback;
import io.github.rawwise.axiom.pipeline.derivation.DefaultDerivationEngine;
import io.github.rawwise.axiom.pipeline.derivation.DerivationEngine;
import io.github.rawwise.axiom.pipeline.derivation.Layer;
import io.github.rawwise.axiom.pipeline.evaluation.DefaultEvaluationEngine;
import io.github.rawwise.axiom.pipeline.evaluation.DefaultExternalEvaluationProvider;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.axiom.pipeline.evaluation.ExternalEvaluationProvider;
import io.github.rawwise.axiom.pipeline.feedback.FeedbackLoopCloser;
import io.github.rawwise.axiom.pipeline.ingestion.IngestionEngine;
import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.executor.service.TxnCoordinatorGrpcService;
import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.OccConfig;
import io.github.rawwise.engine.txn.clock.LogicalClock;
import io.github.rawwise.engine.txn.clock.MetastoreClockImpl;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorFactory;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorImpl;
import io.github.rawwise.governance.config.ConfigLoader;
import io.github.rawwise.governance.config.DynamicConfigRegistry;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.GrpcMetastoreClient;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Action;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.rpc.RpcTimeout;
import org.apache.spark.util.RpcUtils;

@Slf4j
public final class RawWiseExecutorRoutingPlugin implements ExecutorPlugin {
    private PluginContext pluginContext;
    private String executorId;
    private boolean registrationSucceeded;
    private transient RpcEnv rpcEnv;
    private transient DynamicConfigRegistry configRegistry;
    private transient Server grpcServer;
    private transient InProcessCoordEngine coordEngine;
    private int grpcPort;

    @Override
    public void init(PluginContext pluginContext, Map<String, String> extraConf) {
        this.pluginContext =
                Objects.requireNonNull(pluginContext, "pluginContext must not be null");
        this.executorId = requireExecutorId(pluginContext);
        int slotIndex = resolveSlotIndex(executorId);
        int maxRetries =
                pluginContext.conf().getInt("spark.rawwise.routing.register.maxRetries", 3);
        long backoffMs =
                pluginContext.conf().getLong("spark.rawwise.routing.register.backoffMs", 200L);
        boolean failOpen =
                pluginContext.conf().getBoolean("spark.rawwise.routing.register.failOpen", false);

        try {
            askWithRetry(
                    new RoutingMessages.RegisterExecutorMessage(
                            executorId, pluginContext.hostname(), slotIndex),
                    maxRetries,
                    backoffMs);

            org.apache.spark.SparkEnv env = SparkEnv.get();
            if (env == null || env.rpcEnv() == null) {
                if (!failOpen) {
                    throw new IllegalStateException(
                            "SparkEnv.rpcEnv is not available for executor " + executorId);
                }
                return;
            }
            this.rpcEnv = env.rpcEnv();
            RawWiseConfig rawwiseConfig = ConfigLoader.load(pluginContext.conf());
            this.configRegistry = DynamicConfigRegistry.create(rawwiseConfig);
            String endpointName = ExecutorShardStubEndpoint.ENDPOINT_NAME + "-" + executorId;
            rpcEnv.setupEndpoint(
                    endpointName,
                    new ExecutorShardStubEndpoint(
                            rpcEnv, pluginContext.conf(), configRegistry, executorId));
            registerEndpointWithRetry(executorId, endpointName, maxRetries, backoffMs);

            askWithRetry(
                    new RoutingMessages.ExecutorReadyMessage(executorId), maxRetries, backoffMs);

            startGrpcServer(rawwiseConfig);

            registrationSucceeded = true;
        } catch (Exception e) {
            registrationSucceeded = false;
            if (!failOpen) {
                throw new IllegalStateException(
                        "Failed to register executor routing state for " + executorId, e);
            }
        }
    }

    private void startGrpcServer(RawWiseConfig rawwiseConfig) throws IOException {
        grpcPort = pluginContext.conf().getInt("spark.rawwise.executor.grpc.port", 9091);
        log.info(
                "Executor gRPC port resolved: {} (from spark.rawwise.executor.grpc.port)",
                grpcPort);
        ExecutorPointQueryServiceImpl executorService =
                new ExecutorPointQueryServiceImpl(rawwiseConfig);

        DynamicMmapStore store = null;
        String warehousePath = rawwiseConfig.get(RawWiseOptions.CATALOG_WAREHOUSE_PATH);
        if (warehousePath != null && !warehousePath.isBlank()) {
            try {
                store = DynamicMmapStore.getInstance(warehousePath, "spark", rawwiseConfig);
                store.start();
                ensureMetastore(store, rawwiseConfig);
                waitForMetastoreReady(store, rawwiseConfig, 15, 1000L);
                store.initAxiomEngines(rawwiseConfig);
                log.info("Axiom engines initialized on executor {}", executorId);

                initAxiomPipeline(store, rawwiseConfig);
            } catch (Exception e) {
                log.warn(
                        "Failed to init Axiom engines on executor {}: {}",
                        executorId,
                        e.getMessage());
            }
        }

        TxnCoordinatorGrpcService txnCoordinatorService = null;
        String txnEngineStr = pluginContext.conf().get("spark.rawwise.txn.engine", "legacy");
        boolean isPlanA =
                "plan-a".equalsIgnoreCase(txnEngineStr) || "PLAN_A".equalsIgnoreCase(txnEngineStr);
        TxnEngineConfig.Engine engineType =
                isPlanA ? TxnEngineConfig.Engine.PLAN_A : TxnEngineConfig.Engine.LEGACY;
        if (engineType == TxnEngineConfig.Engine.PLAN_A && store != null) {
            try {
                TxnOrchestrator orchestrator =
                        createTxnOrchestrator(store, rawwiseConfig, warehousePath);
                txnCoordinatorService = new TxnCoordinatorGrpcService(orchestrator);
                log.info(
                        "TxnOrchestrator wired into TxnCoordinatorGrpcService on executor {}",
                        executorId);
            } catch (Exception e) {
                log.warn(
                        "Failed to initialize TxnCoordinatorGrpcService on executor {}: {}",
                        executorId,
                        e.getMessage());
            }
        }

        NettyServerBuilder serverBuilder =
                NettyServerBuilder.forPort(grpcPort)
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .maxInboundMessageSize(64 * 1024 * 1024)
                        .flowControlWindow(8 * 1024 * 1024)
                        .permitKeepAliveTime(10, TimeUnit.SECONDS)
                        .permitKeepAliveWithoutCalls(true)
                        .directExecutor()
                        .addService(executorService);
        if (txnCoordinatorService != null) {
            serverBuilder.addService(txnCoordinatorService);
            log.info(
                    "TxnCoordinatorGrpcService registered on executor {} (PLAN_A mode)",
                    executorId);
        }
        grpcServer = serverBuilder.build().start();
        log.info("Executor gRPC server started on port {}", grpcPort);
    }

    private TxnOrchestrator createTxnOrchestrator(
            DynamicMmapStore store, RawWiseConfig rawwiseConfig, String warehousePath) {
        Metastore metastore = store.getMetastore();
        if (metastore == null) {
            Map<String, Object> cfg = rawwiseConfig.asMap();
            String msHost = (String) cfg.getOrDefault("rawwise.metastore.grpc-host", "localhost");
            int msPort = parseIntOrDefault(cfg, "rawwise.metastore.grpc-port", 9001);
            metastore = new GrpcMetastoreClient(msHost, msPort);
            store.setMetastore(metastore);
        }

        WAL wal = store.getWal();
        LmdbInstance lmdbInstance = store.currentLmdbInstance("_default", new byte[] {0});
        if (lmdbInstance == null && warehousePath != null && !warehousePath.isBlank()) {
            lmdbInstance =
                    new LmdbInstance(
                            "txn-default",
                            java.nio.file.Path.of(warehousePath, "txn-default"),
                            128L * 1024L * 1024L);
            store.tableStore("_default").addWritableInstance(lmdbInstance);
            log.info(
                    "Created and registered new LmdbInstance {} for TxnOrchestrator on executor {}",
                    lmdbInstance.instanceId(),
                    executorId);
        }
        if (lmdbInstance == null) {
            throw new IllegalStateException("No LmdbInstance available for TxnOrchestrator");
        }

        // KNOWN LIMITATION: hardcoded catalog/database/table identifiers mean all transactions
        // share a single LMDB instance and single TxnOrchestrator. Multi-table transaction
        // routing requires a larger refactor (per-table TxnOrchestrator + instance selection).
        boolean perKeyOcc = rawwiseConfig.get(RawWiseOptions.OCC_PER_KEY_ENABLED);
        OccConfig occConfig = new OccConfig(perKeyOcc);
        TxnOrchestrator orchestrator =
                TxnOrchestratorFactory.create(
                        new TxnEngineConfig(TxnEngineConfig.Engine.PLAN_A),
                        metastore,
                        lmdbInstance,
                        "rawwise",
                        "default",
                        "_default",
                        wal,
                        store.getOrCreateVersionTracker("rawwise.default._default"),
                        occConfig);
        log.info(
                "TxnOrchestrator created on executor {} (PLAN_A mode, perKeyOCC={})",
                executorId,
                perKeyOcc);

        long sentinelMax =
                LmdbInstance.collectSentinelMaxFromInstances(
                        store.instancePool().getAllInstances());
        if (sentinelMax > 0 && orchestrator instanceof TxnOrchestratorImpl impl) {
            LogicalClock clock = impl.clock();
            if (clock instanceof MetastoreClockImpl mci) {
                mci.advanceFromLmdbSentinels(sentinelMax);
            }
        }

        if (orchestrator instanceof TxnOrchestratorImpl impl) {
            store.setTxnStoreBridge(impl.createStoreBridge());
            log.info(
                    "TxnStoreBridge wired from orchestrator to DynamicMmapStore on executor {}",
                    executorId);
        }

        return orchestrator;
    }

    private Metastore ensureMetastore(DynamicMmapStore store, RawWiseConfig rawwiseConfig) {
        Metastore metastore = store.getMetastore();
        if (metastore == null) {
            metastore = io.github.rawwise.backend.shared.BackendSharedRegistry.getMetastore();
            if (metastore != null) {
                store.setMetastore(metastore);
                log.info(
                        "Using shared Metastore from BackendSharedRegistry (embedded mode): impl={}",
                        metastore.getClass().getSimpleName());
                return metastore;
            }
            Map<String, Object> cfg = rawwiseConfig.asMap();
            String msHost = (String) cfg.getOrDefault("rawwise.metastore.grpc-host", "localhost");
            int msPort = parseIntOrDefault(cfg, "rawwise.metastore.grpc-port", 9001);
            metastore = new GrpcMetastoreClient(msHost, msPort);
            store.setMetastore(metastore);
            log.info("Created GrpcMetastoreClient for AI-Native pipeline: {}:{}", msHost, msPort);
        }
        return metastore;
    }

    private void waitForMetastoreReady(
            DynamicMmapStore store, RawWiseConfig rawwiseConfig, int maxRetries, long backoffMs) {
        Metastore metastore = store.getMetastore();
        if (metastore == null) {
            return;
        }
        for (int i = 0; i < maxRetries; i++) {
            try {
                metastore.get("__axiom_health_check__".getBytes());
                log.info("Metastore gRPC ready after {} attempt(s)", i + 1);
                return;
            } catch (Exception e) {
                if (i < maxRetries - 1) {
                    log.debug(
                            "Metastore not ready (attempt {}/{}): {} — retrying in {}ms",
                            i + 1,
                            maxRetries,
                            e.getMessage(),
                            backoffMs);
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
        log.warn("Metastore gRPC not ready after {} attempts — proceeding anyway", maxRetries);
    }

    private void initAxiomPipeline(DynamicMmapStore store, RawWiseConfig rawwiseConfig) {
        Metastore metastore = ensureMetastore(store, rawwiseConfig);
        Objects.requireNonNull(
                metastore, "Metastore must be initialized before AI-Native pipeline");

        ExternalEvaluationProvider externalEvalProvider = new DefaultExternalEvaluationProvider();
        EvaluationEngine evaluationEngine =
                new DefaultEvaluationEngine(
                        metastore, store.knowledgeStore(), externalEvalProvider);
        log.info(
                "EvaluationEngine wired with ExternalEvaluationProvider: {}",
                externalEvalProvider.providerId());

        IngestionEngine ingestionEngine = store.getIngestionEngine();
        ExecutorService cascadeExecutor =
                Executors.newFixedThreadPool(
                        2,
                        r -> {
                            Thread t = new Thread(r, "feedback-cascade");
                            t.setDaemon(true);
                            return t;
                        });
        FeedbackLoopCloser feedbackLoopCloser =
                new FeedbackLoopCloser(
                        evaluationEngine,
                        null,
                        metastore,
                        store.knowledgeStore(),
                        cascadeExecutor,
                        ingestionEngine);

        CorrectionCallback correctionCallback =
                (viewId, correction, depth) ->
                        feedbackLoopCloser.onAnnotation(viewId, correction, depth);
        coordEngine = new InProcessCoordEngine();
        String defaultAgentId =
                System.getProperty("rawwise.axiom.agent.default-id", "system:auto-derivation");
        ActionExecutionLogger execLogger =
                new ActionExecutionLogger(
                        store.knowledgeStore(), defaultAgentId, "default-executor");
        DefaultActionExecutor actionExecutor =
                new DefaultActionExecutor(
                        metastore,
                        store.knowledgeStore(),
                        correctionCallback,
                        coordEngine,
                        execLogger);
        log.info(
                "DefaultActionExecutor wired with InProcessCoordEngine + ActionExecutionLogger (audit enabled)");

        ActionExecutorRegistry actionExecutorRegistry = new ActionExecutorRegistry();
        actionExecutorRegistry.register(Action.ActionType.RETRIEVE, actionExecutor);
        actionExecutorRegistry.register(Action.ActionType.TRANSFORM, actionExecutor);
        actionExecutorRegistry.register(Action.ActionType.NOTIFY, actionExecutor);
        actionExecutorRegistry.register(Action.ActionType.CORRECT, actionExecutor);
        log.info(
                "ActionExecutorRegistry wired with {} executor types",
                actionExecutorRegistry.registeredTypes().size());

        DerivationFeedbackCallback derivationFeedbackCallback =
                new DerivationFeedbackCallback() {
                    @Override
                    public void onDerive(String entityId, Layer layer, double score) {
                        feedbackLoopCloser.onDerive(entityId, layer, score);
                    }

                    @Override
                    public void onDeriveBatch(
                            List<String> entityIds, Layer layer, double defaultScore) {
                        feedbackLoopCloser.onDeriveBatch(entityIds, layer, defaultScore);
                    }
                };

        var cognitionEngine =
                new DefaultCognitionEngine(
                        metastore,
                        store.knowledgeStore(),
                        evaluationEngine,
                        actionExecutor,
                        derivationFeedbackCallback);
        feedbackLoopCloser.setCognitionEngine(cognitionEngine);

        ConditionConstructionPipeline conditionPipeline =
                new ConditionConstructionPipeline(null, store.knowledgeStore(), cognitionEngine);
        log.info("ConditionConstructionPipeline wired (graphEngine=null, text-only fallback mode)");

        store.setFeedbackLoopCloser(feedbackLoopCloser);
        store.setEvaluationEngine(evaluationEngine);
        store.setCognitionEngine(cognitionEngine);

        // Step 1: Create EntityDerivationStage and inject into DynamicMmapStore,
        // enabling AutoDerivationHook to trigger entity extraction and GraphIndexer graph writes on
        // the write path.
        try {
            io.github.rawwise.axiom.pipeline.entity.EntityExtractionConfig entityCfg =
                    io.github.rawwise.axiom.pipeline.entity.EntityExtractionConfig.builder()
                            .enabled(
                                    Boolean.parseBoolean(
                                            System.getProperty(
                                                    "rawwise.entity.extraction.enabled", "true")))
                            .hybridEnabled(
                                    Boolean.parseBoolean(
                                            System.getProperty(
                                                    "rawwise.entity.extraction.hybrid.enabled",
                                                    "true")))
                            .llmEndpoint(
                                    System.getProperty(
                                            "rawwise.entity.extraction.llm.provider", "stub://"))
                            .llmApiKey(System.getProperty("rawwise.entity.extraction.llm.api-key"))
                            .spacySidecarPort(
                                    Integer.parseInt(
                                            System.getProperty(
                                                    "rawwise.entity.extraction.spacy.sidecar.port",
                                                    "17091")))
                            .timeout(java.time.Duration.ofSeconds(30))
                            .asyncThreads(8)
                            .asyncQueueSize(10000)
                            .build();
            io.github.rawwise.axiom.pipeline.entity.HybridEntityExtractor extractor =
                    io.github.rawwise.axiom.pipeline.entity.EntityExtractorFactory.create(
                            entityCfg);
            io.github.rawwise.axiom.pipeline.graph.GraphIndexer graphIndexer =
                    new io.github.rawwise.axiom.pipeline.graph.GraphIndexer(store.knowledgeStore());
            java.util.concurrent.ThreadPoolExecutor entityPool =
                    io.github.rawwise.axiom.pipeline.entity.EntityDerivationStage.newDefaultPool(
                            entityCfg);
            io.github.rawwise.axiom.pipeline.entity.EntityDerivationStage entityStage =
                    new io.github.rawwise.axiom.pipeline.entity.EntityDerivationStage(
                            extractor, graphIndexer, store.knowledgeStore(), entityPool, entityCfg);
            store.setEntityDerivationStage(entityStage);
            log.info(
                    "EntityDerivationStage wired into DynamicMmapStore: enabled={} hybrid={} llm={}",
                    entityCfg.enabled(),
                    entityCfg.hybridEnabled(),
                    entityCfg.llmEndpoint());

            // Step 3: Create GraphQuery (LocalGraphQueryEngine) and register via
            // BackendSharedRegistry
            // to expose it to the Backend Server (AxiomServiceImpl) in the same JVM for graph
            // recall third path.
            try {
                io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor accessor =
                        new io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor(
                                metastore, store.knowledgeStore());
                io.github.rawwise.axiom.pipeline.graph.GraphQuery graphQuery =
                        new io.github.rawwise.axiom.pipeline.graph.LocalGraphQueryEngine(
                                store.knowledgeStore(), accessor);
                io.github.rawwise.backend.shared.BackendSharedRegistry.setGraphQuery(graphQuery);
                log.info("GraphQuery (LocalGraphQueryEngine) registered to BackendSharedRegistry");
            } catch (RuntimeException e2) {
                log.warn(
                        "GraphQuery registration FAILED, search falls back to vector+keyword only: {}",
                        e2.getMessage());
            }
        } catch (RuntimeException e) {
            log.warn(
                    "EntityDerivationStage wiring FAILED, falling back to vector-only derivation: {}",
                    e.getMessage());
        }

        DerivationEngine derivationEngine = store.getDerivationEngine();
        if (derivationEngine instanceof DefaultDerivationEngine de) {
            de.setDerivationFeedbackCallback(derivationFeedbackCallback);
        }

        log.info("AI-Native pipeline initialized on executor {}", executorId);
    }

    @Override
    public void shutdown() {
        if (grpcServer != null) {
            try {
                grpcServer.shutdown();
            } catch (Exception e) {
                log.warn("Failed to shutdown gRPC server", e);
            }
        }
        if (configRegistry != null) {
            try {
                configRegistry.stop();
            } catch (Exception e) {
                // ignore
            }
            configRegistry = null;
        }
        io.github.rawwise.engine.storage.DynamicMmapStoreFactory.closeAll();
        if (coordEngine != null) {
            try {
                coordEngine.close();
            } catch (Exception e) {
                log.warn("Failed to close InProcessCoordEngine", e);
            }
            coordEngine = null;
        }
        if (pluginContext == null || executorId == null || !registrationSucceeded) {
            return;
        }
        try {
            pluginContext.send(new RoutingMessages.ExecutorLostMessage(executorId));
        } catch (Exception ignored) {
        }
    }

    private void registerEndpointWithRetry(
            String executorId, String endpointName, int maxRetries, long backoffMs)
            throws Exception {
        SparkConf conf = pluginContext.conf();
        RpcEndpointRef driverRef =
                RpcUtils.makeDriverRef(RawWiseDriverRoutingEndpoint.ENDPOINT_NAME, conf, rpcEnv);

        RoutingMessages.RegisterExecutorEndpointMessage message =
                new RoutingMessages.RegisterExecutorEndpointMessage(
                        executorId, rpcEnv.address().host(), rpcEnv.address().port(), endpointName);

        int attempts = Math.max(1, maxRetries);
        Exception last = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                RpcTimeout timeout = RpcUtils.askRpcTimeout(pluginContext.conf());
                driverRef.askSync(
                        message, timeout, scala.reflect.ClassTag$.MODULE$.apply(Object.class));
                return;
            } catch (Exception e) {
                last = e;
                if (attempt == attempts) {
                    break;
                }
                sleepQuietly(backoffMs * attempt);
            }
        }
        throw last;
    }

    private void askWithRetry(Object message, int maxRetries, long backoffMs) throws Exception {
        int attempts = Math.max(1, maxRetries);
        Exception last = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                pluginContext.ask(message);
                return;
            } catch (Exception e) {
                last = e;
                if (attempt == attempts) {
                    break;
                }
                sleepQuietly(backoffMs * attempt);
            }
        }
        throw last;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(Math.max(0L, millis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying routing registration", e);
        }
    }

    private static String requireExecutorId(PluginContext pluginContext) {
        String executorId = pluginContext.executorID();
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalStateException("Spark executorID must not be blank");
        }
        return executorId;
    }

    static int resolveSlotIndex(String executorId) {
        try {
            return Integer.parseInt(executorId);
        } catch (NumberFormatException ignored) {
            return Math.floorMod(executorId.hashCode(), Integer.MAX_VALUE);
        }
    }

    static int parseIntOrDefault(Map<String, Object> cfg, String key, int defaultValue) {
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
}
