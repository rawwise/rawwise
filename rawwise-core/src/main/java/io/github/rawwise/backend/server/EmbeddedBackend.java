package io.github.rawwise.backend.server;

import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.server.auth.QueryAuthorizer;
import io.github.rawwise.backend.server.job.JobCategory;
import io.github.rawwise.backend.server.job.JobEventLog;
import io.github.rawwise.backend.server.job.SparkJobScheduler;
import io.github.rawwise.backend.server.load.BackpressureManager;
import io.github.rawwise.backend.server.load.DefaultLoadCollector;
import io.github.rawwise.backend.server.service.AxiomServiceImpl;
import io.github.rawwise.backend.server.service.ClusterManagementServiceImpl;
import io.github.rawwise.backend.server.service.InferenceServiceImpl;
import io.github.rawwise.backend.server.service.JobManagementServiceImpl;
import io.github.rawwise.backend.server.service.MonitorServiceImpl;
import io.github.rawwise.backend.server.service.QueryServiceImpl;
import io.github.rawwise.backend.server.service.SessionManagementServiceImpl;
import io.github.rawwise.backend.server.service.TableManagementServiceImpl;
import io.github.rawwise.backend.server.service.TrainingDataServiceImpl;
import io.github.rawwise.backend.server.session.InMemorySessionStore;
import io.github.rawwise.backend.server.txn.DefaultServerTxnCoordinator;
import io.github.rawwise.backend.server.txn.ServerTxnCoordinator;
import io.github.rawwise.backend.server.txn.ServerTxnCoordinatorGrpcService;
import io.github.rawwise.backend.spark.RawWiseDataSource;
import io.github.rawwise.backend.spark.routing.SparkRoutingBridge;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.writer.WriterFactoryBootstrap;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.Metastore;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

/**
 * Embedded (single-process) Backend launcher using InProcess gRPC.
 *
 * <p>Creates SparkSession (local[*]), all Backend gRPC services, and InProcess Executor — all
 * communication within the JVM uses InProcess channels (zero TCP ports). Only Metastore gRPC (for
 * Spark plugin) uses a real port (9001).
 *
 * <p>Usage from Frontend Main:
 *
 * <pre>{@code
 * EmbeddedBackend backend = new EmbeddedBackend(config);
 * backend.start();
 * // Frontend connects via GrpcBackendClient.forInProcess(backend.backendInProcName(), 60)
 * // FastPath connects via GrpcExecutorStub with InProcess channel
 * }</pre>
 */
@Slf4j
public class EmbeddedBackend implements AutoCloseable {

    public static final String BACKEND_INPROC_NAME = "rawwise-embedded-backend";
    public static final String EXECUTOR_INPROC_NAME = "rawwise-embedded-executor";

    private final SparkSession spark;
    private final InMemorySessionStore sessionStore;
    private final SparkJobScheduler scheduler;
    private final Server grpcServer;
    private final Server executorServer;
    private final AxiomServiceImpl axiomService;
    private final List<ManagedChannel> managedChannels = new ArrayList<>();

    public EmbeddedBackend(RawWiseConfig config) {
        StorageRegistry.registerDefaults();
        WriterFactoryBootstrap.registerAll();

        this.spark = createEmbeddedSparkSession(config);

        this.axiomService = new AxiomServiceImpl();

        this.sessionStore = new InMemorySessionStore();
        AtomicInteger queueLength = new AtomicInteger(0);
        DefaultLoadCollector loadCollector = new DefaultLoadCollector(queueLength, spark);
        BackpressureManager backpressureManager =
                new BackpressureManager(
                        loadCollector,
                        config.get(RawWiseOptions.BACKPRESSURE_WARNING_THRESHOLD),
                        config.get(RawWiseOptions.BACKPRESSURE_THROTTLE_THRESHOLD),
                        config.get(RawWiseOptions.BACKPRESSURE_REJECT_THRESHOLD),
                        Map.of(
                                JobCategory.INTERACTIVE_QUERY,
                                        config.get(RawWiseOptions.BACKPRESSURE_INTERACTIVE_PERMITS),
                                JobCategory.BATCH_INGESTION,
                                        config.get(RawWiseOptions.BACKPRESSURE_BATCH_PERMITS),
                                JobCategory.TRAINING_SCAN,
                                        config.get(RawWiseOptions.BACKPRESSURE_TRAINING_PERMITS),
                                JobCategory.INFERENCE_REQUEST,
                                        config.get(RawWiseOptions.BACKPRESSURE_INFERENCE_PERMITS)),
                        Map.of(
                                JobCategory.INTERACTIVE_QUERY,
                                        config.get(
                                                RawWiseOptions.BACKPRESSURE_INTERACTIVE_TIMEOUT_MS),
                                JobCategory.BATCH_INGESTION,
                                        config.get(RawWiseOptions.BACKPRESSURE_BATCH_TIMEOUT_MS),
                                JobCategory.TRAINING_SCAN,
                                        config.get(RawWiseOptions.BACKPRESSURE_TRAINING_TIMEOUT_MS),
                                JobCategory.INFERENCE_REQUEST,
                                        config.get(
                                                RawWiseOptions.BACKPRESSURE_INFERENCE_TIMEOUT_MS)),
                        config.get(RawWiseOptions.BACKPRESSURE_TRAINING_DEGRADE_THRESHOLD));
        JobEventLog eventLog = new JobEventLog();
        this.scheduler =
                new SparkJobScheduler(
                        spark, sessionStore, queueLength, backpressureManager, eventLog);

        QueryAuthorizer authorizer = new QueryAuthorizer();

        TxnEngineConfig txnEngineConfig = new TxnEngineConfig();
        RawWiseDataSource.ServerTxnCoordinatorHolder.setConfig(txnEngineConfig);

        ServerTxnCoordinator serverTxnCoordinator = null;

        // InProcess Executor — shared by Axiom stubs and FastPath
        ExecutorPointQueryServiceImpl executorService = new ExecutorPointQueryServiceImpl(config);
        ManagedChannel executorChannel =
                InProcessChannelBuilder.forName(EXECUTOR_INPROC_NAME).directExecutor().build();
        managedChannels.add(executorChannel);
        List<
                        io.github.rawwise.server.ExecutorPointQueryServiceGrpc
                                .ExecutorPointQueryServiceBlockingStub>
                stubs =
                        List.of(
                                io.github.rawwise.server.ExecutorPointQueryServiceGrpc
                                        .newBlockingStub(executorChannel));
        axiomService.setExecutorStubs(stubs);

        // Wire EmbeddingProvider for vector search
        io.github.rawwise.engine.vector.EmbeddingProvider ep =
                io.github.rawwise.engine.vector.EmbeddingProviderFactory
                        .createFromSystemProperties();
        axiomService.setEmbeddingProvider(ep);
        log.info("EmbeddedBackend: EmbeddingProvider wired: model={}", ep.modelIdentifier());

        if (txnEngineConfig.getEngine() == TxnEngineConfig.Engine.PLAN_A) {
            try {
                var routingCoordinator = SparkRoutingBridge.requireCoordinator();
                Map<String, ManagedChannel> executorChannelMap = new HashMap<>();
                executorChannelMap.put("0", executorChannel);
                serverTxnCoordinator =
                        new DefaultServerTxnCoordinator(executorChannelMap, routingCoordinator, 30);
                RawWiseDataSource.ServerTxnCoordinatorHolder.setCoordinator(serverTxnCoordinator);
            } catch (Exception e) {
                log.warn(
                        "Failed to initialize ServerTxnCoordinator in embedded mode: {}",
                        e.getMessage());
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        io.grpc.ServerBuilder rawBuilder =
                InProcessServerBuilder.forName(BACKEND_INPROC_NAME)
                        .maxInboundMessageSize(64 * 1024 * 1024)
                        .addService(
                                new QueryServiceImpl(
                                        sessionStore, scheduler, backpressureManager, authorizer))
                        .addService(
                                new ClusterManagementServiceImpl(
                                        scheduler,
                                        backpressureManager,
                                        config.get(
                                                        RawWiseOptions
                                                                .BACKEND_SPARK_FAIR_SCHEDULER_ENABLED)
                                                ? "FAIR"
                                                : "FIFO"))
                        .addService(new JobManagementServiceImpl())
                        .addService(new SessionManagementServiceImpl())
                        .addService(new TableManagementServiceImpl())
                        .addService(new MonitorServiceImpl(scheduler, spark))
                        .addService(new TrainingDataServiceImpl())
                        .addService(new InferenceServiceImpl())
                        .addService(axiomService);

        if (serverTxnCoordinator != null) {
            rawBuilder =
                    rawBuilder.addService(
                            new ServerTxnCoordinatorGrpcService(serverTxnCoordinator));
        }

        this.grpcServer = rawBuilder.build();

        this.executorServer =
                InProcessServerBuilder.forName(EXECUTOR_INPROC_NAME)
                        .directExecutor()
                        .addService(executorService)
                        .build();

        txnEngineConfig.logStartupInfo();
    }

    public void start() throws Exception {
        grpcServer.start();
        executorServer.start();
        scheduler.start();

        try {
            io.github.rawwise.axiom.pipeline.graph.GraphQuery gq =
                    io.github.rawwise.backend.shared.BackendSharedRegistry.getGraphQuery();
            if (gq != null) {
                axiomService.setGraphQueryEngine(gq);
                log.info(
                        "EmbeddedBackend: GraphQuery injected from BackendSharedRegistry (rescue.enabled={}, boost={})",
                        System.getProperty("rawwise.axiom.graph.rescue.enabled", "false"),
                        System.getProperty("rawwise.axiom.graph.boost", "0.1"));
            } else {
                log.info(
                        "EmbeddedBackend: no GraphQuery available (graph-recall disabled, falling back to vector+keyword)");
            }
        } catch (RuntimeException e) {
            log.warn("EmbeddedBackend: GraphQuery injection failed: {}", e.getMessage());
        }

        setupAxiomNativeFeatures();

        log.info(
                "Embedded Backend started: InProcess gRPC (backend={}, executor={}), Spark master local[*]",
                BACKEND_INPROC_NAME,
                EXECUTOR_INPROC_NAME);
    }

    /**
     * Wire AI-native features (L0 summarization, fact extraction, semantic session, query
     * reformulation) into AxiomServiceImpl. Mirrors the setup in
     * backend/server/Main.setupAxiomStubs() so embedded/benchmark mode has parity with standalone
     * Backend Server.
     */
    private void setupAxiomNativeFeatures() {
        boolean l0SummarizationEnabled =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.axiom.l0summarization.enabled", "false"));
        boolean factExtractionEnabled =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.axiom.fact.extraction.enabled", "false"));
        boolean semanticSessionEnabled =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.axiom.semantic.session.enabled", "false"));
        if (l0SummarizationEnabled || factExtractionEnabled || semanticSessionEnabled) {
            String l0Endpoint = System.getProperty("rawwise.axiom.l0summarization.endpoint", "");
            String l0ApiKey = System.getProperty("rawwise.axiom.l0summarization.api-key", "");
            String l0Model =
                    System.getProperty("rawwise.axiom.l0summarization.model", "gpt-4o-mini");
            if (!l0Endpoint.isEmpty()) {
                try {
                    io.github.rawwise.axiom.memory.HttpSummarizationService summarizationService =
                            new io.github.rawwise.axiom.memory.HttpSummarizationService(
                                    l0Endpoint, l0ApiKey, l0Model);
                    io.github.rawwise.backend.shared.BackendSharedRegistry.setSummarizationService(
                            summarizationService);
                    log.info(
                            "EmbeddedBackend: SummarizationService injected: endpoint={} model={} (l0={} fact={})",
                            l0Endpoint,
                            l0Model,
                            l0SummarizationEnabled,
                            factExtractionEnabled);
                } catch (Exception e) {
                    log.warn(
                            "EmbeddedBackend: SummarizationService creation FAILED: {}",
                            e.getMessage());
                }
            } else {
                log.warn(
                        "EmbeddedBackend: L0 summarization/fact extraction enabled but no endpoint configured");
            }
        }

        boolean llmReformulationEnabled =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.axiom.queryreformulation.enabled", "false"));
        if (llmReformulationEnabled) {
            String reformEndpoint =
                    System.getProperty("rawwise.axiom.queryreformulation.endpoint", "");
            String reformApiKey =
                    System.getProperty("rawwise.axiom.queryreformulation.api-key", "");
            String reformModel =
                    System.getProperty("rawwise.axiom.queryreformulation.model", "gpt-4o-mini");
            if (!reformEndpoint.isEmpty()) {
                try {
                    io.github.rawwise.axiom.pipeline.reformulation.HttpLlmClient llmClient =
                            new io.github.rawwise.axiom.pipeline.reformulation.HttpLlmClient(
                                    reformEndpoint,
                                    reformApiKey,
                                    java.time.Duration.ofSeconds(30),
                                    2);
                    io.github.rawwise.axiom.pipeline.reformulation.ReformulationConfig
                            reformConfig =
                                    io.github.rawwise.axiom.pipeline.reformulation
                                            .ReformulationConfig.builder()
                                            .enabled(true)
                                            .expandN(4)
                                            .cacheTtl(java.time.Duration.ofMinutes(60))
                                            .cacheSize(10_000)
                                            .modelName(reformModel)
                                            .maxTokens(200)
                                            .build();
                    io.github.rawwise.axiom.pipeline.reformulation.DefaultQueryReformulationStage
                            reformStage =
                                    new io.github.rawwise.axiom.pipeline.reformulation
                                            .DefaultQueryReformulationStage(
                                            llmClient, reformConfig);
                    axiomService.setQueryReformulationStage(reformStage);
                    log.info(
                            "EmbeddedBackend: LLM query reformulation enabled: endpoint={} model={}",
                            reformEndpoint,
                            reformModel);
                } catch (Exception e) {
                    log.warn(
                            "EmbeddedBackend: LLM query reformulation setup FAILED: {}",
                            e.getMessage());
                }
            } else {
                log.warn(
                        "EmbeddedBackend: LLM query reformulation enabled but no endpoint configured");
            }
        }
    }

    public String backendInProcName() {
        return BACKEND_INPROC_NAME;
    }

    public String executorInProcName() {
        return EXECUTOR_INPROC_NAME;
    }

    /**
     * Inject Metastore into the DynamicMmapStore so that ingestion/derivation engines can be
     * created. Must be called after Frontend Main creates the Metastore.
     */
    public void setMetastore(Metastore metastore) {
        String warehousePath = io.github.rawwise.governance.config.WarehousePathResolver.resolve();
        DynamicMmapStore store =
                DynamicMmapStore.getInstance(
                        warehousePath, "spark", RawWiseConfig.fromMap(Map.of()));
        store.setMetastore(metastore);
        log.info(
                "EmbeddedBackend: Metastore injected into DynamicMmapStore (warehouse={})",
                warehousePath);
    }

    @Override
    public void close() {
        log.info("Embedded Backend shutting down");
        if (axiomService != null) {
            axiomService.close();
        }
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (executorServer != null) {
            executorServer.shutdown();
            try {
                if (!executorServer.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    executorServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        for (ManagedChannel ch : managedChannels) {
            ch.shutdown();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (spark != null) {
            spark.stop();
        }
        io.github.rawwise.engine.storage.DynamicMmapStoreFactory.closeAll();
        log.info("Embedded Backend shutdown complete");
    }

    private static SparkSession createEmbeddedSparkSession(RawWiseConfig config) {
        String master = config.get(RawWiseOptions.BACKEND_SPARK_MASTER);
        String driverMemory = config.get(RawWiseOptions.BACKEND_SPARK_DRIVER_MEMORY);

        String warehousePath;
        try {
            warehousePath = io.github.rawwise.governance.config.WarehousePathResolver.resolve();
        } catch (IllegalStateException e) {
            String legacy = config.get(RawWiseOptions.CATALOG_WAREHOUSE_PATH);
            if (legacy != null && !legacy.equals("/tmp/rawwise/warehouse")) {
                warehousePath = legacy;
            } else {
                throw e;
            }
        }
        System.setProperty("rawwise.warehouse.path", warehousePath);

        String driverHost = config.get(RawWiseOptions.BACKEND_SPARK_DRIVER_HOST);
        if (driverHost == null || driverHost.isBlank()) {
            try {
                driverHost = java.net.InetAddress.getLocalHost().getHostAddress();
            } catch (Exception e) {
                log.warn("Failed to detect local host address, falling back to 127.0.0.1", e);
                driverHost = "127.0.0.1";
            }
        }

        var builder =
                SparkSession.builder()
                        .appName("RawWiseEmbedded")
                        .master(master)
                        .config("spark.driver.memory", driverMemory)
                        .config("spark.driver.bindAddress", "0.0.0.0")
                        .config("spark.driver.host", driverHost)
                        .config("spark.local.ip", driverHost)
                        .config("spark.executor.memory", "1g")
                        .config("spark.executor.cores", "1")
                        .config("spark.driver.maxResultSize", "1g")
                        .config("spark.sql.adaptive.enabled", "true")
                        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .config(
                                "spark.plugins",
                                "io.github.rawwise.backend.spark.routing.RawWiseSparkPlugin")
                        .config("spark.sql.warehouse.dir", warehousePath)
                        .config("spark.sql.defaultCatalog", "rawwise")
                        .config(
                                "spark.executor.extraJavaOptions",
                                "-Drawwise.warehouse.path="
                                        + warehousePath
                                        + " -Drawwise.storage.lmdb.initial-map-size-mb=256"
                                        + " -Drawwise.storage.lmdb.max-map-size-mb=4096")
                        .config(
                                "spark.driver.extraJavaOptions",
                                "-Drawwise.warehouse.path="
                                        + warehousePath
                                        + " -Drawwise.storage.lmdb.initial-map-size-mb=256"
                                        + " -Drawwise.storage.lmdb.max-map-size-mb=4096")
                        .config("spark.rawwise.executor.grpc.port", "0")
                        .config("spark.rawwise.write.local", "true")
                        .config("spark.rawwise.storage.lmdb.initial-map-size-mb", "16")
                        .config("spark.rawwise.storage.lmdb.max-map-size-mb", "256");

        boolean fairSchedulerEnabled =
                config.get(RawWiseOptions.BACKEND_SPARK_FAIR_SCHEDULER_ENABLED);
        if (fairSchedulerEnabled) {
            String allocFile = resolveClasspathToFile("fairscheduler.xml");
            builder.config("spark.scheduler.mode", "FAIR")
                    .config("spark.scheduler.allocation.file", allocFile);
            log.info("Embedded Spark FAIR scheduler enabled, allocation file: {}", allocFile);
        } else {
            log.info("Embedded Spark FAIR scheduler disabled, using FIFO");
        }

        String metastoreType =
                (String) config.asMap().getOrDefault("rawwise.metastore.type", "inmemory");
        if ("grpc".equals(metastoreType) || "rocksdb".equals(metastoreType)) {
            String grpcHost =
                    (String)
                            config.asMap().getOrDefault("rawwise.metastore.grpc-host", "localhost");
            int grpcPort = parseIntOrDefault(config.asMap(), "rawwise.metastore.grpc-port", 9001);
            builder.config(
                            "spark.sql.catalog.rawwise",
                            "io.github.rawwise.backend.spark.RawWiseSparkCatalog")
                    .config("spark.sql.catalog.rawwise.warehouse-path", warehousePath)
                    .config("spark.sql.catalog.rawwise.store-kind", "grpc")
                    .config("spark.sql.catalog.rawwise.metastore-grpc-host", grpcHost)
                    .config(
                            "spark.sql.catalog.rawwise.metastore-grpc-port",
                            String.valueOf(grpcPort));
            log.info("Registered rawwise catalog with gRPC Metastore at {}:{}", grpcHost, grpcPort);
        } else {
            builder.config(
                            "spark.sql.catalog.rawwise",
                            "io.github.rawwise.backend.spark.RawWiseSparkCatalog")
                    .config("spark.sql.catalog.rawwise.warehouse-path", warehousePath)
                    .config("spark.sql.catalog.rawwise.store-kind", "inmemory");
            log.info("Registered rawwise catalog with inmemory Metastore");
        }

        TxnEngineConfig txnConfigForBroadcast = new TxnEngineConfig();
        builder.config("spark.rawwise.txn.engine", txnConfigForBroadcast.getEngine().name());

        builder.config(
                "spark.sql.extensions", "io.github.rawwise.backend.spark.RawWiseSparkExtensions");

        log.info("Embedded SparkSession: master={}, warehouse={}", master, warehousePath);
        return builder.getOrCreate();
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

    private static String resolveClasspathToFile(String resourceName) {
        File classpathFile = new File("rawwise-core/target/classes/" + resourceName);
        if (classpathFile.exists()) {
            return classpathFile.getAbsolutePath();
        }
        File rootClasspathFile = new File("target/classes/" + resourceName);
        if (rootClasspathFile.exists()) {
            return rootClasspathFile.getAbsolutePath();
        }
        try (InputStream is =
                EmbeddedBackend.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (is == null) {
                throw new IllegalStateException("Classpath resource not found: " + resourceName);
            }
            Path temp = Files.createTempFile("rawwise-" + resourceName.replace(".xml", ""), ".xml");
            Files.copy(is, temp, StandardCopyOption.REPLACE_EXISTING);
            temp.toFile().deleteOnExit();
            return temp.toAbsolutePath().toString();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to resolve classpath resource to file: " + resourceName, e);
        }
    }
}
