package io.github.rawwise.backend.server;

import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.server.auth.AuthServerInterceptor;
import io.github.rawwise.backend.server.auth.MetastoreTokenValidator;
import io.github.rawwise.backend.server.auth.QueryAuthorizer;
import io.github.rawwise.backend.server.auth.SimpleTokenValidator;
import io.github.rawwise.backend.server.auth.TokenValidator;
import io.github.rawwise.backend.server.ha.BackendServerHaCoordinator;
import io.github.rawwise.backend.server.job.JobCategory;
import io.github.rawwise.backend.server.job.JobEventLog;
import io.github.rawwise.backend.server.job.JobScheduler;
import io.github.rawwise.backend.server.job.SparkJobScheduler;
import io.github.rawwise.backend.server.load.BackpressureManager;
import io.github.rawwise.backend.server.load.DefaultLoadCollector;
import io.github.rawwise.backend.server.load.LoadCollector;
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
import io.github.rawwise.backend.server.session.SessionStore;
import io.github.rawwise.backend.server.txn.DefaultServerTxnCoordinator;
import io.github.rawwise.backend.server.txn.ServerTxnCoordinator;
import io.github.rawwise.backend.server.txn.ServerTxnCoordinatorGrpcService;
import io.github.rawwise.backend.spark.RawWiseDataSource;
import io.github.rawwise.backend.spark.routing.SparkRoutingBridge;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.writer.WriterFactoryBootstrap;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingProviderFactory;
import io.github.rawwise.governance.auth.InternalTokenResolver;
import io.github.rawwise.governance.config.ConfigLoader;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.governance.metrics.RawWiseMetrics;
import io.github.rawwise.governance.telemetry.TelemetryConfig;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class Main {
    public static final String BACKEND_NODE_SHARD_ID_KEY = "rawwise.backend.node.shard-id";
    public static final String BACKEND_NODE_SERVER_HOST_KEY = "rawwise.backend.node.server-host";
    public static final String BACKEND_NODE_SERVER_PORT_KEY = "rawwise.backend.node.server-port";
    public static final String BACKEND_NODE_ID_KEY = "rawwise.backend.node.id";

    private final SparkSession spark;
    private final JobScheduler jobScheduler;
    private SessionStore sessionStore;
    private BackpressureManager backpressureManager;
    private LoadCollector loadCollector;
    private Server grpcServer;
    private ExecutorService executor;
    private int port;
    private final CountDownLatch shutdownLatch;
    private RawWiseConfig config;
    private AxiomServiceImpl axiomService;
    private HealthServer healthServer;
    private List<ManagedChannel> executorChannels = new ArrayList<>();
    private io.github.rawwise.axiom.pipeline.feedback.retrieval.RerankerTrainingScheduler
            rerankerScheduler;
    private io.github.rawwise.engine.vector.onnx.DefaultRerankerModel defaultRerankerModel;
    private io.github.rawwise.axiom.pipeline.graph.distributed.GraphCentroidRefreshScheduler
            graphCentroidScheduler;
    private io.github.rawwise.axiom.pipeline.graph.distributed.HybridGraphSearchService
            hybridSearchService;
    private final List<io.github.rawwise.metastore.GrpcMetastoreClient> managedMetastoreClients =
            new ArrayList<>();
    private BackendServerHaCoordinator haCoordinator;
    private io.github.rawwise.metastore.Metastore haMetastore;

    public Main(int port, RawWiseConfig config) {
        this(port, config, false);
    }

    public Main(int port, RawWiseConfig config, boolean noSpark) {
        this.port = port;
        this.config = config;
        this.shutdownLatch = new CountDownLatch(1);

        StorageRegistry.registerDefaults();
        WriterFactoryBootstrap.registerAll();

        if (noSpark) {
            this.spark = null;
            log.info("Spark disabled (no-spark mode)");
        } else {
            this.spark = createSparkSession(config);
        }

        this.axiomService = new AxiomServiceImpl();
        setupAxiomStubs(config);

        if (spark != null) {
            AtomicInteger queueLength = new AtomicInteger(0);
            this.loadCollector = new DefaultLoadCollector(queueLength, spark);
            this.sessionStore = new InMemorySessionStore();
            this.backpressureManager =
                    new BackpressureManager(
                            loadCollector,
                            config.get(RawWiseOptions.BACKPRESSURE_WARNING_THRESHOLD),
                            config.get(RawWiseOptions.BACKPRESSURE_THROTTLE_THRESHOLD),
                            config.get(RawWiseOptions.BACKPRESSURE_REJECT_THRESHOLD),
                            Map.of(
                                    JobCategory.INTERACTIVE_QUERY,
                                            config.get(
                                                    RawWiseOptions
                                                            .BACKPRESSURE_INTERACTIVE_PERMITS),
                                    JobCategory.BATCH_INGESTION,
                                            config.get(RawWiseOptions.BACKPRESSURE_BATCH_PERMITS),
                                    JobCategory.TRAINING_SCAN,
                                            config.get(
                                                    RawWiseOptions.BACKPRESSURE_TRAINING_PERMITS),
                                    JobCategory.INFERENCE_REQUEST,
                                            config.get(
                                                    RawWiseOptions.BACKPRESSURE_INFERENCE_PERMITS)),
                            Map.of(
                                    JobCategory.INTERACTIVE_QUERY,
                                            config.get(
                                                    RawWiseOptions
                                                            .BACKPRESSURE_INTERACTIVE_TIMEOUT_MS),
                                    JobCategory.BATCH_INGESTION,
                                            config.get(
                                                    RawWiseOptions.BACKPRESSURE_BATCH_TIMEOUT_MS),
                                    JobCategory.TRAINING_SCAN,
                                            config.get(
                                                    RawWiseOptions
                                                            .BACKPRESSURE_TRAINING_TIMEOUT_MS),
                                    JobCategory.INFERENCE_REQUEST,
                                            config.get(
                                                    RawWiseOptions
                                                            .BACKPRESSURE_INFERENCE_TIMEOUT_MS)),
                            config.get(RawWiseOptions.BACKPRESSURE_TRAINING_DEGRADE_THRESHOLD));
            JobEventLog eventLog = new JobEventLog();
            this.jobScheduler =
                    new SparkJobScheduler(
                            spark, sessionStore, queueLength, backpressureManager, eventLog);
        } else {
            this.loadCollector = null;
            this.backpressureManager = null;
            this.jobScheduler = null;
            this.sessionStore = new InMemorySessionStore();
        }

        this.executor =
                Executors.newFixedThreadPool(
                        config.get(RawWiseOptions.SERVER_GRPC_THREAD_POOL_SIZE));

        TokenValidator tokenValidator;
        String validatorType = config.get(RawWiseOptions.AUTH_VALIDATOR_TYPE);
        if ("metastore".equalsIgnoreCase(validatorType)) {
            io.github.rawwise.governance.auth.AuthMetastore authMetastore =
                    createAuthMetastore(config);
            tokenValidator = new MetastoreTokenValidator(authMetastore);
            log.info("Using MetastoreTokenValidator for token validation");
        } else {
            tokenValidator = new SimpleTokenValidator();
            log.info("Using SimpleTokenValidator for token validation");
        }
        AuthServerInterceptor authInterceptor = new AuthServerInterceptor(tokenValidator);
        QueryAuthorizer authorizer = new QueryAuthorizer();

        io.grpc.ServerInterceptor traceIdInterceptor =
                new io.grpc.ServerInterceptor() {
                    private static final Metadata.Key<String> TRACE_ID_KEY =
                            Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);

                    @Override
                    public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(
                            io.grpc.ServerCall<ReqT, RespT> call,
                            Metadata headers,
                            io.grpc.ServerCallHandler<ReqT, RespT> next) {
                        String traceId = headers.get(TRACE_ID_KEY);
                        if (traceId != null && !traceId.isBlank()) {
                            TelemetryConfig.setTraceId(traceId);
                        }
                        var delegate = next.startCall(call, headers);
                        return new io.grpc.ForwardingServerCallListener<>() {
                            @Override
                            protected io.grpc.ServerCall.Listener<ReqT> delegate() {
                                return delegate;
                            }

                            @Override
                            public void onComplete() {
                                TelemetryConfig.clearTraceId();
                                super.onComplete();
                            }

                            @Override
                            public void onCancel() {
                                TelemetryConfig.clearTraceId();
                                super.onCancel();
                            }
                        };
                    }
                };

        ServerBuilder<?> serverBuilder;
        boolean tlsEnabled = config.get(RawWiseOptions.BACKEND_TLS_ENABLED);
        String envMode = config.get(RawWiseOptions.ENV_MODE);
        if ("production".equalsIgnoreCase(envMode) && !tlsEnabled) {
            boolean explicitlyDisabled =
                    config.get(RawWiseOptions.BACKEND_TLS_EXPLICITLY_UNSAFE_DISABLE);
            if (!explicitlyDisabled) {
                throw new IllegalStateException(
                        "Production mode requires TLS. Set -Drawwise.backend.tls.enabled=true");
            }
            log.warn("╔══════════════════════════════════════════════════════╗");
            log.warn("║  TLS EXPLICITLY DISABLED IN PRODUCTION              ║");
            log.warn("║  THIS IS UNSAFE - DO NOT USE IN PRODUCTION          ║");
            log.warn("╚══════════════════════════════════════════════════════╝");
        }
        if (tlsEnabled) {
            String certPath = config.get(RawWiseOptions.BACKEND_TLS_CERT_PATH);
            String keyPath = config.get(RawWiseOptions.BACKEND_TLS_KEY_PATH);
            try {
                SslContext sslContext =
                        GrpcSslContexts.forServer(new File(certPath), new File(keyPath)).build();
                serverBuilder = NettyServerBuilder.forPort(port).sslContext(sslContext);
            } catch (Exception e) {
                throw new RuntimeException("Failed to configure TLS: " + e.getMessage(), e);
            }
        } else {
            serverBuilder = ServerBuilder.forPort(port);
        }

        TxnEngineConfig txnEngineConfig = new TxnEngineConfig();
        RawWiseDataSource.ServerTxnCoordinatorHolder.setConfig(txnEngineConfig);

        ServerTxnCoordinator serverTxnCoordinator = null;
        if (txnEngineConfig.getEngine() == TxnEngineConfig.Engine.PLAN_A && spark != null) {
            try {
                RoutingCoordinator routingCoordinator = SparkRoutingBridge.requireCoordinator();
                Map<String, ManagedChannel> executorChannelMap = new HashMap<>();
                for (int i = 0; i < executorChannels.size(); i++) {
                    executorChannelMap.put(String.valueOf(i), executorChannels.get(i));
                }
                int grpcTimeoutSeconds = 30;
                serverTxnCoordinator =
                        new DefaultServerTxnCoordinator(
                                executorChannelMap, routingCoordinator, grpcTimeoutSeconds);
                RawWiseDataSource.ServerTxnCoordinatorHolder.setCoordinator(serverTxnCoordinator);
            } catch (Exception e) {
                log.warn("Failed to initialize ServerTxnCoordinator: {}", e.getMessage());
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        ServerBuilder rawBuilder =
                serverBuilder
                        .executor(executor)
                        .keepAliveTime(30, java.util.concurrent.TimeUnit.SECONDS)
                        .keepAliveTimeout(10, java.util.concurrent.TimeUnit.SECONDS)
                        .maxInboundMessageSize(64 * 1024 * 1024)
                        .permitKeepAliveTime(10, java.util.concurrent.TimeUnit.SECONDS)
                        .permitKeepAliveWithoutCalls(true)
                        .addService(
                                ServerInterceptors.intercept(
                                        new QueryServiceImpl(
                                                sessionStore,
                                                jobScheduler,
                                                backpressureManager,
                                                authorizer),
                                        traceIdInterceptor,
                                        authInterceptor))
                        .addService(
                                ServerInterceptors.intercept(
                                        createClusterManagementService(config),
                                        traceIdInterceptor,
                                        authInterceptor))
                        .addService(
                                ServerInterceptors.intercept(
                                        new JobManagementServiceImpl(),
                                        traceIdInterceptor,
                                        authInterceptor))
                        .addService(
                                ServerInterceptors.intercept(
                                        new SessionManagementServiceImpl(),
                                        traceIdInterceptor,
                                        authInterceptor))
                        .addService(
                                ServerInterceptors.intercept(
                                        new TableManagementServiceImpl(),
                                        traceIdInterceptor,
                                        authInterceptor))
                        .addService(
                                ServerInterceptors.intercept(
                                        new MonitorServiceImpl(
                                                (SparkJobScheduler) jobScheduler, spark),
                                        traceIdInterceptor,
                                        authInterceptor))
                        .addService(
                                ServerInterceptors.intercept(
                                        new TrainingDataServiceImpl(),
                                        traceIdInterceptor,
                                        authInterceptor))
                        .addService(
                                ServerInterceptors.intercept(
                                        new InferenceServiceImpl(),
                                        traceIdInterceptor,
                                        authInterceptor))
                        .addService(
                                ServerInterceptors.intercept(
                                        axiomService, traceIdInterceptor, authInterceptor))
                        // FastPath executor service: no authInterceptor (Frontend internal call, no
                        // token needed)
                        .addService(
                                ServerInterceptors.intercept(
                                        new ExecutorPointQueryServiceImpl(config),
                                        traceIdInterceptor));
        if (serverTxnCoordinator != null) {
            rawBuilder =
                    rawBuilder.addService(
                            ServerInterceptors.intercept(
                                    new ServerTxnCoordinatorGrpcService(serverTxnCoordinator),
                                    traceIdInterceptor,
                                    authInterceptor));
        }
        this.grpcServer = rawBuilder.build();

        txnEngineConfig.logStartupInfo();
    }

    private ClusterManagementServiceImpl createClusterManagementService(RawWiseConfig cfg) {
        String schedulerMode =
                cfg.get(RawWiseOptions.BACKEND_SPARK_FAIR_SCHEDULER_ENABLED) ? "FAIR" : "FIFO";
        RoutingCoordinator routingCoordinator = null;
        try {
            routingCoordinator = SparkRoutingBridge.requireCoordinator();
        } catch (Exception e) {
            log.debug("RoutingCoordinator not available, ClusterManagementService without routing");
        }
        ClusterManagementServiceImpl svc;
        if (routingCoordinator != null) {
            svc =
                    new ClusterManagementServiceImpl(
                            jobScheduler, backpressureManager, schedulerMode, routingCoordinator);
        } else {
            svc =
                    new ClusterManagementServiceImpl(
                            jobScheduler, backpressureManager, schedulerMode);
        }
        svc.setCentroidSink(new io.github.rawwise.backend.replication.InMemoryCentroidSink());

        if (cfg.get(RawWiseOptions.BACKEND_HA_ENABLED)) {
            initHaCoordinator(cfg);
        }
        return svc;
    }

    private void initHaCoordinator(RawWiseConfig cfg) {
        Map<String, Object> haCfg = cfg.asMap();
        String metastoreType = (String) haCfg.getOrDefault("rawwise.metastore.type", "inmemory");
        if ("grpc".equals(metastoreType)) {
            String msHost = (String) haCfg.getOrDefault("rawwise.metastore.grpc-host", "localhost");
            int msPort = parseIntOrDefault(haCfg, "rawwise.metastore.grpc-port", 9001);
            haMetastore = createManagedMetastoreClient(msHost, msPort);
        } else if ("rocksdb".equals(metastoreType)) {
            String basePath =
                    (String)
                            haCfg.getOrDefault(
                                    "rawwise.metastore.base-path", "/tmp/rawwise/metastore");
            try {
                haMetastore = new io.github.rawwise.metastore.RocksdbMetastore(basePath);
            } catch (java.io.IOException e) {
                log.warn("Failed to create RocksdbMetastore for HA: {}", e.getMessage());
                return;
            }
        } else {
            log.warn(
                    "HA enabled with metastore.type={}, HA requires grpc or rocksdb metastore",
                    metastoreType);
            return;
        }

        String ownerId = cfg.get(RawWiseOptions.BACKEND_HA_OWNER_ID);
        if (ownerId == null || ownerId.isBlank()) {
            ownerId = "be-server-" + Integer.toHexString(config.hashCode()) + "-" + port;
        }
        String advertiseAddress = cfg.get(RawWiseOptions.BACKEND_HA_ADVERTISE_ADDRESS);
        if (advertiseAddress == null || advertiseAddress.isBlank()) {
            try {
                advertiseAddress =
                        java.net.InetAddress.getLocalHost().getHostAddress() + ":" + port;
            } catch (Exception e) {
                advertiseAddress = "localhost:" + port;
            }
        }

        long leaseTtl = cfg.get(RawWiseOptions.BACKEND_HA_LEASE_TTL_MS);
        long renewInterval = cfg.get(RawWiseOptions.BACKEND_HA_RENEW_INTERVAL_MS);
        long standbyPoll = cfg.get(RawWiseOptions.BACKEND_HA_STANDBY_POLL_MS);

        haCoordinator =
                new BackendServerHaCoordinator(
                        haMetastore,
                        ownerId,
                        advertiseAddress,
                        leaseTtl,
                        renewInterval,
                        standbyPoll);
        log.info(
                "BackendServerHaCoordinator created: ownerId={} address={} ttl={}ms renew={}ms standbyPoll={}ms",
                ownerId,
                advertiseAddress,
                leaseTtl,
                renewInterval,
                standbyPoll);
    }

    public void start() throws Exception {
        if (haCoordinator != null) {
            boolean acquired = haCoordinator.tryAcquireActive();
            haCoordinator.start();
            log.info("HA coordinator started, acquiredActive={}", acquired);
        }

        grpcServer.start();
        if (jobScheduler != null) {
            jobScheduler.start();
        }

        int healthPort = config.get(RawWiseOptions.BACKEND_HEALTH_PORT);
        healthServer = new HealthServer(healthPort, spark);

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        log.info("Backend server started on port {}", port);

        shutdownLatch.await();
    }

    private void setupAxiomStubs(RawWiseConfig config) {
        Map<String, Object> cfg = config.asMap();
        String executorHost =
                (String) cfg.getOrDefault("rawwise.backend.executor.grpc.host", "localhost");
        int executorGrpcPort = parseIntOrDefault(cfg, "rawwise.backend.executor.grpc.port", 9091);
        int numExecutors = parseIntOrDefault(cfg, "rawwise.backend.executor.count", 1);
        boolean executorTlsEnabled = config.get(RawWiseOptions.BACKEND_EXECUTOR_TLS_ENABLED);
        String internalToken = InternalTokenResolver.resolve();

        List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs =
                new ArrayList<>();
        for (int i = 0; i < numExecutors; i++) {
            // use NettyChannelBuilder with directExecutor + flowControlWindow,
            // with Frontend → BE Node FastPath path consistent with, eliminating HTTP/2
            // backpressure waitandthread switching overhead
            io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder channelBuilder =
                    io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.forAddress(
                                    executorHost, executorGrpcPort + i)
                            .maxInboundMessageSize(64 * 1024 * 1024)
                            .flowControlWindow(8 * 1024 * 1024)
                            .keepAliveTime(30, java.util.concurrent.TimeUnit.SECONDS)
                            .keepAliveTimeout(10, java.util.concurrent.TimeUnit.SECONDS)
                            .keepAliveWithoutCalls(true)
                            .directExecutor();
            if (executorTlsEnabled) {
                channelBuilder.useTransportSecurity();
            } else {
                channelBuilder.usePlaintext();
            }
            ManagedChannel channel = channelBuilder.build();
            executorChannels.add(channel);

            var stub = ExecutorPointQueryServiceGrpc.newBlockingStub(channel);
            if (internalToken != null && !internalToken.isBlank()) {
                Metadata header = new Metadata();
                header.put(
                        Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                        internalToken);
                stub =
                        stub.withInterceptors(
                                io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(header));
            }
            stubs.add(stub);
        }
        axiomService.setExecutorStubs(stubs);

        EmbeddingProvider ep = EmbeddingProviderFactory.createFromSystemProperties();

        int embeddingDimension = EmbeddingProviderFactory.DEFAULT_DIMENSION;
        try {
            String dimStr = System.getProperty(EmbeddingProviderFactory.KEY_DIMENSION);
            if (dimStr != null && !dimStr.isBlank()) {
                embeddingDimension = Integer.parseInt(dimStr.trim());
                if (embeddingDimension <= 0)
                    embeddingDimension = EmbeddingProviderFactory.DEFAULT_DIMENSION;
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid embedding dimension property, using default: {}", e.getMessage());
        }

        EmbeddingProvider activeProvider = ep;
        io.github.rawwise.engine.vector.onnx.ReindexCoordinator reindexCoordinator = null;
        io.github.rawwise.engine.vector.onnx.EmbeddingProviderSwitcher embeddingSwitcher = null;

        try {
            String msHost = (String) cfg.getOrDefault("rawwise.metastore.grpc-host", "localhost");
            int msPort = parseIntOrDefault(cfg, "rawwise.metastore.grpc-port", 9001);
            io.github.rawwise.metastore.GrpcMetastoreClient embeddingMs =
                    createManagedMetastoreClient(msHost, msPort);

            io.github.rawwise.engine.vector.onnx.EmbeddingProviderRegistry embeddingRegistry =
                    new io.github.rawwise.engine.vector.onnx.EmbeddingProviderRegistry(embeddingMs);
            embeddingRegistry.registerCurrentModel(
                    ep.modelIdentifier(),
                    io.github.rawwise.engine.vector.onnx.OnnxEmbeddingConfig.builder()
                            .enabled(true)
                            .dimension(embeddingDimension)
                            .modelPath(
                                    System.getProperty(
                                            EmbeddingProviderFactory.KEY_ONNX_MODEL_PATH, ""))
                            .executionProvider(
                                    System.getProperty(
                                            EmbeddingProviderFactory.KEY_ONNX_EXECUTION_PROVIDER,
                                            "CPU"))
                            .build());

            embeddingSwitcher =
                    new io.github.rawwise.engine.vector.onnx.EmbeddingProviderSwitcher(
                            ep,
                            new io.github.rawwise.engine.vector.NoOpEmbeddingProvider(
                                    embeddingDimension));

            reindexCoordinator =
                    new io.github.rawwise.engine.vector.onnx.ReindexCoordinator(embeddingMs);

            embeddingSwitcher.setReindexCoordinator(reindexCoordinator);

            activeProvider = embeddingSwitcher;
            log.info(
                    "Embedding wiring complete: registry registered modelId={}, switcher={}, reindex={}",
                    ep.modelIdentifier(),
                    embeddingSwitcher.modelIdentifier(),
                    reindexCoordinator.getClass().getSimpleName());
        } catch (Exception e) {
            log.warn(
                    "Embedding provider registry/switcher/reindex wiring failed, using raw provider: {}",
                    e.getMessage());
        }

        boolean asyncEnabled =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.embedding.async.enabled", "false"));
        if (asyncEnabled
                && ep instanceof io.github.rawwise.engine.vector.HttpEmbeddingProvider httpEp) {
            int asyncThreadPool =
                    Integer.parseInt(
                            System.getProperty("rawwise.embedding.async.thread-pool-size", "4"));
            int asyncBatchSize =
                    Integer.parseInt(
                            System.getProperty(
                                    EmbeddingProviderFactory.KEY_BATCH_SIZE,
                                    String.valueOf(EmbeddingProviderFactory.DEFAULT_BATCH_SIZE)));
            long asyncTimeoutMs =
                    Long.parseLong(
                            System.getProperty(
                                    EmbeddingProviderFactory.KEY_TIMEOUT_MS,
                                    String.valueOf(EmbeddingProviderFactory.DEFAULT_TIMEOUT_MS)));
            io.github.rawwise.engine.vector.AsyncEmbeddingProvider asyncEp =
                    new io.github.rawwise.engine.vector.AsyncEmbeddingProvider(
                            httpEp, asyncThreadPool, asyncBatchSize, asyncTimeoutMs);
            activeProvider = asyncEp;
            log.info(
                    "AsyncEmbeddingProvider enabled: threadPool={} batchSize={} timeoutMs={}",
                    asyncThreadPool,
                    asyncBatchSize,
                    asyncTimeoutMs);
        }

        axiomService.setEmbeddingProvider(activeProvider);

        if (Boolean.parseBoolean(
                System.getProperty("rawwise.vector.index-manager.enabled", "false"))) {
            try {
                boolean adaptiveRouting =
                        Boolean.parseBoolean(
                                System.getProperty(
                                        "rawwise.vector.adaptive-routing.enabled", "true"));
                String warehousePath =
                        System.getProperty("rawwise.warehouse.path", "/tmp/rawwise/warehouse");
                io.github.rawwise.engine.vector.JVectorConfig jVectorConfig =
                        new io.github.rawwise.engine.vector.JVectorConfig(
                                "disk",
                                embeddingDimension,
                                32,
                                10,
                                Integer.getInteger("rawwise.vector.hnsw.ef-search", 50),
                                warehousePath + "/vector",
                                10000,
                                10000,
                                500000);
                io.github.rawwise.engine.vector.VectorIndexManager indexManager =
                        new io.github.rawwise.engine.vector.VectorIndexManager(
                                new io.github.rawwise.engine.vector.IvfPqConfig(
                                        embeddingDimension, 1024, 64, 32, 10, 20, 10000, 42, 3),
                                jVectorConfig,
                                false,
                                java.nio.file.Path.of(warehousePath),
                                adaptiveRouting);
                io.github.rawwise.axiom.pipeline.AxiomEngineRegistry.setVectorIndexManager(
                        indexManager);
                log.info(
                        "VectorIndexManager created and injected (tableFormatMode=false, JVector mode, adaptiveRouting={})",
                        adaptiveRouting);

                if (reindexCoordinator != null && embeddingSwitcher != null) {
                    try {
                        io.github.rawwise.engine.storage.KnowledgeStore reindexKs =
                                io.github.rawwise.backend.shared.BackendSharedRegistry
                                        .getKnowledgeStore();
                        if (reindexKs != null) {
                            String rMsHost =
                                    (String)
                                            cfg.getOrDefault(
                                                    "rawwise.metastore.grpc-host", "localhost");
                            int rMsPort =
                                    parseIntOrDefault(cfg, "rawwise.metastore.grpc-port", 9001);
                            io.github.rawwise.metastore.GrpcMetastoreClient reindexMs =
                                    createManagedMetastoreClient(rMsHost, rMsPort);
                            final io.github.rawwise.engine.vector.onnx.EmbeddingProviderSwitcher
                                    switcherRef = embeddingSwitcher;
                            reindexCoordinator.registerDefaultExecutor(
                                    new io.github.rawwise.engine.vector.onnx
                                            .KnowledgeStoreReindexExecutor(
                                            indexManager,
                                            () ->
                                                    switcherRef.getShadow() != null
                                                            ? switcherRef.getShadow()
                                                            : switcherRef.getCurrent(),
                                            reindexKs,
                                            reindexMs,
                                            "embedding"));
                            log.info(
                                    "KnowledgeStoreReindexExecutor registered with ReindexCoordinator");
                        } else {
                            log.info(
                                    "KnowledgeStoreReindexExecutor not registered: no KnowledgeStore in BackendSharedRegistry");
                        }
                    } catch (Exception exEx) {
                        log.warn("Failed to register KnowledgeStoreReindexExecutor", exEx);
                    }
                }
            } catch (Exception e) {
                log.warn(
                        "VectorIndexManager creation failed, falling back to default VectorIndex: {}",
                        e.getMessage());
            }
        }

        try {
            String msHost = (String) cfg.getOrDefault("rawwise.metastore.grpc-host", "localhost");
            int msPort = parseIntOrDefault(cfg, "rawwise.metastore.grpc-port", 9001);
            io.github.rawwise.metastore.GrpcMetastoreClient ms =
                    createManagedMetastoreClient(msHost, msPort);
            axiomService.loadIvfRouterFromMetastore(ms);
        } catch (Exception e) {
            log.warn(
                    "Could not load IvfRouter from metastore, using empty router: {}",
                    e.getMessage());
        }

        // Pull GraphQuery from BackendSharedRegistry (registered by Executor in same JVM),
        // inject into AxiomServiceImpl to enable graph recall third path in search.
        try {
            io.github.rawwise.axiom.pipeline.graph.GraphQuery gq =
                    io.github.rawwise.backend.shared.BackendSharedRegistry.getGraphQuery();
            if (gq != null) {
                axiomService.setGraphQueryEngine(gq);
                boolean rescueEnabled =
                        Boolean.parseBoolean(
                                System.getProperty("rawwise.axiom.graph.rescue.enabled", "false"));
                log.info(
                        "AxiomServiceImpl: GraphQuery injected from BackendSharedRegistry (rescue.enabled={}, boost={})",
                        rescueEnabled,
                        System.getProperty("rawwise.axiom.graph.boost", "0.1"));
            } else {
                log.info(
                        "AxiomServiceImpl: no GraphQuery available (graph-recall disabled, falling back to vector+keyword)");
            }
        } catch (RuntimeException e) {
            log.warn("AxiomServiceImpl: GraphQuery injection FAILED: {}", e.getMessage());
        }

        boolean l0SummarizationEnabled =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.axiom.l0summarization.enabled", "false"));
        boolean factExtractionEnabled =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.axiom.fact.extraction.enabled", "false"));
        boolean semanticSessionEnabled =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.axiom.semantic.session.enabled", "false"));
        // SummarizationService is shared by L0 summarization, Strategy D fact extraction, and
        // Session-Level Semantic Derivation
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
                            "SummarizationService injected: endpoint={} model={} (l0={} fact={})",
                            l0Endpoint,
                            l0Model,
                            l0SummarizationEnabled,
                            factExtractionEnabled);
                } catch (Exception e) {
                    log.warn("SummarizationService creation FAILED: {}", e.getMessage());
                }
            } else {
                log.warn(
                        "L0 summarization/fact extraction enabled but no endpoint configured (rawwise.axiom.l0summarization.endpoint)");
            }
        }

        io.github.rawwise.axiom.pipeline.reformulation.DefaultQueryReformulationStage reformStage =
                null;
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
                    reformStage =
                            new io.github.rawwise.axiom.pipeline.reformulation
                                    .DefaultQueryReformulationStage(llmClient, reformConfig);
                    axiomService.setQueryReformulationStage(reformStage);
                    log.info(
                            "LLM query reformulation enabled: endpoint={} model={}",
                            reformEndpoint,
                            reformModel);
                } catch (Exception e) {
                    log.warn("LLM query reformulation setup FAILED: {}", e.getMessage());
                }
            } else {
                log.warn(
                        "LLM query reformulation enabled but no endpoint configured (rawwise.axiom.queryreformulation.endpoint)");
            }
        }

        try {
            io.github.rawwise.engine.storage.KnowledgeStore ks =
                    io.github.rawwise.backend.shared.BackendSharedRegistry.getKnowledgeStore();
            if (ks == null) {
                ks = new io.github.rawwise.engine.storage.InMemoryKnowledgeStore();
                log.info(
                        "RetrievalFeedbackCollector: using InMemoryKnowledgeStore (no shared KS in BackendSharedRegistry)");
            }

            io.github.rawwise.axiom.pipeline.feedback.retrieval.RetrievalFeedbackCollector
                    feedbackCollector =
                            new io.github.rawwise.axiom.pipeline.feedback.retrieval
                                    .RetrievalFeedbackCollector(ks, "default");
            axiomService.setFeedbackCollector(feedbackCollector);

            double abPct =
                    io.github.rawwise.axiom.pipeline.feedback.retrieval.RerankerTrainingConfig
                            .defaults()
                            .abTrafficPercent();
            io.github.rawwise.axiom.pipeline.feedback.retrieval.RerankerAbSwitcher
                    rerankerSwitcher =
                            new io.github.rawwise.axiom.pipeline.feedback.retrieval
                                    .RerankerAbSwitcher(abPct);
            axiomService.setRerankerSwitcher(rerankerSwitcher);

            String rerankerModelPath =
                    System.getProperty("rawwise.reranker.default-model-path", "");
            if (!rerankerModelPath.isBlank()
                    && java.nio.file.Files.exists(java.nio.file.Path.of(rerankerModelPath))) {
                try {
                    this.defaultRerankerModel =
                            new io.github.rawwise.engine.vector.onnx.DefaultRerankerModel(
                                    rerankerModelPath);
                    axiomService.setRerankerModel(this.defaultRerankerModel);
                    log.info(
                            "DefaultRerankerModel wired: path={} fallback={}",
                            rerankerModelPath,
                            this.defaultRerankerModel.isFallback());
                } catch (Exception e) {
                    log.warn(
                            "Failed to load DefaultRerankerModel from {}: {}",
                            rerankerModelPath,
                            e.getMessage());
                }
            } else if (!rerankerModelPath.isBlank()) {
                log.warn(
                        "DefaultRerankerModel path configured but file not found: {}",
                        rerankerModelPath);
            }

            String msHost = (String) cfg.getOrDefault("rawwise.metastore.grpc-host", "localhost");
            int msPort = parseIntOrDefault(cfg, "rawwise.metastore.grpc-port", 9001);
            io.github.rawwise.metastore.Metastore rerankerMs =
                    createManagedMetastoreClient(msHost, msPort);

            final io.github.rawwise.axiom.pipeline.feedback.retrieval.RerankerTrainingScheduler[]
                    holder =
                            new io.github.rawwise.axiom.pipeline.feedback.retrieval
                                            .RerankerTrainingScheduler[1];
            io.github.rawwise.axiom.pipeline.feedback.retrieval.RerankerTrainingScheduler
                    scheduler =
                            new io.github.rawwise.axiom.pipeline.feedback.retrieval
                                    .RerankerTrainingScheduler(
                                    feedbackCollector,
                                    io.github.rawwise.axiom.pipeline.feedback.retrieval
                                            .RerankerTrainingConfig.defaults(),
                                    rerankerMs,
                                    () -> {
                                        if (holder[0] != null) {
                                            holder[0]
                                                    .latestModel()
                                                    .ifPresent(rerankerSwitcher::setCandidateModel);
                                        }
                                    });
            holder[0] = scheduler;
            scheduler.setKnowledgeStore(ks);
            scheduler.start();
            this.rerankerScheduler = scheduler;

            log.info(
                    "RetrievalFeedbackCollector + RerankerTrainingScheduler + RerankerAbSwitcher wired (KS impl={})",
                    ks.getClass().getSimpleName());
        } catch (Exception e) {
            log.warn("Retrieval feedback / reranker wiring FAILED: {}", e.getMessage());
        }

        // === Graph Search Wiring ===
        if (Boolean.parseBoolean(
                System.getProperty("rawwise.graph.centroid-router.enabled", "true"))) {
            try {
                String gmsHost =
                        (String) cfg.getOrDefault("rawwise.metastore.grpc-host", "localhost");
                int gmsPort = parseIntOrDefault(cfg, "rawwise.metastore.grpc-port", 9001);
                io.github.rawwise.metastore.GrpcMetastoreClient graphMs =
                        createManagedMetastoreClient(gmsHost, gmsPort);

                io.github.rawwise.axiom.pipeline.graph.distributed.MetastoreEntityCentroidStore
                        centroidStore =
                                new io.github.rawwise.axiom.pipeline.graph.distributed
                                        .MetastoreEntityCentroidStore(graphMs);

                io.github.rawwise.engine.storage.KnowledgeStore gks =
                        io.github.rawwise.backend.shared.BackendSharedRegistry.getKnowledgeStore();
                io.github.rawwise.axiom.pipeline.graph.distributed.NameIndexResolver nameResolver =
                        gks != null
                                ? new io.github.rawwise.axiom.pipeline.graph.distributed
                                        .KnowledgeStoreNameIndexResolver(gks)
                                : io.github.rawwise.axiom.pipeline.graph.distributed
                                        .NameIndexResolver.empty();

                io.github.rawwise.axiom.pipeline.graph.distributed.GraphCentroidRouter
                        graphCentroidRouter =
                                new io.github.rawwise.axiom.pipeline.graph.distributed
                                        .GraphCentroidRouter(centroidStore, nameResolver);

                long refreshPeriod =
                        Long.parseLong(
                                System.getProperty(
                                        "rawwise.graph.centroid-refresh.period-seconds", "30"));
                this.graphCentroidScheduler =
                        new io.github.rawwise.axiom.pipeline.graph.distributed
                                .GraphCentroidRefreshScheduler(graphCentroidRouter, refreshPeriod);
                this.graphCentroidScheduler.start();
                log.info("Graph centroid router enabled: refreshPeriod={}s", refreshPeriod);

                if (Boolean.parseBoolean(
                        System.getProperty("rawwise.graph.shard-coordinator.enabled", "false"))) {
                    io.github.rawwise.axiom.pipeline.graph.GraphQuery graphQuery =
                            io.github.rawwise.backend.shared.BackendSharedRegistry.getGraphQuery();
                    Map<
                                    io.github.rawwise.axiom.pipeline.graph.distributed.ShardId,
                                    io.github.rawwise.axiom.pipeline.graph.GraphQuery>
                            engines = new HashMap<>();
                    if (graphQuery != null) {
                        engines.put(
                                new io.github.rawwise.axiom.pipeline.graph.distributed.ShardId(0),
                                graphQuery);
                    }
                    io.github.rawwise.axiom.pipeline.graph.distributed.InProcessShardedGraphClient
                            shardedClient =
                                    new io.github.rawwise.axiom.pipeline.graph.distributed
                                            .InProcessShardedGraphClient(engines);

                    io.github.rawwise.axiom.pipeline.graph.distributed.GraphShardCoordinator
                            graphShardCoordinator =
                                    new io.github.rawwise.axiom.pipeline.graph.distributed
                                            .GraphShardCoordinator(
                                            graphCentroidRouter, shardedClient);

                    io.github.rawwise.axiom.pipeline.graph.distributed.GraphSearchCoordinatorImpl
                            graphSearchCoordinator =
                                    new io.github.rawwise.axiom.pipeline.graph.distributed
                                            .GraphSearchCoordinatorImpl(
                                            graphCentroidRouter, graphShardCoordinator);

                    if ("hybrid"
                            .equals(System.getProperty("rawwise.axiom.search.path", "legacy"))) {
                        io.github.rawwise.engine.vector.DistributedSearchCoordinator
                                distCoordinator = axiomService.getSearchCoordinator();
                        io.github.rawwise.engine.vector.IvfRouter ivfRouter =
                                axiomService.getIvfRouter();

                        io.github.rawwise.axiom.pipeline.graph.distributed
                                        .VectorSearchCoordinatorAdapter
                                vecAdapter =
                                        new io.github.rawwise.axiom.pipeline.graph.distributed
                                                .VectorSearchCoordinatorAdapter(
                                                activeProvider, distCoordinator, ivfRouter);

                        io.github.rawwise.axiom.pipeline.graph.distributed
                                        .KeywordSearchCoordinatorAdapter
                                kwAdapter =
                                        new io.github.rawwise.axiom.pipeline.graph.distributed
                                                .KeywordSearchCoordinatorAdapter(stubs);

                        io.github.rawwise.axiom.pipeline.graph.distributed.QueryReformulationStage
                                reformAdapter =
                                        reformStage != null
                                                ? new io.github.rawwise.axiom.pipeline.graph
                                                        .distributed.QueryReformulationAdapter(
                                                        reformStage)
                                                : io.github.rawwise.axiom.pipeline.graph.distributed
                                                        .QueryReformulationStage.identity();

                        long hybridTimeout =
                                Long.parseLong(
                                        System.getProperty(
                                                "rawwise.axiom.hybrid.timeout-ms", "800"));
                        this.hybridSearchService =
                                new io.github.rawwise.axiom.pipeline.graph.distributed
                                        .HybridGraphSearchService(
                                        vecAdapter,
                                        kwAdapter,
                                        graphSearchCoordinator,
                                        reformAdapter,
                                        hybridTimeout);
                        axiomService.setHybridSearchService(this.hybridSearchService);
                        log.info("HybridGraphSearchService wired: timeoutMs={}ms", hybridTimeout);
                    }
                }
            } catch (Exception e) {
                log.warn("Graph search wiring FAILED: {}", e.getMessage());
            }
        }

        log.info(
                "Axiom stubs configured: {} executor(s) at {}:{}, embeddingProvider={}",
                numExecutors,
                executorHost,
                executorGrpcPort,
                ep.modelIdentifier());
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
        try (InputStream is = Main.class.getClassLoader().getResourceAsStream(resourceName)) {
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

    private io.github.rawwise.metastore.GrpcMetastoreClient createManagedMetastoreClient(
            String host, int port) {
        var c = new io.github.rawwise.metastore.GrpcMetastoreClient(host, port);
        managedMetastoreClients.add(c);
        return c;
    }

    private static io.github.rawwise.governance.auth.AuthMetastore createAuthMetastore(
            RawWiseConfig config) {
        Map<String, Object> cfg = config.asMap();
        String metastoreType = (String) cfg.getOrDefault("rawwise.metastore.type", "inmemory");
        io.github.rawwise.metastore.Metastore store;
        if ("grpc".equals(metastoreType)) {
            String host = (String) cfg.getOrDefault("rawwise.metastore.grpc-host", "localhost");
            int port = parseIntOrDefault(cfg, "rawwise.metastore.grpc-port", 9001);
            store = new io.github.rawwise.metastore.GrpcMetastoreClient(host, port);
        } else {
            store = new io.github.rawwise.metastore.InMemoryMetastore();
        }
        return new io.github.rawwise.governance.auth.DefaultAuthMetastore(store);
    }

    public void shutdown() {
        log.info("Backend server shutting down");
        shutdownLatch.countDown();

        if (haCoordinator != null) {
            try {
                haCoordinator.stop();
            } catch (Exception e) {
                log.warn("Failed to stop HA coordinator cleanly: {}", e.getMessage());
            }
        }

        if (haMetastore instanceof io.github.rawwise.metastore.RocksdbMetastore rocks) {
            try {
                rocks.close();
            } catch (Exception e) {
                log.debug("Failed to close HA RocksdbMetastore", e);
            }
        }

        if (rerankerScheduler != null) {
            rerankerScheduler.stop();
        }

        if (graphCentroidScheduler != null) {
            graphCentroidScheduler.stop();
        }

        if (hybridSearchService != null) {
            hybridSearchService.close();
        }

        if (axiomService != null) {
            axiomService.close();
        }

        if (defaultRerankerModel != null) {
            try {
                defaultRerankerModel.close();
            } catch (Exception e) {
                log.debug("Failed to close DefaultRerankerModel", e);
            }
        }

        for (var c : managedMetastoreClients) {
            try {
                c.close();
            } catch (Exception e) {
                log.debug("Failed to close metastore client", e);
            }
        }

        if (healthServer != null) {
            healthServer.stop();
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

        for (ManagedChannel ch : executorChannels) {
            ch.shutdown();
        }

        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (jobScheduler != null) {
            jobScheduler.shutdown();
        }

        if (spark != null) {
            spark.stop();
        }

        io.github.rawwise.engine.storage.DynamicMmapStoreFactory.closeAll();

        log.info("Backend server shutdown complete");
    }

    private SparkSession createSparkSession(RawWiseConfig config) {
        String master = config.get(RawWiseOptions.BACKEND_SPARK_MASTER);
        String driverMemory = config.get(RawWiseOptions.BACKEND_SPARK_DRIVER_MEMORY);
        // Resolve warehouse path with fallback chain to prevent silent /tmp data loss:
        // 1. -Drawwise.warehouse.path (set by bin/rawwise-start.sh)
        // 2. RAWWISE_WAREHOUSE_PATH env var
        // 3. rawwise.catalog.warehouse-path config (legacy key)
        // 4. Throw if neither is set and -Drawwise.warehouse.allow-tmp-fallback != true
        String warehousePath;
        try {
            warehousePath = io.github.rawwise.governance.config.WarehousePathResolver.resolve();
        } catch (IllegalStateException e) {
            // Fallback to legacy CATALOG_WAREHOUSE_PATH if it has been explicitly configured
            // (not equal to its hardcoded default /tmp/rawwise/warehouse).
            String legacy = config.get(RawWiseOptions.CATALOG_WAREHOUSE_PATH);
            if (legacy != null && !legacy.equals("/tmp/rawwise/warehouse")) {
                warehousePath = legacy;
                System.setProperty("rawwise.warehouse.path", warehousePath);
                log.warn(
                        "Warehouse path resolved from legacy {} config; "
                                + "set -Drawwise.warehouse.path explicitly for clarity",
                        RawWiseOptions.CATALOG_WAREHOUSE_PATH.key());
            } else {
                throw e;
            }
        }
        // Propagate to system property so Spark Executor JVMs and other components inherit it.
        System.setProperty("rawwise.warehouse.path", warehousePath);
        String metastoreType =
                (String) config.asMap().getOrDefault("rawwise.metastore.type", "inmemory");

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
                        .appName("RawWiseBackendServer")
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
                                "-Drawwise.warehouse.path=" + warehousePath)
                        .config(
                                "spark.driver.extraJavaOptions",
                                "-Drawwise.warehouse.path=" + warehousePath);

        boolean fairSchedulerEnabled =
                config.get(RawWiseOptions.BACKEND_SPARK_FAIR_SCHEDULER_ENABLED);
        if (fairSchedulerEnabled) {
            String allocFile = resolveClasspathToFile("fairscheduler.xml");
            builder.config("spark.scheduler.mode", "FAIR")
                    .config("spark.scheduler.allocation.file", allocFile);
            log.info("Spark FAIR scheduler enabled, allocation file: {}", allocFile);
        } else {
            log.info(
                    "Spark FAIR scheduler disabled (rawwise.backend.spark.fair-scheduler.enabled=false), using FIFO");
        }

        int executorGrpcPort =
                parseIntOrDefault(config.asMap(), "rawwise.backend.executor.grpc.port", 9092);
        builder.config("spark.rawwise.executor.grpc.port", String.valueOf(executorGrpcPort));
        log.info("Executor gRPC port configured: {}", executorGrpcPort);

        if ("grpc".equals(metastoreType)) {
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
        log.info(
                "TxnEngine config broadcast to executors: engine={}",
                txnConfigForBroadcast.getEngine());

        // RawWiseSparkExtensions requires scala-maven-plugin; skip when not compiled
        try {
            Class.forName("io.github.rawwise.backend.spark.RawWiseSparkExtensions");
            builder.config(
                    "spark.sql.extensions",
                    "io.github.rawwise.backend.spark.RawWiseSparkExtensions");
            log.info("Registered RawWiseSparkExtensions for LazyUpdate rewrite rules");
        } catch (ClassNotFoundException e) {
            log.warn("RawWiseSparkExtensions not found on classpath, skipping Spark extensions");
        }

        return builder.getOrCreate();
    }

    private static Map<String, String> parseCliArgs(String[] args) {
        Map<String, String> overrides = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (i + 1 < args.length) overrides.put("grpc.backend.port", args[++i]);
                    break;
                case "--config":
                    if (i + 1 < args.length) System.setProperty("rawwise.config.file", args[++i]);
                    break;
                case "--master":
                    if (i + 1 < args.length)
                        overrides.put(RawWiseOptions.BACKEND_SPARK_MASTER.key(), args[++i]);
                    break;
                default:
                    break;
            }
        }
        return overrides;
    }

    public static void main(String[] args) {
        RawWiseConfig config = ConfigLoader.builder().cliOverrides(parseCliArgs(args)).load();

        String mode = null;
        for (int i = 0; i < args.length; i++) {
            if ("--mode".equals(args[i]) && i + 1 < args.length) {
                mode = args[i + 1];
                break;
            }
        }

        if ("node".equalsIgnoreCase(mode)) {
            log.info("Starting in NODE mode (standalone BE Node)");
            NodeModeLauncher nodeLauncher = new NodeModeLauncher(config);
            try {
                nodeLauncher.start();
            } catch (Exception e) {
                log.error("BE Node error", e);
                System.exit(1);
            }
            return;
        }

        var prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        RawWiseMetrics.initialize(prometheusRegistry);
        int prometheusPort = config.get(RawWiseOptions.METRICS_PROMETHEUS_PORT);
        try {
            new HTTPServer(prometheusPort);
            log.info("Prometheus HTTP server started on port {}", prometheusPort);
        } catch (Exception e) {
            log.warn(
                    "Failed to start Prometheus HTTP server on port {}: {}",
                    prometheusPort,
                    e.getMessage());
        }

        boolean noSpark = false;
        for (String arg : args) {
            if ("--no-spark".equals(arg)) {
                noSpark = true;
                break;
            }
        }

        int port = config.get(RawWiseOptions.FRONTEND_GRPC_BACKEND_PORT);
        Main server = new Main(port, config, noSpark);
        try {
            server.start();
        } catch (Exception e) {
            log.error("Server error", e);
        }
    }
}
