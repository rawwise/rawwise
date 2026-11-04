package io.github.rawwise.frontend.server;

import io.github.rawwise.axiom.memory.AgentIndexStore;
import io.github.rawwise.axiom.memory.AxiomClient;
import io.github.rawwise.axiom.memory.AxiomMemoryFacade;
import io.github.rawwise.axiom.memory.DecayPolicy;
import io.github.rawwise.axiom.memory.HttpAxiomClient;
import io.github.rawwise.axiom.memory.MemoryFacade;
import io.github.rawwise.axiom.memory.SummarizationService;
import io.github.rawwise.axiom.memory.SummarizationServiceFactory;
import io.github.rawwise.axiom.memory.ViewMetadataStore;
import io.github.rawwise.axiom.memory.semantic.GraphEngine;
import io.github.rawwise.axiom.memory.semantic.GraphEngineFactory;
import io.github.rawwise.axiom.memory.semantic.GraphSchema;
import io.github.rawwise.axiom.scheduler.AxiomScheduler;
import io.github.rawwise.axiom.scheduler.AxiomSchedulerConfig;
import io.github.rawwise.backend.server.EmbeddedBackend;
import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.engine.clean.CleanMetrics;
import io.github.rawwise.engine.compaction.CompactionManager;
import io.github.rawwise.engine.compaction.cleanup.FileCleanupManager;
import io.github.rawwise.engine.compaction.cleanup.FileGarbageCollector;
import io.github.rawwise.engine.compaction.recovery.CompactionRecoveryManager;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.spi.LocalStorage;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.writer.WriterFactoryBootstrap;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingProviderFactory;
import io.github.rawwise.engine.vector.InMemoryVectorIndex;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.execution.DefaultExecutionManager;
import io.github.rawwise.execution.ExecutionManager;
import io.github.rawwise.frontend.api.FrontendServer;
import io.github.rawwise.frontend.api.RateLimitFilter;
import io.github.rawwise.frontend.api.routes.AxiomKnowledgeRoutes;
import io.github.rawwise.frontend.api.routes.AxiomRouteGroup;
import io.github.rawwise.frontend.api.routes.ContextRoutes;
import io.github.rawwise.frontend.api.routes.ExecutionRoutes;
import io.github.rawwise.frontend.api.routes.MemoryRoutes;
import io.github.rawwise.frontend.api.routes.RoutingRoutes;
import io.github.rawwise.frontend.api.routes.StateRoutes;
import io.github.rawwise.frontend.api.routes.TrustRoutes;
import io.github.rawwise.frontend.api.session.SessionManager;
import io.github.rawwise.frontend.backend.BackendClient;
import io.github.rawwise.frontend.backend.BackendClientFactory;
import io.github.rawwise.frontend.backend.GrpcBackendClient;
import io.github.rawwise.frontend.backend.GrpcExecutorStub;
import io.github.rawwise.frontend.backend.MultiBackendClient;
import io.github.rawwise.frontend.config.FrontendOptions;
import io.github.rawwise.frontend.evaluator.DefaultQueryEvaluator;
import io.github.rawwise.frontend.evaluator.QueryEvaluator;
import io.github.rawwise.frontend.ir.IrRuntime;
import io.github.rawwise.frontend.ir.routing.RoutingTableManager;
import io.github.rawwise.frontend.ir.routing.RoutingTableWriter;
import io.github.rawwise.frontend.mcp.RawWiseMcpConfig;
import io.github.rawwise.frontend.mcp.RawWiseMcpPrompts;
import io.github.rawwise.frontend.mcp.RawWiseMcpResources;
import io.github.rawwise.frontend.mcp.RawWiseMcpServer;
import io.github.rawwise.frontend.mcp.RawWiseMcpTools;
import io.github.rawwise.frontend.scheduler.CompactionScheduler;
import io.github.rawwise.frontend.session.FrontendSessionStore;
import io.github.rawwise.governance.auth.AuthCrypto;
import io.github.rawwise.governance.auth.AuthFilter;
import io.github.rawwise.governance.auth.AuthMetastore;
import io.github.rawwise.governance.auth.AuthRoutes;
import io.github.rawwise.governance.auth.DefaultAuthMetastore;
import io.github.rawwise.governance.auth.DefaultProvenanceStore;
import io.github.rawwise.governance.auth.PermissionCache;
import io.github.rawwise.governance.auth.Principal;
import io.github.rawwise.governance.auth.ProvenanceAuditor;
import io.github.rawwise.governance.auth.ProvenanceStore;
import io.github.rawwise.governance.auth.TokenCache;
import io.github.rawwise.governance.config.ConfigLoader;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.governance.context.DefaultContextManager;
import io.github.rawwise.governance.metrics.RawWiseMetrics;
import io.github.rawwise.governance.trust.MerkleVerifier;
import io.github.rawwise.governance.trust.TrustEngine;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.MetastoreRegistry;
import io.github.rawwise.metastore.server.GrpcServer;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {

    private static Map<String, String> parseCliArgs(String[] args) {
        Map<String, String> overrides = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (i + 1 < args.length)
                        overrides.put(RawWiseOptions.FRONTEND_PORT.key(), args[++i]);
                    break;
                case "--backend-host":
                    if (i + 1 < args.length)
                        overrides.put(RawWiseOptions.FRONTEND_GRPC_BACKEND_HOST.key(), args[++i]);
                    break;
                case "--backend-port":
                    if (i + 1 < args.length)
                        overrides.put(RawWiseOptions.FRONTEND_GRPC_BACKEND_PORT.key(), args[++i]);
                    break;
                case "--config":
                    if (i + 1 < args.length) System.setProperty("rawwise.config.file", args[++i]);
                    break;
                case "--metastore":
                    if (i + 1 < args.length)
                        overrides.put(RawWiseOptions.METASTORE_TYPE.key(), args[++i]);
                    break;
                case "--embedded":
                    System.setProperty("rawwise.deploy.mode", "embedded");
                    break;
                default:
                    break;
            }
        }
        return overrides;
    }

    public static void main(String[] args) {
        WriterFactoryBootstrap.registerAll();
        log.info("Writer factories registered");

        RawWiseConfig config = ConfigLoader.builder().cliOverrides(parseCliArgs(args)).load();

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

        FrontendOptions options = new FrontendOptions(config.asMap());

        MetastoreRegistry registry = MetastoreRegistry.getInstance();
        registry.registerDefaults();
        String metastoreKind = config.get(RawWiseOptions.METASTORE_TYPE);
        Metastore store = registry.create(metastoreKind, config);
        log.info("Metastore created: kind={}", metastoreKind);

        if ("embedded".equals(System.getProperty("rawwise.deploy.mode", ""))) {
            io.github.rawwise.backend.shared.BackendSharedRegistry.setMetastore(store);
        }

        boolean embedded = "embedded".equals(System.getProperty("rawwise.deploy.mode", ""));
        EmbeddedBackend embeddedBackend = null;
        if (embedded) {
            log.info("Embedded mode: starting Backend in-process (InProcess gRPC)");
            embeddedBackend = new EmbeddedBackend(config);
            try {
                embeddedBackend.start();
            } catch (Exception e) {
                throw new RuntimeException("Failed to start EmbeddedBackend", e);
            }
        }

        BackendClientFactory backendClientFactory = new BackendClientFactory();
        BackendClient backendClient;
        if (embedded) {
            backendClient =
                    GrpcBackendClient.forInProcess(
                            EmbeddedBackend.BACKEND_INPROC_NAME, options.getQueryTimeoutSeconds());
        } else {
            backendClient = backendClientFactory.createBackendClient(options);
        }

        FrontendSessionStore sessionStore = new FrontendSessionStore();
        QueryEvaluator queryEvaluator = new DefaultQueryEvaluator();

        boolean irEnabled = config.get(RawWiseOptions.FRONTEND_IR_ENABLED);

        // In embedded mode, inject Metastore into Backend's Executor for axiom ingest/derivation
        if (embedded && embeddedBackend != null) {
            embeddedBackend.setMetastore(store);
        }

        RoutingRoutes routingRoutes = null;
        String backendGroupsCfg = options.getGrpcBackendGroups();
        if (!backendGroupsCfg.isBlank()) {
            Map<String, GrpcBackendClient> clientMap = new java.util.LinkedHashMap<>();
            for (String entry : backendGroupsCfg.split(",")) {
                String[] parts = entry.split(":");
                String groupId = parts[0].trim();
                String host = parts[1].trim();
                int port = Integer.parseInt(parts[2].trim());
                clientMap.put(
                        groupId,
                        new GrpcBackendClient(
                                host, port, options.getQueryTimeoutSeconds(), options));
                log.info("Registered backend group: {} ({})", groupId, host + ":" + port);
            }
            RoutingTableManager routingManager =
                    new RoutingTableManager(store, options.getRoutingRefreshSeconds());
            RoutingTableWriter routingWriter = new RoutingTableWriter(store);
            backendClient = new MultiBackendClient(clientMap, routingManager);
            routingRoutes = new RoutingRoutes(routingWriter, routingManager);
            log.info("MultiBackendClient initialized with {} groups", clientMap.size());
        }

        Catalog catalog;
        try {
            MetastoreCatalogConfig catalogConfig =
                    MetastoreCatalogConfig.builder()
                            .store(store)
                            .basePath(
                                    io.github.rawwise.governance.config.WarehousePathResolver
                                            .resolve())
                            .build();
            catalog = new MetastoreCatalog(catalogConfig);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Catalog", e);
        }

        SnapshotManager snapshotManager = null;
        if (irEnabled) {
            try {
                snapshotManager = new SnapshotManager(catalog, store);
            } catch (Exception e) {
                log.warn("Failed to initialize SnapshotManager: {}", e.getMessage());
            }
        }

        CompactionScheduler scheduler = createCompactionScheduler(catalog, store, snapshotManager);

        IrRuntime irRuntime = null;
        if (irEnabled) {
            try {
                irRuntime = IrRuntime.create(catalog, snapshotManager);
                log.info("IR runtime initialized successfully");

                wireFastPath(irRuntime, options, embedded ? embeddedBackend : null);
            } catch (Exception e) {
                log.warn("Failed to initialize IR runtime, IR path disabled: {}", e.getMessage());
            }
        }

        AuthMetastore authMetastore = null;
        AuthFilter authFilter = null;
        AuthRoutes authRoutes = null;
        if (options.isAuthEnabled()) {
            authMetastore = new DefaultAuthMetastore(store);
            PermissionCache permissionCache =
                    new PermissionCache(
                            options.getAuthCacheMaxSize(), options.getAuthCacheExpireSeconds());
            TokenCache tokenCache =
                    new TokenCache(
                            options.getAuthCacheMaxSize(),
                            options.getAuthTokenCacheExpireSeconds());

            ProvenanceStore provenanceStore = new DefaultProvenanceStore(store);
            ProvenanceAuditor auditLogger =
                    new ProvenanceAuditor(
                            options.isAuthAuditEnabled(),
                            provenanceStore,
                            options.isAuthAuditEnabled());

            authFilter =
                    new AuthFilter(
                            authMetastore,
                            permissionCache,
                            tokenCache,
                            irRuntime != null ? irRuntime.queryParser() : null,
                            auditLogger,
                            options.isAuthUnauthenticatedDeny());
            authRoutes = new AuthRoutes(authMetastore, permissionCache, tokenCache, auditLogger);

            bootstrapSuperAdminToken(authMetastore);
        }

        if (!options.isAuthEnabled()) {
            log.warn("╔══════════════════════════════════════════════════════╗");
            log.warn("║  AUTHENTICATION DISABLED - NOT FOR PRODUCTION USE   ║");
            log.warn("╚══════════════════════════════════════════════════════╝");
        }

        ContextRoutes contextRoutes = null;
        ExecutionRoutes executionRoutes = null;
        AxiomScheduler axiomScheduler = null;
        MemoryRoutes memoryRoutes = null;
        MemoryFacade memoryFacade = null;
        GraphEngine graphEngine = null;
        TrustRoutes trustRoutes = null;
        if (options.isContextEnabled()) {
            ContextManager contextManager =
                    new DefaultContextManager(store, options.getContextSessionHistoryVersions());
            contextRoutes = new ContextRoutes(contextManager);

            TrustEngine trustEngine = new MerkleVerifier(contextManager, store);
            trustRoutes = new TrustRoutes(trustEngine);
            log.info("TrustEngine initialized (MerkleVerifier, contextManager-backed)");

            if (options.isExecutionEnabled()) {
                ExecutionManager executionManager =
                        new DefaultExecutionManager(store, contextManager);
                executionRoutes = new ExecutionRoutes(executionManager);
            }

            try {
                EmbeddingProvider embeddingProvider =
                        EmbeddingProviderFactory.createFromSystemProperties();
                VectorIndex vectorIndex = new InMemoryVectorIndex(embeddingProvider);
                SummarizationService summarizationService =
                        SummarizationServiceFactory.createFromSystemProperties();
                // In embedded mode, register SummarizationService to BackendSharedRegistry so
                // AutoDerivationHook (fact extraction / L0 summarization) can access it.
                // In standalone mode, Backend Main.java does this separately.
                if (summarizationService != null && summarizationService.isAvailable()) {
                    io.github.rawwise.backend.shared.BackendSharedRegistry.setSummarizationService(
                            summarizationService);
                    log.info(
                            "SummarizationService registered to BackendSharedRegistry (embedded mode, available=true)");
                }
                graphEngine =
                        GraphEngineFactory.createFromSystemProperties(
                                null, null, contextManager, embeddingProvider, vectorIndex);
                // MemoryFacade delegates to the Axiom semantic-stack backend over HTTP
                // (/api/axiom/*). Legacy tiered engines (ConsolidationCoordinator/DecayEngine/
                // TieredReflectionEngine/TieredRecallEngine) are no longer constructed here.
                String axiomBaseUrl =
                        System.getProperty("rawwise.axiom.base-url", "http://localhost:8080");
                AxiomClient axiomClient = new HttpAxiomClient(axiomBaseUrl);
                ViewMetadataStore viewMetadataStore = new ViewMetadataStore(store);
                AxiomMemoryFacade wiredFacade =
                        new AxiomMemoryFacade(axiomClient, viewMetadataStore, DecayPolicy.DEFAULT);
                wiredFacade.setAgentIndexStore(new AgentIndexStore(store));
                memoryFacade = wiredFacade;
                memoryRoutes = new MemoryRoutes(memoryFacade);

                AxiomSchedulerConfig schedConfig = AxiomSchedulerConfig.fromSystemProperties();
                axiomScheduler = new AxiomScheduler(memoryFacade, schedConfig);
                if (schedConfig.enabled()) {
                    axiomScheduler.start();
                }
                log.info(
                        "AxiomScheduler wired (enabled={}, agents={})",
                        schedConfig.enabled(),
                        schedConfig.agents());

                runMemoryStartupSmokeCheck(memoryFacade, graphEngine);
            } catch (Exception e) {
                log.warn(
                        "Failed to wire AxiomScheduler, decay/consolidation disabled: {}",
                        e.getMessage());
            }

            log.info(
                    "AI Native context module initialized (execution={})",
                    options.isExecutionEnabled());
        }

        CountDownLatch shutdownLatch = new CountDownLatch(1);

        boolean embeddedMetastore = !"grpc".equals(metastoreKind);
        GrpcServer metastoreGrpcServer = null;
        if (embeddedMetastore) {
            int metastoreGrpcPort = config.get(RawWiseOptions.METASTORE_GRPC_PORT);
            metastoreGrpcServer = new GrpcServer(metastoreGrpcPort, catalog, store);
        }

        io.github.rawwise.axiom.pipeline.state.StateEngine stateEngine = null;
        try {
            stateEngine = new io.github.rawwise.axiom.pipeline.state.DefaultStateEngine(store);
        } catch (Exception e) {
            log.warn("Failed to create StateEngine: {}", e.getMessage(), e);
        }

        AxiomRouteGroup axiomRouteGroup =
                createAxiomRouteGroup(
                        options,
                        memoryRoutes,
                        memoryFacade,
                        embedded ? embeddedBackend : null,
                        stateEngine,
                        trustRoutes);

        RawWiseMcpServer mcpServer =
                createMcpServer(
                        options,
                        backendClient,
                        embedded ? embeddedBackend : null,
                        graphEngine,
                        memoryFacade,
                        stateEngine);

        int globalRateLimit = config.get(RawWiseOptions.FRONTEND_RATE_LIMIT_GLOBAL);
        int perIpRateLimit = config.get(RawWiseOptions.FRONTEND_RATE_LIMIT_PER_IP);
        RateLimitFilter rateLimitFilter = new RateLimitFilter(globalRateLimit, perIpRateLimit);

        TxnEngineConfig txnEngineConfig = new TxnEngineConfig();
        txnEngineConfig.logStartupInfo();

        SessionManager sessionManager = new SessionManager();
        log.info("SessionManager created for txn session management");

        try {
            if (metastoreGrpcServer != null) {
                metastoreGrpcServer.start();
                log.info(
                        "Embedded Metastore gRPC server started on port {}",
                        config.get(RawWiseOptions.METASTORE_GRPC_PORT));
            } else {
                log.info(
                        "Using remote Metastore via gRPC at {}:{}",
                        config.get(RawWiseOptions.METASTORE_GRPC_HOST),
                        config.get(RawWiseOptions.METASTORE_GRPC_PORT));
            }

            FrontendServer server =
                    new FrontendServer(
                            options,
                            sessionStore,
                            backendClient,
                            queryEvaluator,
                            irRuntime,
                            authFilter,
                            authRoutes,
                            contextRoutes,
                            executionRoutes,
                            axiomRouteGroup,
                            authMetastore,
                            routingRoutes,
                            rateLimitFilter,
                            createBackupManager(store, config),
                            sessionManager,
                            mcpServer);

            GrpcServer finalMetastoreGrpcServer = metastoreGrpcServer;
            AxiomScheduler finalAxiomScheduler = axiomScheduler;
            EmbeddedBackend finalEmbeddedBackend = embeddedBackend;
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    () -> {
                                        log.info("Shutting down frontend server");
                                        server.stop();
                                        if (scheduler != null) {
                                            scheduler.close();
                                        }
                                        if (finalAxiomScheduler != null) {
                                            try {
                                                finalAxiomScheduler.close();
                                            } catch (Exception e) {
                                                log.warn("AxiomScheduler close failed", e);
                                            }
                                        }
                                        if (finalMetastoreGrpcServer != null) {
                                            finalMetastoreGrpcServer.shutdown();
                                        }
                                        if (finalEmbeddedBackend != null) {
                                            finalEmbeddedBackend.close();
                                        }
                                        try {
                                            store.close();
                                        } catch (Exception e) {
                                            log.warn("Failed to close Metastore", e);
                                        }
                                        shutdownLatch.countDown();
                                    }));

            server.start();

            if (scheduler != null) {
                scheduler.start();
            }

            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Frontend server failed", e);
        } finally {
            if (embeddedBackend != null) {
                embeddedBackend.close();
            }
            if (metastoreGrpcServer != null) {
                metastoreGrpcServer.shutdown();
            }
            if (scheduler != null) {
                scheduler.close();
            }
            if (axiomScheduler != null) {
                try {
                    axiomScheduler.close();
                } catch (Exception e) {
                    log.warn("AxiomScheduler close failed", e);
                }
            }
            if (backendClient instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) backendClient).close();
                } catch (Exception e) {
                    log.warn("Failed to close backend client", e);
                }
            }
            try {
                sessionStore.close();
            } catch (Exception e) {
                log.warn("Failed to close session store", e);
            }
        }
    }

    private static void runMemoryStartupSmokeCheck(
            MemoryFacade memoryFacade, GraphEngine graphEngine) {
        try {
            var status = memoryFacade.status("default");
            if (status == null) {
                log.error(
                        "Memory startup smoke check: status('default') returned null; benchmark recipe D will silently skip multi-hop");
            } else {
                log.info("Memory startup smoke check: status={}", status);
            }
        } catch (Exception e) {
            log.error(
                    "Memory startup smoke check: status('default') threw {}; benchmark recipe D may misbehave",
                    e.getMessage());
        }
        if (graphEngine == null) {
            log.error(
                    "Memory startup smoke check: graphEngine is null; multi-hop graph operations are disabled");
            return;
        }
        try {
            java.util.List<GraphSchema> graphs = graphEngine.listGraphsByAgent("default");
            log.info(
                    "Memory startup smoke check: graphEngine.listGraphsByAgent('default') returned {} graphs",
                    graphs.size());
        } catch (Exception e) {
            log.error(
                    "Memory startup smoke check: graphEngine.listGraphsByAgent('default') threw {}; multi-hop graph operations may fail",
                    e.getMessage(),
                    e);
        }
    }

    private static void bootstrapSuperAdminToken(AuthMetastore authMetastore) {
        try {
            var roles = authMetastore.listRoles();
            if (!roles.isEmpty()) {
                return;
            }
        } catch (Exception e) {
            // ignore, proceed with bootstrap
        }

        String superToken = AuthCrypto.generateSecureToken();
        try {
            Principal superAdmin = new Principal.UserPrincipal("superadmin", "Super Admin");
            authMetastore.createToken(superToken, superAdmin);
            if (authMetastore instanceof DefaultAuthMetastore dam) {
                dam.setSuperAdmin(superAdmin);
            }
            persistSuperAdminToken(superToken);
        } catch (Exception e) {
            log.error("Failed to bootstrap super admin token", e);
        }
    }

    private static void persistSuperAdminToken(String superToken) {
        // Prefer system property rawwise.token.persist-dir, fallback to /run/rawwise, then fallback
        // to /tmp/rawwise
        String dirOverride = System.getProperty("rawwise.token.persist-dir", "/run/rawwise");
        Path tokenDir = Path.of(dirOverride);
        try {
            if (!java.nio.file.Files.exists(tokenDir)) {
                java.nio.file.Files.createDirectories(tokenDir);
                setPosixPermissionsSafely(
                        tokenDir,
                        Set.of(
                                java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                                java.nio.file.attribute.PosixFilePermission.OWNER_WRITE,
                                java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE));
            }
            java.nio.file.Path tokenFile = tokenDir.resolve("bootstrap-token");
            if (java.nio.file.Files.exists(tokenFile)) {
                java.nio.file.Files.delete(tokenFile);
            }
            java.nio.file.Files.writeString(
                    tokenFile,
                    superToken,
                    java.nio.file.StandardOpenOption.CREATE_NEW,
                    java.nio.file.StandardOpenOption.WRITE);
            setPosixPermissionsSafely(
                    tokenFile,
                    Set.of(
                            java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                            java.nio.file.attribute.PosixFilePermission.OWNER_WRITE));
            log.info("Super admin token written to {}", tokenFile);
        } catch (Exception e) {
            log.error(
                    "FATAL: Failed to persist super admin token to {}. Cannot start without secure token persistence.",
                    tokenDir,
                    e);
            throw new RuntimeException("Super admin token persistence failed", e);
        }
    }

    private static void setPosixPermissionsSafely(
            java.nio.file.Path path,
            java.util.Set<java.nio.file.attribute.PosixFilePermission> perms) {
        try {
            java.nio.file.Files.setPosixFilePermissions(path, perms);
        } catch (java.lang.UnsupportedOperationException e) {
            log.warn(
                    "POSIX file permissions not supported on this platform. Token file permissions may be less restrictive.");
        } catch (Exception e) {
            log.warn("Failed to set POSIX file permissions: {}", e.getMessage());
        }
    }

    private static AxiomRouteGroup createAxiomRouteGroup(
            FrontendOptions options,
            MemoryRoutes memoryRoutes,
            MemoryFacade memoryFacade,
            @org.jetbrains.annotations.Nullable EmbeddedBackend embeddedBackend,
            @org.jetbrains.annotations.Nullable
                    io.github.rawwise.axiom.pipeline.state.StateEngine stateEngine,
            @org.jetbrains.annotations.Nullable TrustRoutes trustRoutes) {
        if (!options.isAxiomEnabled()) {
            return null;
        }
        try {
            io.grpc.ManagedChannel channel;
            if (embeddedBackend != null) {
                channel =
                        io.grpc.inprocess.InProcessChannelBuilder.forName(
                                        EmbeddedBackend.BACKEND_INPROC_NAME)
                                .directExecutor()
                                .build();
            } else {
                channel =
                        io.grpc.ManagedChannelBuilder.forAddress(
                                        options.getGrpcBackendHost(), options.getGrpcBackendPort())
                                .usePlaintext()
                                .keepAliveTime(30, TimeUnit.SECONDS)
                                .keepAliveTimeout(10, TimeUnit.SECONDS)
                                .keepAliveWithoutCalls(true)
                                .maxInboundMessageSize(64 * 1024 * 1024)
                                .build();
            }
            String internalToken =
                    io.github.rawwise.governance.auth.InternalTokenResolver.resolve();
            io.grpc.Channel authChannel =
                    io.grpc.ClientInterceptors.intercept(
                            channel,
                            new io.grpc.ClientInterceptor() {
                                private final io.grpc.Metadata.Key<String> TOKEN_KEY =
                                        io.grpc.Metadata.Key.of(
                                                "authorization",
                                                io.grpc.Metadata.ASCII_STRING_MARSHALLER);

                                @Override
                                public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                                        io.grpc.MethodDescriptor<ReqT, RespT> method,
                                        io.grpc.CallOptions callOptions,
                                        io.grpc.Channel next) {
                                    io.grpc.ClientCall<ReqT, RespT> call =
                                            next.newCall(method, callOptions);
                                    return new io.grpc.ForwardingClientCall
                                            .SimpleForwardingClientCall<>(call) {
                                        @Override
                                        public void start(
                                                Listener<RespT> responseListener,
                                                io.grpc.Metadata headers) {
                                            headers.put(TOKEN_KEY, internalToken);
                                            super.start(responseListener, headers);
                                        }
                                    };
                                }
                            });
            io.github.rawwise.proto.axiom.AxiomServiceGrpc.AxiomServiceBlockingStub stub =
                    io.github.rawwise.proto.axiom.AxiomServiceGrpc.newBlockingStub(authChannel);
            AxiomKnowledgeRoutes knowledgeRoutes = new AxiomKnowledgeRoutes(stub, memoryFacade);
            StateRoutes stateRoutes = stateEngine != null ? new StateRoutes(stateEngine) : null;
            return new AxiomRouteGroup(
                    memoryRoutes, stateRoutes, trustRoutes, null, null, null, knowledgeRoutes);
        } catch (Exception e) {
            log.warn("Failed to create AxiomRouteGroup: {}", e.getMessage());
            return null;
        }
    }

    @org.jetbrains.annotations.Nullable
    private static RawWiseMcpServer createMcpServer(
            FrontendOptions options,
            BackendClient backendClient,
            @org.jetbrains.annotations.Nullable EmbeddedBackend embeddedBackend,
            @org.jetbrains.annotations.Nullable GraphEngine graphEngine,
            @org.jetbrains.annotations.Nullable MemoryFacade memoryFacade,
            @org.jetbrains.annotations.Nullable
                    io.github.rawwise.axiom.pipeline.state.StateEngine stateEngine) {
        if (!options.isAxiomEnabled()) {
            log.info("MCP server disabled: axiom is not enabled");
            return null;
        }
        RawWiseMcpConfig mcpConfig = RawWiseMcpConfig.fromSystemProperties();
        if (!mcpConfig.enabled()) {
            log.info("MCP server disabled: rawwise.mcp.enabled=false");
            return null;
        }
        try {
            io.grpc.ManagedChannel channel;
            if (embeddedBackend != null) {
                channel =
                        io.grpc.inprocess.InProcessChannelBuilder.forName(
                                        EmbeddedBackend.BACKEND_INPROC_NAME)
                                .directExecutor()
                                .build();
            } else {
                channel =
                        io.grpc.ManagedChannelBuilder.forAddress(
                                        options.getGrpcBackendHost(), options.getGrpcBackendPort())
                                .usePlaintext()
                                .keepAliveTime(30, TimeUnit.SECONDS)
                                .keepAliveTimeout(10, TimeUnit.SECONDS)
                                .keepAliveWithoutCalls(true)
                                .maxInboundMessageSize(64 * 1024 * 1024)
                                .build();
            }
            String internalToken =
                    io.github.rawwise.governance.auth.InternalTokenResolver.resolve();
            io.grpc.Channel authChannel =
                    io.grpc.ClientInterceptors.intercept(
                            channel,
                            new io.grpc.ClientInterceptor() {
                                private final io.grpc.Metadata.Key<String> TOKEN_KEY =
                                        io.grpc.Metadata.Key.of(
                                                "authorization",
                                                io.grpc.Metadata.ASCII_STRING_MARSHALLER);

                                @Override
                                public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                                        io.grpc.MethodDescriptor<ReqT, RespT> method,
                                        io.grpc.CallOptions callOptions,
                                        io.grpc.Channel next) {
                                    io.grpc.ClientCall<ReqT, RespT> call =
                                            next.newCall(method, callOptions);
                                    return new io.grpc.ForwardingClientCall
                                            .SimpleForwardingClientCall<>(call) {
                                        @Override
                                        public void start(
                                                Listener<RespT> responseListener,
                                                io.grpc.Metadata headers) {
                                            headers.put(TOKEN_KEY, internalToken);
                                            super.start(responseListener, headers);
                                        }
                                    };
                                }
                            });
            io.github.rawwise.proto.axiom.AxiomServiceGrpc.AxiomServiceBlockingStub stub =
                    io.github.rawwise.proto.axiom.AxiomServiceGrpc.newBlockingStub(authChannel);
            RawWiseMcpTools mcpTools =
                    new RawWiseMcpTools(
                            stub, backendClient, mcpConfig, memoryFacade, graphEngine, stateEngine);

            RawWiseMcpResources mcpResources = null;
            if (mcpConfig.resourcesEnabled()) {
                mcpResources = new RawWiseMcpResources(backendClient, stub, graphEngine);
                log.info(
                        "MCP Resources enabled (graphEngine={})",
                        graphEngine != null ? "available" : "null");
            }

            RawWiseMcpPrompts mcpPrompts = null;
            if (mcpConfig.promptsEnabled()) {
                mcpPrompts = new RawWiseMcpPrompts();
                log.info("MCP Prompts enabled");
            }

            return new RawWiseMcpServer(mcpTools, mcpConfig, mcpResources, mcpPrompts);
        } catch (Exception e) {
            log.warn("Failed to create MCP server: {}", e.getMessage());
            return null;
        }
    }

    private static CompactionScheduler createCompactionScheduler(
            Catalog catalog, Metastore store, SnapshotManager snapshotManager) {
        try {
            if (snapshotManager == null) {
                snapshotManager = new SnapshotManager(catalog, store);
            }
            Storage storage = new LocalStorage("compaction");
            FileCleanupManager cleanupManager = new FileCleanupManager(storage);
            CompactionRecoveryManager recoveryManager =
                    new CompactionRecoveryManager(cleanupManager, storage);
            FileGarbageCollector garbageCollector =
                    new FileGarbageCollector(storage, snapshotManager, store);
            CleanMetrics cleanMetrics = new CleanMetrics();
            garbageCollector.setMetrics(cleanMetrics);
            CompactionManager compactionManager =
                    new CompactionManager(
                            snapshotManager,
                            cleanupManager,
                            recoveryManager,
                            garbageCollector,
                            storage,
                            3);
            long intervalMs = RawWiseOptions.COMPACTION_TRIGGER_INTERVAL_MS.defaultValue();
            return CompactionScheduler.createDefault(
                    compactionManager, snapshotManager.getCatalog(), intervalMs);
        } catch (Exception e) {
            log.warn("Failed to create CompactionScheduler, compaction disabled", e);
            return null;
        }
    }

    @org.jetbrains.annotations.Nullable
    private static io.github.rawwise.metastore.backup.MetastoreBackupManager createBackupManager(
            Metastore store, io.github.rawwise.governance.config.RawWiseConfig config) {
        if (!(store instanceof io.github.rawwise.metastore.RocksdbMetastore rocksdb)) {
            return null;
        }
        boolean enabled =
                config.get(
                        io.github.rawwise.governance.config.RawWiseOptions
                                .METASTORE_BACKUP_ENABLED);
        if (!enabled) {
            return null;
        }
        String backupPath =
                config.get(
                        io.github.rawwise.governance.config.RawWiseOptions.METASTORE_BACKUP_PATH);
        int intervalHours =
                config.get(
                        io.github.rawwise.governance.config.RawWiseOptions
                                .METASTORE_BACKUP_INTERVAL_HOURS);
        var manager =
                new io.github.rawwise.metastore.backup.MetastoreBackupManager(
                        rocksdb, backupPath, intervalHours);
        manager.start();
        return manager;
    }

    private static void wireFastPath(
            IrRuntime irRuntime,
            FrontendOptions options,
            @org.jetbrains.annotations.Nullable EmbeddedBackend embeddedBackend) {
        try {
            if (embeddedBackend != null) {
                ManagedChannel channel =
                        io.grpc.inprocess.InProcessChannelBuilder.forName(
                                        EmbeddedBackend.EXECUTOR_INPROC_NAME)
                                .directExecutor()
                                .build();
                GrpcExecutorStub stub =
                        new GrpcExecutorStub(channel, (int) options.getQueryTimeoutSeconds());
                irRuntime.fastPathExecutor().registerExecutor("driver", stub);
                irRuntime.fastPathExecutor().registerShardExecutor(0, "driver");
                log.info(
                        "FastPath wired to InProcess executor (name={})",
                        EmbeddedBackend.EXECUTOR_INPROC_NAME);
                return;
            }
            // Distributed mode: read BE Node list from -Drawwise.frontend.fastpath.nodes
            // Format: host1:port1,host2:port2,...
            String nodesConfig = System.getProperty("rawwise.frontend.fastpath.nodes", "");
            if (nodesConfig.isBlank()) {
                log.warn(
                        "FastPath disabled in distributed mode: rawwise.frontend.fastpath.nodes not set. "
                                + "Set -Drawwise.frontend.fastpath.nodes=host1:port1,host2:port2,... to enable.");
                return;
            }
            String[] nodeEntries = nodesConfig.split(",");
            for (int i = 0; i < nodeEntries.length; i++) {
                String[] parts = nodeEntries[i].trim().split(":");
                if (parts.length != 2) continue;
                String host = parts[0].trim();
                int port = Integer.parseInt(parts[1].trim());
                String executorId = "executor-" + i;
                ManagedChannel channel =
                        NettyChannelBuilder.forAddress(host, port)
                                .usePlaintext()
                                .maxInboundMessageSize(64 * 1024 * 1024)
                                .flowControlWindow(8 * 1024 * 1024)
                                .keepAliveTime(30, TimeUnit.SECONDS)
                                .keepAliveTimeout(10, TimeUnit.SECONDS)
                                .keepAliveWithoutCalls(true)
                                .build();
                GrpcExecutorStub stub =
                        new GrpcExecutorStub(channel, (int) options.getQueryTimeoutSeconds());
                irRuntime.fastPathExecutor().registerExecutor(executorId, stub);
                irRuntime.fastPathExecutor().registerShardExecutor(i, executorId);
                log.info(
                        "FastPath wired to BE Node {}:{} (executorId={}, shardId={})",
                        host,
                        port,
                        executorId,
                        i);
            }
        } catch (Exception e) {
            log.warn("Failed to wire FastPath executor: {}", e.getMessage());
        }
    }
}
