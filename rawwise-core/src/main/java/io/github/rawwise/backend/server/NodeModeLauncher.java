package io.github.rawwise.backend.server;

import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.executor.service.TxnCoordinatorGrpcService;
import io.github.rawwise.backend.server.health.HeartbeatService;
import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.writer.WriterFactoryBootstrap;
import io.github.rawwise.engine.txn.OccConfig;
import io.github.rawwise.engine.txn.clock.LogicalClock;
import io.github.rawwise.engine.txn.clock.MetastoreClockImpl;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorFactory;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorImpl;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.GrpcMetastoreClient;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.server.ClusterManagementServiceGrpc;
import io.github.rawwise.server.RegisterExecutorRequest;
import io.github.rawwise.server.RegisterExecutorResponse;
import io.github.rawwise.server.Status;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * BE Node mode launcher.
 *
 * <p>When Main detects {@code --mode node}, skips SparkSession creation, uses this class to handle
 * startup:
 *
 * <ol>
 *   <li>initialize DynamicMmapStore (LMDB hot storage) + Metastore gRPC connection
 *   <li>initialize Axiom engine (DerivationEngine / CognitionEngine / FeedbackLoopCloser)
 *   <li>start gRPC services (ExecutorPointQueryService + TxnCoordinatorGrpcService)
 *   <li>send register request to BE Server via {@code RegisterExecutor}
 *   <li>wait for JVM shutdown signal
 * </ol>
 *
 * <p>Deployment boundary: this class holds Backend Node components (DynamicMmapStore / LmdbInstance
 * / VectorIndex), does not hold Backend Server components (JobScheduler / SparkJobScheduler /
 * ServerTxnCoordinator).
 */
@Slf4j
public final class NodeModeLauncher {

    private final RawWiseConfig config;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private DynamicMmapStore store;
    private Server grpcServer;
    private ManagedChannel serverChannel;
    private HeartbeatService heartbeatService;
    private io.grpc.protobuf.services.HealthStatusManager grpcHealthService;
    private String executorId;
    private int grpcPort;
    private int shardId;
    private String nodeId;

    public NodeModeLauncher(RawWiseConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        StorageRegistry.registerDefaults();
        WriterFactoryBootstrap.registerAll();
    }

    public void start() throws Exception {
        Map<String, Object> cfg = config.asMap();

        // 1. parse Node modeconfiguration
        this.shardId = parseIntOrDefault(cfg, Main.BACKEND_NODE_SHARD_ID_KEY, 0);
        String serverHost =
                (String) cfg.getOrDefault(Main.BACKEND_NODE_SERVER_HOST_KEY, "localhost");
        int serverPort =
                parseIntOrDefault(
                        cfg,
                        Main.BACKEND_NODE_SERVER_PORT_KEY,
                        config.get(RawWiseOptions.FRONTEND_GRPC_BACKEND_PORT));
        this.grpcPort =
                parseIntOrDefault(
                        cfg,
                        "grpc.backend.port",
                        config.get(RawWiseOptions.FRONTEND_GRPC_BACKEND_PORT));
        this.nodeId =
                (String)
                        cfg.getOrDefault(
                                Main.BACKEND_NODE_ID_KEY, InetAddress.getLocalHost().getHostName());
        int slotIndex = 0;
        this.executorId = "node-" + nodeId + "-shard-" + shardId;

        log.info(
                "Starting BE Node: executorId={} nodeId={} shardId={} grpcPort={} server={}:{}",
                executorId,
                nodeId,
                shardId,
                grpcPort,
                serverHost,
                serverPort);

        // 2. initialize DynamicMmapStore + Axiom engine
        initStoreAndAxiom();

        // 3. startup gRPC service
        startGrpcServer();

        // 4. Register with BE Server
        registerToServer(serverHost, serverPort, slotIndex);

        // 5. Register JVM shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "node-shutdown"));

        log.info(
                "BE Node started: executorId={} grpcPort={} shardId={}",
                executorId,
                grpcPort,
                shardId);
        shutdownLatch.await();
    }

    private void initStoreAndAxiom() {
        String warehousePath = config.get(RawWiseOptions.CATALOG_WAREHOUSE_PATH);
        if (warehousePath == null || warehousePath.isBlank()) {
            throw new IllegalStateException(
                    "rawwise.catalog.warehouse-path must be set for BE Node mode");
        }

        store = DynamicMmapStore.getInstance(warehousePath, "node-" + nodeId, config);
        store.start();

        ensureMetastore(store, config);
        waitForMetastoreReady(store, 15, 1000L);

        store.initAxiomEngines(config);
        log.info("Axiom engines initialized on BE Node {} (executorId={})", nodeId, executorId);
    }

    private void ensureMetastore(DynamicMmapStore store, RawWiseConfig rawwiseConfig) {
        Metastore metastore = store.getMetastore();
        if (metastore != null) {
            return;
        }
        Map<String, Object> cfg = rawwiseConfig.asMap();
        String msHost = (String) cfg.getOrDefault("rawwise.metastore.grpc-host", "localhost");
        int msPort = parseIntOrDefault(cfg, "rawwise.metastore.grpc-port", 9001);
        metastore = new GrpcMetastoreClient(msHost, msPort);
        store.setMetastore(metastore);
        log.info("Created GrpcMetastoreClient for BE Node: {}:{}", msHost, msPort);
    }

    private void waitForMetastoreReady(DynamicMmapStore store, int maxRetries, long backoffMs) {
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

    private void startGrpcServer() throws Exception {
        ExecutorPointQueryServiceImpl executorService = new ExecutorPointQueryServiceImpl(config);

        TxnCoordinatorGrpcService txnService = null;
        try {
            TxnOrchestrator orchestrator = createTxnOrchestrator(store, config);
            txnService = new TxnCoordinatorGrpcService(orchestrator);
            log.info("TxnCoordinatorGrpcService wired on BE Node {} (PLAN_A mode)", executorId);
        } catch (Exception e) {
            log.warn(
                    "Failed to initialize TxnCoordinatorGrpcService on BE Node {}: {}",
                    executorId,
                    e.getMessage());
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
        if (txnService != null) {
            serverBuilder.addService(txnService);
        }
        this.grpcHealthService = new io.grpc.protobuf.services.HealthStatusManager();
        grpcHealthService.setStatus(
                "", io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING);
        serverBuilder.addService(grpcHealthService.getHealthService());
        grpcServer = serverBuilder.build().start();
        log.info("BE Node gRPC server started on port {} (with HealthCheckService)", grpcPort);
    }

    private TxnOrchestrator createTxnOrchestrator(
            DynamicMmapStore store, RawWiseConfig rawwiseConfig) {
        Metastore metastore = store.getMetastore();
        if (metastore == null) {
            throw new IllegalStateException("Metastore must be initialized before TxnOrchestrator");
        }

        WAL wal = store.getWal();
        String warehousePath = rawwiseConfig.get(RawWiseOptions.CATALOG_WAREHOUSE_PATH);
        LmdbInstance lmdbInstance = store.currentLmdbInstance("_default", new byte[] {0});
        if (lmdbInstance == null && warehousePath != null && !warehousePath.isBlank()) {
            lmdbInstance =
                    new LmdbInstance(
                            "txn-node-" + nodeId,
                            java.nio.file.Path.of(warehousePath, "txn-node-" + nodeId),
                            128L * 1024L * 1024L);
            store.tableStore("_default").addWritableInstance(lmdbInstance);
            log.info(
                    "Created and registered LmdbInstance {} for BE Node {}",
                    lmdbInstance.instanceId(),
                    executorId);
        }
        if (lmdbInstance == null) {
            throw new IllegalStateException(
                    "No LmdbInstance available for TxnOrchestrator on BE Node " + executorId);
        }

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
                "TxnOrchestrator created on BE Node {} (PLAN_A, perKeyOCC={})",
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
                    "TxnStoreBridge wired from orchestrator to DynamicMmapStore on BE Node {}",
                    executorId);
        }

        return orchestrator;
    }

    private void registerToServer(String serverHost, int serverPort, int slotIndex) {
        serverChannel =
                NettyChannelBuilder.forAddress(serverHost, serverPort)
                        .usePlaintext()
                        .maxInboundMessageSize(64 * 1024 * 1024)
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .keepAliveWithoutCalls(true)
                        .build();

        ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub stub =
                ClusterManagementServiceGrpc.newBlockingStub(serverChannel);

        RegisterExecutorRequest request =
                RegisterExecutorRequest.newBuilder()
                        .setExecutorId(executorId)
                        .setHost(resolveSelfHost())
                        .setPort(grpcPort)
                        .setShardId(shardId)
                        .setNodeId(nodeId)
                        .setSlotIndex(slotIndex)
                        .build();

        int maxRetries = 5;
        long backoffMs = 1000L;
        Exception lastError = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                RegisterExecutorResponse response = stub.registerExecutor(request);
                if (response.getStatus() != null
                        && response.getStatus().getCode() == Status.Code.OK) {
                    log.info(
                            "BE Node registered to BE Server ({}:{}) — epoch={}, attempt={}/{}",
                            serverHost,
                            serverPort,
                            response.getEpoch(),
                            attempt,
                            maxRetries);

                    heartbeatService = new HeartbeatService(stub, executorId, nodeId, shardId);
                    heartbeatService.start();
                    log.info("HeartbeatService started: executor={} interval=5s", executorId);
                    return;
                }
                String msg =
                        response.getStatus() == null
                                ? "null status"
                                : response.getStatus().getMessage();
                throw new RuntimeException("RegisterExecutor rejected by BE Server: " + msg);
            } catch (Exception e) {
                lastError = e;
                log.warn(
                        "RegisterExecutor attempt {}/{} failed: {} — retrying in {}ms",
                        attempt,
                        maxRetries,
                        e.getMessage(),
                        backoffMs * attempt);
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(backoffMs * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during BE Server registration", ie);
                    }
                }
            }
        }
        throw new RuntimeException(
                "Failed to register BE Node to BE Server after "
                        + maxRetries
                        + " attempts: "
                        + lastError.getMessage(),
                lastError);
    }

    private static String resolveSelfHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "localhost";
        }
    }

    private void shutdown() {
        log.info("Shutting down BE Node {}", executorId);
        try {
            if (grpcHealthService != null) {
                grpcHealthService.setStatus(
                        "", io.grpc.health.v1.HealthCheckResponse.ServingStatus.NOT_SERVING);
            }
        } catch (Exception e) {
            log.debug("Failed to set gRPC health status to NOT_SERVING", e);
        }
        try {
            if (heartbeatService != null) {
                heartbeatService.stop();
            }
        } catch (Exception e) {
            log.warn("Failed to stop HeartbeatService cleanly: {}", e.getMessage());
        }
        try {
            if (grpcServer != null) {
                grpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.warn("Failed to shutdown gRPC server cleanly: {}", e.getMessage());
        }
        try {
            if (serverChannel != null) {
                serverChannel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.warn("Failed to shutdown server channel cleanly: {}", e.getMessage());
        }
        try {
            io.github.rawwise.engine.storage.DynamicMmapStoreFactory.closeAll();
        } catch (Exception e) {
            log.warn("Failed to close DynamicMmapStore instances cleanly: {}", e.getMessage());
        }
        shutdownLatch.countDown();
    }

    private static int parseIntOrDefault(Map<String, Object> cfg, String key, int defaultValue) {
        Object v = cfg.get(key);
        if (v instanceof Number n) return n.intValue();
        if (v instanceof String s) {
            try {
                return Integer.parseInt(s.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }
}
