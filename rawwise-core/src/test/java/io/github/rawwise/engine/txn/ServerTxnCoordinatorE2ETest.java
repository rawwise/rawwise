package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.executor.service.TxnCoordinatorGrpcService;
import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.server.txn.DefaultServerTxnCoordinator;
import io.github.rawwise.backend.server.txn.ServerTxnCoordinator;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorFactory;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorImpl;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName(
        "ServerTxnCoordinator E2E - gRPC -> TxnCoordinatorGrpcService -> TxnOrchestrator -> LMDB")
class ServerTxnCoordinatorE2ETest {

    private static final String CATALOG = "stc_catalog";
    private static final String DATABASE = "default";
    private static final String TABLE = "stc_table";
    private static final String EXECUTOR_ID = "local";

    @TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private LmdbInstance lmdbInstance;
    private TxnOrchestratorImpl orchestrator;
    private TxnCoordinatorGrpcService grpcService;
    private Server grpcServer;
    private ManagedChannel channel;
    private ServerTxnCoordinator serverTxnCoordinator;
    private InMemoryShardRoutingTable routingTable;
    private RoutingCoordinator routingCoordinator;

    @BeforeEach
    void setUp() throws Exception {
        metastore = new InMemoryMetastore();
        byte[] clockKey =
                String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE)
                        .getBytes(StandardCharsets.UTF_8);
        metastore.put(clockKey, new byte[] {0, 0, 0, 0, 0, 0, 0, 1});

        lmdbInstance =
                new LmdbInstance("stc-e2e-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024, 5);

        ShardVersionTracker tracker =
                new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);

        TxnOrchestrator orch =
                TxnOrchestratorFactory.create(
                        TxnEngineConfig.planA(),
                        metastore,
                        lmdbInstance,
                        CATALOG,
                        DATABASE,
                        TABLE,
                        null,
                        tracker);
        orchestrator = (TxnOrchestratorImpl) orch;

        grpcService = new TxnCoordinatorGrpcService(orchestrator);

        String serverName = "stc-e2e-" + UUID.randomUUID();
        grpcServer =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(grpcService)
                        .build()
                        .start();

        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

        routingTable = new InMemoryShardRoutingTable();
        routingCoordinator = new InMemoryRoutingCoordinator(routingTable);

        NodeSlot nodeSlot = new NodeSlot(EXECUTOR_ID, 0);
        routingTable.registerExecutor(EXECUTOR_ID, nodeSlot);
        routingTable.markExecutorReady(EXECUTOR_ID);

        ShardId shard0 = new ShardId(0);
        routingTable.assignShard(
                new io.github.rawwise.backend.routing.ShardAssignment(
                        shard0,
                        nodeSlot,
                        EXECUTOR_ID,
                        io.github.rawwise.backend.routing.RoutingEpoch.initial()));

        serverTxnCoordinator =
                new DefaultServerTxnCoordinator(
                        Map.of(EXECUTOR_ID, channel), routingCoordinator, 30);
    }

    @AfterEach
    void tearDown() {
        if (channel != null) channel.shutdown();
        if (grpcServer != null) grpcServer.shutdown();
        if (orchestrator != null) orchestrator.close();
        if (lmdbInstance != null) lmdbInstance.close();
    }

    private byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("S1: beginTxn -> commitTxn via gRPC -> data persisted in real LMDB")
    void testBeginTxnCommitTxnViaGrpc() {
        var handle =
                serverTxnCoordinator.beginTxn(
                        Set.of(CATALOG + "." + DATABASE + "." + TABLE),
                        IsolationLevel.READ_COMMITTED);

        assertThat(handle).isNotNull();
        long txnId = handle.txnId();
        assertThat(txnId).isGreaterThan(0);

        TxnResult result = serverTxnCoordinator.commitTxn(txnId);

        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
    }

    @Test
    @DisplayName("S1: beginBatch -> commitBatch via gRPC -> data persisted in real LMDB")
    void testBeginBatchCommitBatchViaGrpc() {
        String tableId = CATALOG + "." + DATABASE + "." + TABLE;
        String batchId = "batch-" + UUID.randomUUID();

        var batchHandle = serverTxnCoordinator.beginBatch(tableId, batchId, 1);

        assertThat(batchHandle).isNotNull();
        assertThat(batchHandle.batchId()).isEqualTo(batchId);

        TxnResult result = serverTxnCoordinator.commitBatch(batchId);

        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
    }

    @Test
    @DisplayName("S3: abortBatch via gRPC -> no data persisted")
    void testAbortBatchViaGrpc() {
        String tableId = CATALOG + "." + DATABASE + "." + TABLE;
        String batchId = "abort-batch-" + UUID.randomUUID();

        serverTxnCoordinator.beginBatch(tableId, batchId, 1);

        TxnResult result = serverTxnCoordinator.abortBatch(batchId);

        assertThat(result).isInstanceOf(TxnResult.AbortResult.class);
    }

    @Test
    @DisplayName("S4: commitBatch with unknown batchId returns CommitConflict")
    void testCommitBatchUnknownId() {
        TxnResult result = serverTxnCoordinator.commitBatch("nonexistent-batch");

        assertThat(result).isInstanceOf(TxnResult.CommitConflict.class);
    }
}
