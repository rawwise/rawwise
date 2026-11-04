package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.executor.service.TxnCoordinatorGrpcService;
import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingEpoch;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.server.txn.DefaultServerTxnCoordinator;
import io.github.rawwise.backend.server.txn.RecoveryDecision;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName(
        "Cross-Shard 2PC E2E - 2 Executors (simulated Nodes) -> gRPC -> TxnCoordinatorGrpcService -> TxnOrchestrator -> LMDB")
class CrossShardTwoPhaseCommitE2ETest {

    private static final String CATALOG = "csm_catalog";
    private static final String DATABASE = "default";
    private static final String TABLE = "csm_table";
    private static final String TABLE_ID = CATALOG + "." + DATABASE + "." + TABLE;
    private static final String EXEC_A = "exec-A";
    private static final String EXEC_B = "exec-B";

    @TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private LmdbInstance lmdbA;
    private LmdbInstance lmdbB;
    private TxnOrchestratorImpl orchestratorA;
    private TxnOrchestratorImpl orchestratorB;
    private Server grpcServerA;
    private Server grpcServerB;
    private ManagedChannel channelA;
    private ManagedChannel channelB;
    private InMemoryShardRoutingTable routingTable;
    private RoutingCoordinator routingCoordinator;

    @BeforeEach
    void setUp() throws Exception {
        metastore = new InMemoryMetastore();
        byte[] clockKey =
                String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE)
                        .getBytes(StandardCharsets.UTF_8);
        metastore.put(clockKey, new byte[] {0, 0, 0, 0, 0, 0, 0, 1});

        Path dirA = Files.createDirectories(tempDir.resolve("node-a"));
        Path dirB = Files.createDirectories(tempDir.resolve("node-b"));
        lmdbA = new LmdbInstance("csm-A-" + UUID.randomUUID(), dirA, 128L * 1024 * 1024, 5);
        lmdbB = new LmdbInstance("csm-B-" + UUID.randomUUID(), dirB, 128L * 1024 * 1024, 5);

        ShardVersionTracker trackerA =
                new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);
        ShardVersionTracker trackerB =
                new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);

        TxnOrchestrator orchA =
                TxnOrchestratorFactory.create(
                        TxnEngineConfig.planA(),
                        metastore,
                        lmdbA,
                        CATALOG,
                        DATABASE,
                        TABLE,
                        null,
                        trackerA);
        TxnOrchestrator orchB =
                TxnOrchestratorFactory.create(
                        TxnEngineConfig.planA(),
                        metastore,
                        lmdbB,
                        CATALOG,
                        DATABASE,
                        TABLE,
                        null,
                        trackerB);
        orchestratorA = (TxnOrchestratorImpl) orchA;
        orchestratorB = (TxnOrchestratorImpl) orchB;

        TxnCoordinatorGrpcService serviceA = new TxnCoordinatorGrpcService(orchestratorA);
        TxnCoordinatorGrpcService serviceB = new TxnCoordinatorGrpcService(orchestratorB);

        String nameA = "csm-A-" + UUID.randomUUID();
        String nameB = "csm-B-" + UUID.randomUUID();
        grpcServerA =
                InProcessServerBuilder.forName(nameA)
                        .directExecutor()
                        .addService(serviceA)
                        .build()
                        .start();
        grpcServerB =
                InProcessServerBuilder.forName(nameB)
                        .directExecutor()
                        .addService(serviceB)
                        .build()
                        .start();
        channelA = InProcessChannelBuilder.forName(nameA).directExecutor().build();
        channelB = InProcessChannelBuilder.forName(nameB).directExecutor().build();

        routingTable = new InMemoryShardRoutingTable();
        routingCoordinator = new InMemoryRoutingCoordinator(routingTable);

        NodeSlot slotA = new NodeSlot(EXEC_A, 0);
        NodeSlot slotB = new NodeSlot(EXEC_B, 0);
        routingTable.registerExecutor(EXEC_A, slotA);
        routingTable.registerExecutor(EXEC_B, slotB);
        routingTable.markExecutorReady(EXEC_A);
        routingTable.markExecutorReady(EXEC_B);

        routingTable.assignShard(
                new ShardAssignment(new ShardId(0), slotA, EXEC_A, RoutingEpoch.initial()));
        routingTable.assignShard(
                new ShardAssignment(new ShardId(1), slotB, EXEC_B, RoutingEpoch.initial()));
    }

    @AfterEach
    void tearDown() {
        if (channelA != null) channelA.shutdown();
        if (channelB != null) channelB.shutdown();
        if (grpcServerA != null) grpcServerA.shutdown();
        if (grpcServerB != null) grpcServerB.shutdown();
        if (orchestratorA != null) orchestratorA.close();
        if (orchestratorB != null) orchestratorB.close();
        if (lmdbA != null) lmdbA.close();
        if (lmdbB != null) lmdbB.close();
    }

    private DefaultServerTxnCoordinator newCoordinator() {
        return new DefaultServerTxnCoordinator(
                Map.of(EXEC_A, channelA, EXEC_B, channelB), routingCoordinator, 30);
    }

    @Test
    @DisplayName(
            "S1: cross-shard beginBatch(expectedShardCount=2) -> commitBatch -> CommitOk via 2 executors")
    void testCrossShardBeginBatchCommitBatch() {
        DefaultServerTxnCoordinator coordinator = newCoordinator();
        String batchId = "cross-commit-" + UUID.randomUUID();

        var handle = coordinator.beginBatch(TABLE_ID, batchId, 2);

        assertThat(handle).isNotNull();
        assertThat(handle.expectedShardCount()).isEqualTo(2);
        assertThat(handle.shardAssignments()).hasSize(2);
        assertThat(handle.shardAssignments()).containsKeys(0, 1);

        TxnResult result = coordinator.commitBatch(batchId);

        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
    }

    @Test
    @DisplayName("S1: cross-shard abortBatch -> AbortResult (all executors abort)")
    void testCrossShardAbortBatch() {
        DefaultServerTxnCoordinator coordinator = newCoordinator();
        String batchId = "cross-abort-" + UUID.randomUUID();

        coordinator.beginBatch(TABLE_ID, batchId, 2);

        TxnResult result = coordinator.abortBatch(batchId);

        assertThat(result).isInstanceOf(TxnResult.AbortResult.class);
    }

    @Test
    @DisplayName(
            "S3: BE Server crash (restart) -> decideBatchRecovery queries executors -> not ACTIVE_BATCH")
    void testDecideBatchRecoveryAfterCoordinatorRestart() {
        DefaultServerTxnCoordinator coordinator1 = newCoordinator();
        String batchId = "crash-recovery-" + UUID.randomUUID();

        coordinator1.beginBatch(TABLE_ID, batchId, 2);

        DefaultServerTxnCoordinator coordinator2 = newCoordinator();

        RecoveryDecision decision = coordinator2.decideBatchRecovery(batchId);

        assertThat(decision).isNotNull();
        assertThat(decision.type())
                .as("after restart batch is not in memory; must query executors")
                .isNotEqualTo(RecoveryDecision.DecisionType.ACTIVE_BATCH);
    }

    @Test
    @DisplayName("S4: commitBatch with unknown batchId -> CommitConflict")
    void testCommitBatchUnknownId() {
        DefaultServerTxnCoordinator coordinator = newCoordinator();

        TxnResult result = coordinator.commitBatch("nonexistent-cross-batch");

        assertThat(result).isInstanceOf(TxnResult.CommitConflict.class);
    }

    @Test
    @DisplayName("S4: abortBatch with unknown batchId -> AbortResult")
    void testAbortBatchUnknownId() {
        DefaultServerTxnCoordinator coordinator = newCoordinator();

        TxnResult result = coordinator.abortBatch("nonexistent-cross-abort");

        assertThat(result).isInstanceOf(TxnResult.AbortResult.class);
    }
}
