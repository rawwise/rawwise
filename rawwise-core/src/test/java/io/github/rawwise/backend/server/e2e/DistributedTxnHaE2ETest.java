package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.rawwise.backend.executor.service.TxnCoordinatorGrpcService;
import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingEpoch;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.server.ha.BackendServerHaCoordinator;
import io.github.rawwise.backend.server.txn.BatchRecoveryExecutor;
import io.github.rawwise.backend.server.txn.DefaultServerTxnCoordinator;
import io.github.rawwise.backend.server.txn.RecoveryDecision;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.RecordHeaderProbe;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.ingest.BatchState;
import io.github.rawwise.engine.txn.ingest.CommitLedger;
import io.github.rawwise.engine.txn.ingest.LeaseInfo;
import io.github.rawwise.engine.txn.ingest.MetastoreCommitLedgerStore;
import io.github.rawwise.engine.txn.ingest.MetastoreGlobalCommitMarkerStore;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorFactory;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorImpl;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.txn.BeginBatchRequest;
import io.github.rawwise.txn.BeginBatchResponse;
import io.github.rawwise.txn.TxnCoordinatorServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 3.3-T7: distributedtransaction + BE Server HA E2E test.
 *
 * <p>validationthreelargeScenario:
 *
 * <ol>
 *   <li><b>persistence crash recovery</b>: BE Server crashafterre-, BatchRecoveryExecutor
 *       scannotcompletesuccess CommitLedger andadvancetoendstate(OPEN/SHARDS_PREPARED -> ABORTED,
 *       DATA_DURABLE -> COMMITTED)
 *   <li><b>Active-Standby failover</b>: Active BE Server lease expiredafter, Standby viapass CAS
 *       grabprimary, epoch monotonically increasing, RTO &lt; lease TTL
 *   <li><b>lease epoch reject</b>: old Active(low epoch)recoveryaftersendtransactiondirective, BE
 *       Node TxnCoordinatorGrpcService reject STALE_EPOCH request
 * </ol>
 *
 * <p>follow RawWise E2E teststandard:
 *
 * <ul>
 *   <li>makeusetrue LMDB + InProcess gRPC(non- mock keylinkgroupitem)
 *   <li>makeusetrue MetastoreCommitLedgerStore / MetastoreGlobalCommitMarkerStore(non- mock)
 *   <li>makeusetrue BackendServerHaCoordinator + Metastore CAS(non- mock)
 *   <li>makeusetrue TxnOrchestratorImpl(non- mock)
 *   <li>InMemoryMetastore simulateindependentestablish Metastore service(cross coordinator instance
 *       sharingstate, andproduction modelconsistent——Metastore isindependent process)
 * </ul>
 */
@DisplayName(
        "Phase 3.3-T7: Distributed Txn Crash Recovery + Active-Standby HA + Epoch Rejection E2E")
class DistributedTxnHaE2ETest {

    private static final String CATALOG = "csm_catalog";
    private static final String DATABASE = "default";
    private static final String TABLE = "csm_table";
    private static final String TABLE_ID = CATALOG + "." + DATABASE + "." + TABLE;
    private static final String EXEC_A = "exec-A";
    private static final String EXEC_B = "exec-B";

    @TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private MetastoreCommitLedgerStore commitLedgerStore;
    private MetastoreGlobalCommitMarkerStore globalCommitMarkerStore;
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

        commitLedgerStore = new MetastoreCommitLedgerStore(metastore);
        globalCommitMarkerStore = new MetastoreGlobalCommitMarkerStore(metastore);

        Path dirA = Files.createDirectories(tempDir.resolve("node-a"));
        Path dirB = Files.createDirectories(tempDir.resolve("node-b"));
        lmdbA = new LmdbInstance("ha-A-" + UUID.randomUUID(), dirA, 128L * 1024 * 1024, 5);
        lmdbB = new LmdbInstance("ha-B-" + UUID.randomUUID(), dirB, 128L * 1024 * 1024, 5);

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

        String nameA = "ha-A-" + UUID.randomUUID();
        String nameB = "ha-B-" + UUID.randomUUID();
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

    /** constructwithpersistpathcoordinator(inject CommitLedgerStore + GlobalCommitMarkerStore). */
    private DefaultServerTxnCoordinator newPersistentCoordinator() {
        return new DefaultServerTxnCoordinator(
                Map.of(EXEC_A, channelA, EXEC_B, channelB),
                routingCoordinator,
                30,
                commitLedgerStore,
                globalCommitMarkerStore);
    }

    private LeaseInfo testLeaseInfo() {
        return new LeaseInfo("test-driver", System.currentTimeMillis() + 300_000L, 1L);
    }

    // ============================================================================================
    // Scenario 1: persistence crash recovery
    // ============================================================================================

    @Test
    @DisplayName(
            "S1: Crash recovery — OPEN ledger -> ABORTED (no in-memory handle, simulates BE Server restart)")
    void testCrashRecoveryAbortsOpenLedger() throws Exception {
        String idempotencyKey = "crash-open-" + UUID.randomUUID();
        CommitLedger openLedger =
                CommitLedger.open(idempotencyKey, idempotencyKey, TABLE_ID, 2, 0L, testLeaseInfo());
        commitLedgerStore.save(openLedger);

        DefaultServerTxnCoordinator coordinator = newPersistentCoordinator();
        BatchRecoveryExecutor recoveryExecutor =
                new BatchRecoveryExecutor(coordinator, commitLedgerStore);

        BatchRecoveryExecutor.RecoverySummary summary = recoveryExecutor.recoverAll();

        assertThat(summary.total()).isGreaterThanOrEqualTo(1);
        assertThat(summary.recovered()).isGreaterThanOrEqualTo(1);

        Optional<CommitLedger> reloaded = commitLedgerStore.load(idempotencyKey);
        assertThat(reloaded).isPresent();
        assertThat(reloaded.get().state())
                .as("OPEN ledger must be aborted during crash recovery (data not globally visible)")
                .isEqualTo(BatchState.ABORTED);
    }

    @Test
    @DisplayName(
            "S2: Crash recovery — SHARDS_PREPARED ledger -> ABORTED (data not globally visible)")
    void testCrashRecoveryAbortsShardsPreparedLedger() throws Exception {
        String idempotencyKey = "crash-prepared-" + UUID.randomUUID();
        CommitLedger ledger =
                CommitLedger.open(idempotencyKey, idempotencyKey, TABLE_ID, 2, 0L, testLeaseInfo());
        commitLedgerStore.save(ledger);
        commitLedgerStore.updateState(idempotencyKey, BatchState.SHARDS_PREPARED);

        DefaultServerTxnCoordinator coordinator = newPersistentCoordinator();
        BatchRecoveryExecutor recoveryExecutor =
                new BatchRecoveryExecutor(coordinator, commitLedgerStore);

        BatchRecoveryExecutor.RecoverySummary summary = recoveryExecutor.recoverAll();
        assertThat(summary.recovered()).isGreaterThanOrEqualTo(1);

        Optional<CommitLedger> reloaded = commitLedgerStore.load(idempotencyKey);
        assertThat(reloaded).isPresent();
        assertThat(reloaded.get().state())
                .as("SHARDS_PREPARED ledger must be aborted (still abortable per BatchState rules)")
                .isEqualTo(BatchState.ABORTED);
    }

    @Test
    @DisplayName(
            "S3: Crash recovery — DATA_DURABLE ledger -> COMMITTED (continue-commit path, past durability boundary)")
    void testCrashRecoveryContinuesCommitDataDurableLedger() throws Exception {
        String idempotencyKey = "crash-durable-" + UUID.randomUUID();
        CommitLedger ledger =
                CommitLedger.open(idempotencyKey, idempotencyKey, TABLE_ID, 2, 0L, testLeaseInfo());
        commitLedgerStore.save(ledger);
        commitLedgerStore.updateState(idempotencyKey, BatchState.SHARDS_PREPARED);
        commitLedgerStore.updateState(idempotencyKey, BatchState.DATA_DURABLE);

        DefaultServerTxnCoordinator coordinator = newPersistentCoordinator();
        BatchRecoveryExecutor recoveryExecutor =
                new BatchRecoveryExecutor(coordinator, commitLedgerStore);

        BatchRecoveryExecutor.RecoverySummary summary = recoveryExecutor.recoverAll();
        assertThat(summary.recovered()).isGreaterThanOrEqualTo(1);

        Optional<CommitLedger> reloaded = commitLedgerStore.load(idempotencyKey);
        assertThat(reloaded).isPresent();
        assertThat(reloaded.get().state())
                .as(
                        "DATA_DURABLE ledger must be committed via continue-commit (data already durable)")
                .isEqualTo(BatchState.COMMITTED);
    }

    @Test
    @DisplayName("S4: Crash recovery — COMMITTED ledger -> skipped (already terminal)")
    void testCrashRecoverySkipsCommittedLedger() throws Exception {
        String idempotencyKey = "crash-committed-" + UUID.randomUUID();
        CommitLedger ledger =
                CommitLedger.open(idempotencyKey, idempotencyKey, TABLE_ID, 2, 0L, testLeaseInfo());
        commitLedgerStore.save(ledger);
        commitLedgerStore.updateState(idempotencyKey, BatchState.SHARDS_PREPARED);
        commitLedgerStore.updateState(idempotencyKey, BatchState.DATA_DURABLE);
        commitLedgerStore.updateState(idempotencyKey, BatchState.GLOBAL_MARKER_COMMITTED);
        commitLedgerStore.updateState(idempotencyKey, BatchState.COMMITTED);

        DefaultServerTxnCoordinator coordinator = newPersistentCoordinator();
        BatchRecoveryExecutor recoveryExecutor =
                new BatchRecoveryExecutor(coordinator, commitLedgerStore);

        RecoveryDecision decision = coordinator.recoverBatch(idempotencyKey);
        assertThat(decision.type())
                .as("COMMITTED ledger must be detected as ALREADY_COMMITTED (idempotent)")
                .isEqualTo(RecoveryDecision.DecisionType.ALREADY_COMMITTED);

        Optional<CommitLedger> reloaded = commitLedgerStore.load(idempotencyKey);
        assertThat(reloaded).isPresent();
        assertThat(reloaded.get().state()).isEqualTo(BatchState.COMMITTED);
    }

    @Test
    @DisplayName("S5: Crash recovery — no incomplete batches -> RecoverySummary(0,0,0)")
    void testCrashRecoveryNoIncompleteBatches() {
        DefaultServerTxnCoordinator coordinator = newPersistentCoordinator();
        BatchRecoveryExecutor recoveryExecutor =
                new BatchRecoveryExecutor(coordinator, commitLedgerStore);

        BatchRecoveryExecutor.RecoverySummary summary = recoveryExecutor.recoverAll();
        assertThat(summary.recovered()).isEqualTo(0);
        assertThat(summary.failed()).isEqualTo(0);
        assertThat(summary.skipped()).isEqualTo(0);
    }

    // ============================================================================================
    // Scenario 2: Active-Standby failover
    // ============================================================================================

    @Test
    @DisplayName("S6: Active-Standby failover — Active lease expires -> Standby acquires, epoch+1")
    void testActiveStandbyFailover() throws Exception {
        Metastore haMetastore = new InMemoryMetastore();
        long shortTtlMs = 500L;
        long shortPollMs = 100L;

        BackendServerHaCoordinator active1 =
                new BackendServerHaCoordinator(
                        haMetastore, "server-1", "host1:9090", shortTtlMs, 200L, shortPollMs);
        BackendServerHaCoordinator active2 =
                new BackendServerHaCoordinator(
                        haMetastore, "server-2", "host2:9090", shortTtlMs, 200L, shortPollMs);

        assertThat(active1.tryAcquireActive()).as("First server acquires ACTIVE role").isTrue();
        assertThat(active1.isActive()).isTrue();
        assertThat(active1.getCurrentEpoch()).as("Initial Active epoch = 1").isEqualTo(1L);

        assertThat(active2.tryAcquireActive())
                .as("Standby cannot acquire while Active lease is valid")
                .isFalse();
        assertThat(active2.isActive()).isFalse();

        Thread.sleep(shortTtlMs + 150L);

        assertThat(active2.tryAcquireActive())
                .as("Standby acquires ACTIVE after lease expires")
                .isTrue();
        assertThat(active2.isActive()).isTrue();
        assertThat(active2.getCurrentEpoch())
                .as("Standby takeover increments epoch to 2")
                .isEqualTo(2L);

        Optional<String> activeAddr = active2.getActiveAddress();
        assertThat(activeAddr)
                .as("Active address is exposed for Frontend discovery")
                .isPresent()
                .contains("host2:9090");

        Optional<String> staleAddr = active1.getActiveAddress();
        assertThat(staleAddr)
                .as("Metastore reflects new Active (host2), not stale host1")
                .isPresent()
                .contains("host2:9090");

        assertThat(active1.getCurrentEpoch())
                .as(
                        "Old Active's local epoch view is stale (1), split-brain protection via epoch comparison at BE Node")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName(
            "S7: Active-Standby failover — Standby polling acquires Active automatically within RTO (crash path, epoch+1)")
    void testStandbyPollingAutoAcquiresActive() throws Exception {
        Metastore haMetastore = new InMemoryMetastore();
        long shortTtlMs = 400L;
        long shortPollMs = 80L;

        // Simulate crash: acquire lease but DON'T start renewal (no start() call).
        // Lease will expire naturally -> Standby takeover path -> epoch+1.
        BackendServerHaCoordinator active1 =
                new BackendServerHaCoordinator(
                        haMetastore, "server-1", "host1:9090", shortTtlMs, 150L, shortPollMs);
        active1.tryAcquireActive();
        assertThat(active1.isActive()).isTrue();
        assertThat(active1.getCurrentEpoch()).isEqualTo(1L);

        BackendServerHaCoordinator active2 =
                new BackendServerHaCoordinator(
                        haMetastore, "server-2", "host2:9090", shortTtlMs, 150L, shortPollMs);
        active2.start();
        assertThat(active2.isActive())
                .as("Standby polling does not acquire while Active lease is valid")
                .isFalse();

        long rtoStart = System.currentTimeMillis();

        await().atMost(2, TimeUnit.SECONDS)
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertThat(active2.isActive())
                                        .as(
                                                "Standby auto-acquires ACTIVE via polling within RTO (< TTL + poll interval)")
                                        .isTrue());

        long rtoMs = System.currentTimeMillis() - rtoStart;
        assertThat(rtoMs)
                .as("RTO must be < lease TTL + 2*pollInterval (got " + rtoMs + "ms)")
                .isLessThan(shortTtlMs + 2 * shortPollMs + 200L);
        assertThat(active2.getCurrentEpoch())
                .as("Crash-path takeover (expired lease) increments epoch 1 → 2")
                .isEqualTo(2L);

        active2.stop();
    }

    @Test
    @DisplayName(
            "S8: Active voluntary release — stop() deletes lease, Standby acquires immediately (NotExists path, epoch=1)")
    void testVoluntaryReleaseAllowsImmediateTakeover() throws Exception {
        // Voluntary release = clean shutdown: stop() deletes the CAS key.
        // Next acquisition uses NotExists path -> epoch=1 (fresh lease, no split-brain risk
        // because old Active cleanly released). In production, BE Nodes should also restart
        // on clean shutdown to reset acceptedEpoch.
        Metastore haMetastore = new InMemoryMetastore();
        long ttlMs = 5_000L;
        BackendServerHaCoordinator active1 =
                new BackendServerHaCoordinator(
                        haMetastore, "server-1", "host1:9090", ttlMs, 1_000L, 200L);
        active1.tryAcquireActive();
        active1.start();
        assertThat(active1.isActive()).isTrue();

        active1.stop();

        BackendServerHaCoordinator active2 =
                new BackendServerHaCoordinator(
                        haMetastore, "server-2", "host2:9090", ttlMs, 1_000L, 200L);
        long acquireStart = System.currentTimeMillis();
        assertThat(active2.tryAcquireActive())
                .as(
                        "After voluntary release (delete), Standby acquires immediately without waiting for TTL")
                .isTrue();
        long acquireMs = System.currentTimeMillis() - acquireStart;
        assertThat(acquireMs)
                .as("Immediate acquisition (no TTL wait) — took " + acquireMs + "ms")
                .isLessThan(500L);
        assertThat(active2.getCurrentEpoch())
                .as("Voluntary release (NotExists path) starts fresh epoch=1 (no takeover)")
                .isEqualTo(1L);

        active2.stop();
    }

    // ============================================================================================
    // Scenario 3: lease epoch reject (Split-Brain protection)
    // ============================================================================================

    @Test
    @DisplayName(
            "S9: Epoch monotonic — BE Node accepts epoch=1, then epoch=2, updates acceptedEpoch")
    void testEpochMonotonicAcceptance() {
        TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub =
                TxnCoordinatorServiceGrpc.newBlockingStub(channelA);

        BeginBatchResponse resp1 =
                stub.beginBatch(
                        BeginBatchRequest.newBuilder()
                                .setBatchId("epoch-1-" + UUID.randomUUID())
                                .setTableId(TABLE_ID)
                                .setExpectedShardCount(2)
                                .setEpoch(1L)
                                .build());
        assertThat(resp1.getBatchId()).isNotEmpty();

        BeginBatchResponse resp2 =
                stub.beginBatch(
                        BeginBatchRequest.newBuilder()
                                .setBatchId("epoch-2-" + UUID.randomUUID())
                                .setTableId(TABLE_ID)
                                .setExpectedShardCount(2)
                                .setEpoch(2L)
                                .build());
        assertThat(resp2.getBatchId()).isNotEmpty();
    }

    @Test
    @DisplayName(
            "S10: STALE_EPOCH — BE Node rejects request with epoch < acceptedEpoch (split-brain protection)")
    void testStaleEpochRejection() {
        TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub =
                TxnCoordinatorServiceGrpc.newBlockingStub(channelA);

        stub.beginBatch(
                BeginBatchRequest.newBuilder()
                        .setBatchId("stale-ok-1-" + UUID.randomUUID())
                        .setTableId(TABLE_ID)
                        .setExpectedShardCount(2)
                        .setEpoch(5L)
                        .build());

        assertThatThrownBy(
                        () ->
                                stub.beginBatch(
                                        BeginBatchRequest.newBuilder()
                                                .setBatchId("stale-reject-" + UUID.randomUUID())
                                                .setTableId(TABLE_ID)
                                                .setExpectedShardCount(2)
                                                .setEpoch(3L)
                                                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("STALE_EPOCH")
                .hasMessageContaining("3")
                .hasMessageContaining("5");
    }

    @Test
    @DisplayName(
            "S11: Epoch=0 bypass — unconfigured HA (epoch=0) is accepted for backward compatibility")
    void testEpochZeroBypass() {
        TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub =
                TxnCoordinatorServiceGrpc.newBlockingStub(channelA);

        stub.beginBatch(
                BeginBatchRequest.newBuilder()
                        .setBatchId("epoch-0-" + UUID.randomUUID())
                        .setTableId(TABLE_ID)
                        .setExpectedShardCount(2)
                        .setEpoch(10L)
                        .build());

        BeginBatchResponse resp =
                stub.beginBatch(
                        BeginBatchRequest.newBuilder()
                                .setBatchId("epoch-0-bypass-" + UUID.randomUUID())
                                .setTableId(TABLE_ID)
                                .setExpectedShardCount(2)
                                .setEpoch(0L)
                                .build());
        assertThat(resp.getBatchId()).isNotEmpty();
    }

    @Test
    @DisplayName(
            "S12: End-to-end split-brain — Active1(epoch=1) -> Standby takeover(epoch=2) -> old Active1 rejected")
    void testEndToEndSplitBrainRejection() throws Exception {
        Metastore haMetastore = new InMemoryMetastore();
        long shortTtlMs = 400L;
        BackendServerHaCoordinator active1 =
                new BackendServerHaCoordinator(
                        haMetastore, "server-1", "host1:9090", shortTtlMs, 150L, 100L);
        active1.tryAcquireActive();
        long oldEpoch = active1.getCurrentEpoch();
        assertThat(oldEpoch).isEqualTo(1L);

        TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub =
                TxnCoordinatorServiceGrpc.newBlockingStub(channelA);

        // Step 1: Old Active sends epoch=1 -> BE Node accepts (acceptedEpoch 0 -> 1)
        BeginBatchResponse accepted =
                stub.beginBatch(
                        BeginBatchRequest.newBuilder()
                                .setBatchId("split-accepted-" + UUID.randomUUID())
                                .setTableId(TABLE_ID)
                                .setExpectedShardCount(2)
                                .setEpoch(oldEpoch)
                                .build());
        assertThat(accepted.getBatchId()).isNotEmpty();

        // Step 2: Lease expires, Standby takes over (crash path -> epoch+1=2)
        Thread.sleep(shortTtlMs + 100L);
        BackendServerHaCoordinator active2 =
                new BackendServerHaCoordinator(
                        haMetastore, "server-2", "host2:9090", shortTtlMs, 150L, 100L);
        assertThat(active2.tryAcquireActive()).isTrue();
        long newEpoch = active2.getCurrentEpoch();
        assertThat(newEpoch).isGreaterThan(oldEpoch);

        // Step 3: New Active sends epoch=2 -> BE Node accepts (acceptedEpoch 1 -> 2)
        BeginBatchResponse newActiveAccepted =
                stub.beginBatch(
                        BeginBatchRequest.newBuilder()
                                .setBatchId("split-new-active-" + UUID.randomUUID())
                                .setTableId(TABLE_ID)
                                .setExpectedShardCount(2)
                                .setEpoch(newEpoch)
                                .build());
        assertThat(newActiveAccepted.getBatchId()).isNotEmpty();

        // Step 4: Old Active (still epoch=1) sends request -> REJECTED (1 < acceptedEpoch=2)
        assertThatThrownBy(
                        () ->
                                stub.beginBatch(
                                        BeginBatchRequest.newBuilder()
                                                .setBatchId("split-rejected-" + UUID.randomUUID())
                                                .setTableId(TABLE_ID)
                                                .setExpectedShardCount(2)
                                                .setEpoch(oldEpoch)
                                                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("STALE_EPOCH")
                .hasMessageContaining(String.valueOf(oldEpoch))
                .hasMessageContaining(String.valueOf(newEpoch));
    }
}
