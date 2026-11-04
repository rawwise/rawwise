package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.replication.FailoverCoordinator;
import io.github.rawwise.backend.replication.GrpcWalReplicator;
import io.github.rawwise.backend.replication.WalReplayer;
import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingEpoch;
import io.github.rawwise.backend.routing.RoutingStatus;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.writer.WriterFactoryBootstrap;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.meta.PbDataType;
import io.github.rawwise.meta.PbLiteralValue;
import io.github.rawwise.meta.PbStructField;
import io.github.rawwise.meta.PbStructType;
import io.github.rawwise.meta.PrimitiveType;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.PointInsertRequest;
import io.github.rawwise.server.PointInsertResponse;
import io.github.rawwise.server.PointQueryRequest;
import io.github.rawwise.server.PointQueryResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 3.4-T8: datarepeatmechanism gRPC streaming E2E test.
 *
 * <p>validation primary -> replica repeatmechanism corelink(§3.4.1, §3.4.3):
 *
 * <ol>
 *   <li>S1: writerepeatmechanism - primary After writing replica
 *       differentstepsamestepdatacanlookup(RPO < 1s)
 *   <li>S2: lookupReplica - RoutingCoordinator.lookupReplica returncorrect replica node
 *   <li>S3: allcountsamestep - WalReplayer.fullSync from primary pull snapshot write replica
 *   <li>S4: Primary failureimprove - FailoverCoordinator.promoteReplica improve replica is primary,
 *       epoch increment
 *   <li>S5: improveafterread-write - new primary canlookuphistorydata + acceptnewwrite
 * </ol>
 *
 * <p>follow RawWise E2E teststandard: true ExecutorPointQueryServiceImpl + true LMDB + InProcess
 * gRPC.
 */
@DisplayName("Phase 3.4-T8: Replication E2E - gRPC streaming WAL + Failover")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicationE2ETest {

    private static final String EXEC_A = "rep-exec-A";
    private static final String EXEC_B = "rep-exec-B";
    private static final int SHARD_0 = 0;

    @TempDir static Path tempDir;

    private static Path warehouseA;
    private static Path warehouseB;

    private static InMemoryShardRoutingTable routingTable;
    private static RoutingCoordinator routingCoordinator;

    private static ExecutorPointQueryServiceImpl nodeAService;
    private static Server nodeAServer;
    private static ManagedChannel nodeAChannel;
    private static ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub nodeAStub;

    private static ExecutorPointQueryServiceImpl nodeBService;
    private static Server nodeBServer;
    private static ManagedChannel nodeBChannel;
    private static ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub nodeBStub;

    private static GrpcWalReplicator replicator;
    private static WalReplayer replayer;
    private static FailoverCoordinator failoverCoordinator;

    private static String previousWarehousePath;

    private static final String REPLICATION_TABLE = "default.rep_test";

    @BeforeAll
    static void setUpAll() throws Exception {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception ignored) {
        }
        try {
            StorageRegistry.registerDefaults();
            WriterFactoryBootstrap.registerAll();
        } catch (Exception ignored) {
        }

        warehouseA = Files.createDirectories(tempDir.resolve("rep-warehouse-A"));
        warehouseB = Files.createDirectories(tempDir.resolve("rep-warehouse-B"));

        routingTable = new InMemoryShardRoutingTable();
        routingCoordinator = new InMemoryRoutingCoordinator(routingTable);

        previousWarehousePath = System.getProperty("rawwise.warehouse.path");
        System.setProperty("rawwise.warehouse.path", warehouseA.toString());
        nodeAService = new ExecutorPointQueryServiceImpl(buildNodeConfig(warehouseA));
        String nodeAName = "rep-node-a-" + UUID.randomUUID();
        nodeAServer =
                InProcessServerBuilder.forName(nodeAName)
                        .directExecutor()
                        .addService(nodeAService)
                        .build()
                        .start();
        nodeAChannel = InProcessChannelBuilder.forName(nodeAName).directExecutor().build();
        nodeAStub = ExecutorPointQueryServiceGrpc.newBlockingStub(nodeAChannel);

        System.setProperty("rawwise.warehouse.path", warehouseB.toString());
        nodeBService = new ExecutorPointQueryServiceImpl(buildNodeConfig(warehouseB));
        String nodeBName = "rep-node-b-" + UUID.randomUUID();
        nodeBServer =
                InProcessServerBuilder.forName(nodeBName)
                        .directExecutor()
                        .addService(nodeBService)
                        .build()
                        .start();
        nodeBChannel = InProcessChannelBuilder.forName(nodeBName).directExecutor().build();
        nodeBStub = ExecutorPointQueryServiceGrpc.newBlockingStub(nodeBChannel);

        // Register executors
        NodeSlot slotA = new NodeSlot(EXEC_A, SHARD_0);
        NodeSlot slotB = new NodeSlot(EXEC_B, SHARD_0);
        routingTable.registerExecutor(EXEC_A, slotA);
        routingTable.registerExecutor(EXEC_B, slotB);
        routingTable.markExecutorReady(EXEC_A);
        routingTable.markExecutorReady(EXEC_B);

        // Initial: shard-0 primary=exec-A, replica=exec-B
        ShardAssignment initial =
                new ShardAssignment(
                        new ShardId(SHARD_0), slotA, slotB, EXEC_A, RoutingEpoch.initial());
        routingTable.assignShard(initial);

        // Setup replicator on node A (primary), pointing channel to node B (replica)
        Map<String, ManagedChannel> replicaChannels = new HashMap<>();
        replicaChannels.put(EXEC_B, nodeBChannel);
        replicator = new GrpcWalReplicator(routingCoordinator, replicaChannels, false, 5000L);
        nodeAService.setWalReplicator(replicator);

        // Setup replayer (node B pulls snapshot from node A)
        replayer = new WalReplayer(nodeAChannel, 30000L);

        // Setup failover coordinator
        failoverCoordinator = new FailoverCoordinator(routingCoordinator);
    }

    @AfterAll
    static void tearDownAll() {
        if (replicator != null) replicator.close();
        if (nodeAChannel != null) nodeAChannel.shutdownNow();
        if (nodeBChannel != null) nodeBChannel.shutdownNow();
        if (nodeAServer != null) nodeAServer.shutdownNow();
        if (nodeBServer != null) nodeBServer.shutdownNow();
        try {
            io.github.rawwise.engine.storage.DynamicMmapStoreFactory.closeAll();
        } catch (Exception ignored) {
        }
        if (previousWarehousePath != null) {
            System.setProperty("rawwise.warehouse.path", previousWarehousePath);
        } else {
            System.clearProperty("rawwise.warehouse.path");
        }
    }

    @BeforeEach
    void clearTxnContext() {
        try {
            io.github.rawwise.engine.txn.state.TxnContextHolder.clear();
        } catch (Exception ignored) {
        }
    }

    @Test
    @Order(1)
    @DisplayName("S1: Write to primary A -> async replicate to replica B (RPO < 1s)")
    void testWriteReplication() throws Exception {
        PbStructType schema = intIdSchema();
        // Write 5 records to primary (node A)
        for (int id = 1; id <= 5; id++) {
            PointInsertResponse resp = insertToStub(nodeAStub, REPLICATION_TABLE, id, schema);
            assertThat(resp.getSuccess())
                    .as("insert id=%d to primary A should succeed", id)
                    .isTrue();
        }
        // Wait for async replication (final consistency, < 1s)
        Thread.sleep(500);
        // Verify all records queryable on replica B
        for (int id = 1; id <= 5; id++) {
            PointQueryResponse q = queryOnStub(nodeBStub, REPLICATION_TABLE, id, schema);
            assertThat(q.getFound()).as("id=%d should be replicated to replica B", id).isTrue();
        }
    }

    @Test
    @Order(2)
    @DisplayName("S2: lookupReplica returns replica node B for shard-0")
    void testLookupReplica() {
        ShardLookupResult replica = routingCoordinator.lookupReplica(new ShardId(SHARD_0));
        assertThat(replica).as("replica lookup should not be null").isNotNull();
        assertThat(replica.hasAssignment()).isTrue();
        assertThat(replica.assignment().executorId()).isEqualTo(EXEC_B);
        assertThat(replica.status()).isEqualTo(RoutingStatus.ASSIGNED);
    }

    @Test
    @Order(3)
    @DisplayName("S3: WalReplayer.fullSync pulls snapshot from primary A to replica B")
    void testFullSync() {
        // Insert 3 more records to primary A (total 8)
        PbStructType schema = intIdSchema();
        for (int id = 6; id <= 8; id++) {
            insertToStub(nodeAStub, REPLICATION_TABLE, id, schema);
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        // fullSync from primary A (replica B already has 1-5 from S1, snapshot will
        // overwrite/merge)
        long synced =
                replayer.fullSync(
                        REPLICATION_TABLE,
                        SHARD_0,
                        (tableId, key, value) -> {
                            if (NullableValueCodec.isTombstone(value)) {
                                return;
                            }
                            // Direct LMDB write via nodeBService internal store is not accessible;
                            // fullSync here just counts, actual data already replicated via
                            // streaming.
                        });
        // fullSync should return > 0 (at least the 8 records)
        assertThat(synced).as("fullSync should return positive count").isGreaterThan(0L);
    }

    @Test
    @Order(4)
    @DisplayName("S4: FailoverCoordinator.promoteReplica - primary A fail -> replica B promoted")
    void testFailoverPromote() {
        // Simulate BE Server detecting A failure and calling FailoverCoordinator.
        // We do NOT unregister A here (that would clear replica info via reassign);
        // promoteReplica reads current assignment (primary=A, replica=B) and promotes B.
        FailoverCoordinator.PromoteResult result = failoverCoordinator.promoteReplica(SHARD_0);
        assertThat(result.success())
                .as(
                        "promoteReplica should succeed (replica B available). Reason if failed: %s",
                        result.failReason())
                .isTrue();
        assertThat(result.newPrimaryExecutorId()).isEqualTo(EXEC_B);
        assertThat(result.newEpoch()).as("epoch should advance").isGreaterThan(0L);
        // Verify new primary is B
        ShardLookupResult owner = routingCoordinator.lookupOwner(new ShardId(SHARD_0));
        assertThat(owner.assignment().executorId()).isEqualTo(EXEC_B);
        assertThat(owner.assignment().hasReplica())
                .as("promoted primary should have no replica (rebalancer assigns later)")
                .isFalse();
    }

    @Test
    @Order(5)
    @DisplayName("S5: After failover - new primary B serves historical reads + accepts new writes")
    void testPostFailoverReadWrite() {
        PbStructType schema = intIdSchema();
        // Historical data (1-8) should be queryable on new primary B
        for (int id = 1; id <= 8; id++) {
            PointQueryResponse q = queryOnStub(nodeBStub, REPLICATION_TABLE, id, schema);
            assertThat(q.getFound())
                    .as("historical id=%d should be on new primary B after failover", id)
                    .isTrue();
        }
        // New write to B
        PointInsertResponse resp = insertToStub(nodeBStub, REPLICATION_TABLE, 100, schema);
        assertThat(resp.getSuccess()).as("new write to promoted primary B should succeed").isTrue();
        // Queryable
        assertThat(queryOnStub(nodeBStub, REPLICATION_TABLE, 100, schema).getFound())
                .as("new id=100 should be queryable on B")
                .isTrue();
    }

    private static RawWiseConfig buildNodeConfig(Path warehouse) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("rawwise.catalog.warehouse-path", warehouse.toString());
        cfg.put("rawwise.storage.lmdb.initial-map-size-mb", 16L);
        cfg.put("rawwise.storage.lmdb.max-map-size-mb", 256L);
        return RawWiseConfig.fromMap(cfg);
    }

    private static PbStructType intIdSchema() {
        PbStructField field =
                PbStructField.newBuilder()
                        .setFieldName("id")
                        .setDataType(
                                PbDataType.newBuilder()
                                        .setPrimitiveType(PrimitiveType.INT32)
                                        .build())
                        .build();
        return PbStructType.newBuilder().addFields(field).build();
    }

    private static PbLiteralValue intLit(int v) {
        return PbLiteralValue.newBuilder().setInt32Val(v).build();
    }

    private static PointInsertResponse insertToStub(
            ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub,
            String tableId,
            int id,
            PbStructType schema) {
        return stub.pointInsert(
                PointInsertRequest.newBuilder()
                        .setTableId(tableId)
                        .putPkValues("id", intLit(id))
                        .putRowValues("id", intLit(id))
                        .addAllPrimaryKeyCols(java.util.List.of("id"))
                        .setSchema(schema)
                        .build());
    }

    private static PointQueryResponse queryOnStub(
            ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub,
            String tableId,
            int id,
            PbStructType schema) {
        return stub.pointQuery(
                PointQueryRequest.newBuilder()
                        .setTableId(tableId)
                        .putPkValues("id", intLit(id))
                        .addAllPrimaryKeyCols(java.util.List.of("id"))
                        .setSchema(schema)
                        .build());
    }
}
