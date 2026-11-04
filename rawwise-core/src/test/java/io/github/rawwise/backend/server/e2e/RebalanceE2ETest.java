package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.rebalance.ShardRebalancer;
import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingEpoch;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.backend.server.migration.ShardMigrationCoordinator;
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
import java.util.List;
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
 * Phase 3.5-T4: Shard selfre-balance E2E test.
 *
 * <p>validationnodeadd/leavewhen shard migratecorelink(§3.5.1, §3.5.2):
 *
 * <ol>
 *   <li>S1: initialstartstate - allhas shard in Node A, write datacanlookup
 *   <li>S2: nodeadd - Node B register, rebalanceOnJoin detectnotbalancedandmigrate shard
 *   <li>S3: migrateaftervalidation - shard datainnew Node B canlookup, old Node A noresidual,
 *       pathbytableupdatenew
 *   <li>S4: migrateafternewwrite - tonew owner writenew key canlookup
 *   <li>S5: nodeleave - rebalanceOnLeave will Node B shard migratereturn Node A, migrateafter Node
 *       A canlookup
 * </ol>
 *
 * <p>follow RawWise E2E teststandard: true ExecutorPointQueryServiceImpl + true LMDB + InProcess
 * gRPC.
 */
@DisplayName("Phase 3.5-T4: Rebalance E2E - Join/Leave shard migration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RebalanceE2ETest {

    private static final String EXEC_A = "rebal-exec-A";
    private static final String EXEC_B = "rebal-exec-B";
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

    private static ShardMigrationCoordinator migrationCoordinator;
    private static ShardRebalancer rebalancer;

    private static String previousWarehousePath;
    private static String rebalanceTableId;

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

        warehouseA = Files.createDirectories(tempDir.resolve("rebal-warehouse-A"));
        warehouseB = Files.createDirectories(tempDir.resolve("rebal-warehouse-B"));

        routingTable = new InMemoryShardRoutingTable();
        routingCoordinator = new InMemoryRoutingCoordinator(routingTable);

        previousWarehousePath = System.getProperty("rawwise.warehouse.path");
        System.setProperty("rawwise.warehouse.path", warehouseA.toString());
        nodeAService = new ExecutorPointQueryServiceImpl(buildNodeConfig(warehouseA));
        String nodeAName = "rebal-node-a-" + UUID.randomUUID();
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
        String nodeBName = "rebal-node-b-" + UUID.randomUUID();
        nodeBServer =
                InProcessServerBuilder.forName(nodeBName)
                        .directExecutor()
                        .addService(nodeBService)
                        .build()
                        .start();
        nodeBChannel = InProcessChannelBuilder.forName(nodeBName).directExecutor().build();
        nodeBStub = ExecutorPointQueryServiceGrpc.newBlockingStub(nodeBChannel);

        Map<String, ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs =
                new HashMap<>();
        stubs.put(EXEC_A, nodeAStub);
        stubs.put(EXEC_B, nodeBStub);
        migrationCoordinator =
                new ShardMigrationCoordinator(stubs, routingCoordinator, routingTable);
        rebalancer = new ShardRebalancer(routingCoordinator, routingTable, migrationCoordinator);

        // Register executors
        NodeSlot slotA = new NodeSlot(EXEC_A, SHARD_0);
        NodeSlot slotB = new NodeSlot(EXEC_B, SHARD_0);
        routingTable.registerExecutor(EXEC_A, slotA);
        routingTable.registerExecutor(EXEC_B, slotB);
        routingTable.markExecutorReady(EXEC_A);
        routingTable.markExecutorReady(EXEC_B);

        // Initial: shard-0 -> exec-A (single-node, imbalanced)
        ShardAssignment initial =
                new ShardAssignment(new ShardId(SHARD_0), slotA, EXEC_A, RoutingEpoch.initial());
        routingTable.assignShard(initial);

        rebalanceTableId = "default.rebal_test";
    }

    @AfterAll
    static void tearDownAll() {
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
    @DisplayName("S1: Initial - write 10 records to Node A (single-node, imbalanced)")
    void testInitialWrite() {
        PbStructType schema = intIdSchema();
        for (int id = 1; id <= 10; id++) {
            PointInsertResponse resp = insertToStub(nodeAStub, rebalanceTableId, id, schema);
            assertThat(resp.getSuccess()).as("insert id=%d to node A should succeed", id).isTrue();
        }
        for (int id = 1; id <= 10; id++) {
            assertThat(queryOnStub(nodeAStub, rebalanceTableId, id, schema).getFound())
                    .as("id=%d should be queryable on node A")
                    .isTrue();
        }
    }

    @Test
    @Order(2)
    @DisplayName("S2: Join - rebalanceOnJoin detects imbalance, migrates shard-0 from A to B")
    void testRebalanceOnJoin() {
        ShardRebalancer.RebalanceResult result =
                rebalancer.rebalanceOnJoin(EXEC_B, rebalanceTableId);
        assertThat(result.success())
                .as("rebalanceOnJoin should succeed. Message: %s", result.message())
                .isTrue();
        assertThat(result.migratedCount())
                .as("should migrate at least 1 shard")
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    @Order(3)
    @DisplayName("S3: Post-join - shard-0 data queryable on Node B, routing table updated")
    void testPostJoinDataOnNewNode() {
        PbStructType schema = intIdSchema();
        // shard-0 should now be owned by B
        ShardLookupResult owner = routingCoordinator.lookupOwner(new ShardId(SHARD_0));
        assertThat(owner.assignment().executorId())
                .as("shard-0 owner should be exec-B after rebalance")
                .isEqualTo(EXEC_B);
        // Data should be queryable on B
        for (int id = 1; id <= 10; id++) {
            assertThat(queryOnStub(nodeBStub, rebalanceTableId, id, schema).getFound())
                    .as("id=%d should be queryable on node B after rebalance", id)
                    .isTrue();
        }
    }

    @Test
    @Order(4)
    @DisplayName("S4: Post-join new write - write to new owner B succeeds")
    void testPostJoinNewWrite() {
        PbStructType schema = intIdSchema();
        PointInsertResponse resp = insertToStub(nodeBStub, rebalanceTableId, 100, schema);
        assertThat(resp.getSuccess()).as("new write to node B should succeed").isTrue();
        assertThat(queryOnStub(nodeBStub, rebalanceTableId, 100, schema).getFound())
                .as("new id=100 should be queryable on B")
                .isTrue();
    }

    @Test
    @Order(5)
    @DisplayName("S5: Leave - rebalanceOnLeave migrates shard-0 from B back to A")
    void testRebalanceOnLeave() {
        ShardRebalancer.RebalanceResult result =
                rebalancer.rebalanceOnLeave(EXEC_B, rebalanceTableId);
        assertThat(result.success())
                .as("rebalanceOnLeave should succeed. Message: %s", result.message())
                .isTrue();
        assertThat(result.migratedCount())
                .as("should migrate at least 1 shard")
                .isGreaterThanOrEqualTo(1);
        // shard-0 should now be back on A
        ShardLookupResult owner = routingCoordinator.lookupOwner(new ShardId(SHARD_0));
        assertThat(owner.assignment().executorId())
                .as("shard-0 owner should be exec-A after leave rebalance")
                .isEqualTo(EXEC_A);
        // Data queryable on A
        PbStructType schema = intIdSchema();
        for (int id = 1; id <= 10; id++) {
            assertThat(queryOnStub(nodeAStub, rebalanceTableId, id, schema).getFound())
                    .as("id=%d should be queryable on node A after leave rebalance", id)
                    .isTrue();
        }
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
                        .addAllPrimaryKeyCols(List.of("id"))
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
                        .addAllPrimaryKeyCols(List.of("id"))
                        .setSchema(schema)
                        .build());
    }
}
