package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.backend.server.service.ClusterManagementServiceImpl;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.writer.WriterFactoryBootstrap;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.meta.PbDataType;
import io.github.rawwise.meta.PbLiteralValue;
import io.github.rawwise.meta.PbStructField;
import io.github.rawwise.meta.PbStructType;
import io.github.rawwise.meta.PrimitiveType;
import io.github.rawwise.server.ClusterManagementServiceGrpc;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.PointInsertRequest;
import io.github.rawwise.server.PointInsertResponse;
import io.github.rawwise.server.PointQueryRequest;
import io.github.rawwise.server.PointQueryResponse;
import io.github.rawwise.server.RegisterExecutorRequest;
import io.github.rawwise.server.RegisterExecutorResponse;
import io.github.rawwise.server.Status;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.charset.StandardCharsets;
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
 * Phase 3.1-T7: distributedcommunication E2E test — 1 BE Server + 2 BE Nodes.
 *
 * <p>validationmultinodecommunication corelink:
 *
 * <ol>
 *   <li>BE Node startafterviapass gRPC {@code RegisterExecutor} to BE Server register
 *   <li>BE Server RoutingCoordinator correctupdatenewshardpathbytable
 *   <li>FastPath pathby: baseat shardId lookup owner executor -> selectforshould gRPC channel ->
 *       pointInsert/pointQuery
 *   <li>crossshardstoreisolated: notsame BE Node hasindependentestablish LMDB store
 *   <li>Re-registration when epoch increment
 * </ol>
 *
 * <p>follow RawWise E2E teststandard:
 *
 * <ul>
 *   <li>makeusetrue {@link ExecutorPointQueryServiceImpl}(production entry), non- mock
 *   <li>makeusetrue {@link ClusterManagementServiceImpl} + {@link InMemoryRoutingCoordinator}
 *   <li>makeusetrue LMDB store(notsame warehouse path isolated)
 *   <li>makeuse InProcess gRPC(nonetworkoverhead, but usestrue gRPC transferlayer)
 * </ul>
 */
@DisplayName("Phase 3.1-T7: Distributed Communication E2E — 1 BE Server + 2 BE Nodes")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DistributedCommE2ETest {

    private static final String EXEC_A = "exec-A";
    private static final String EXEC_B = "exec-B";
    private static final String NODE_A = "node-A";
    private static final String NODE_B = "node-B";
    private static final int SHARD_0 = 0;
    private static final int SHARD_1 = 1;

    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir static Path tempDir;

    private static Path warehouseA;
    private static Path warehouseB;

    private static InMemoryShardRoutingTable routingTable;
    private static RoutingCoordinator routingCoordinator;
    private static Server beServer;
    private static ManagedChannel beServerChannel;
    private static ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub clusterStub;

    private static ExecutorPointQueryServiceImpl nodeAService;
    private static Server nodeAServer;
    private static ManagedChannel nodeAChannel;
    private static ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub nodeAStub;

    private static ExecutorPointQueryServiceImpl nodeBService;
    private static Server nodeBServer;
    private static ManagedChannel nodeBChannel;
    private static ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub nodeBStub;

    private static String previousWarehousePath;

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

        warehouseA = Files.createDirectories(tempDir.resolve("warehouse-A"));
        warehouseB = Files.createDirectories(tempDir.resolve("warehouse-B"));

        routingTable = new InMemoryShardRoutingTable();
        routingCoordinator = new InMemoryRoutingCoordinator(routingTable);
        ClusterManagementServiceImpl clusterService =
                new ClusterManagementServiceImpl(null, null, "FIFO", routingCoordinator);

        String beServerName = "be-server-" + UUID.randomUUID();
        beServer =
                InProcessServerBuilder.forName(beServerName)
                        .directExecutor()
                        .addService(clusterService)
                        .build()
                        .start();
        beServerChannel = InProcessChannelBuilder.forName(beServerName).directExecutor().build();
        clusterStub = ClusterManagementServiceGrpc.newBlockingStub(beServerChannel);

        previousWarehousePath = System.getProperty("rawwise.warehouse.path");
        System.setProperty("rawwise.warehouse.path", warehouseA.toString());
        nodeAService = new ExecutorPointQueryServiceImpl(buildNodeConfig(warehouseA));
        String nodeAName = "be-node-a-" + UUID.randomUUID();
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
        String nodeBName = "be-node-b-" + UUID.randomUUID();
        nodeBServer =
                InProcessServerBuilder.forName(nodeBName)
                        .directExecutor()
                        .addService(nodeBService)
                        .build()
                        .start();
        nodeBChannel = InProcessChannelBuilder.forName(nodeBName).directExecutor().build();
        nodeBStub = ExecutorPointQueryServiceGrpc.newBlockingStub(nodeBChannel);
    }

    @AfterAll
    static void tearDownAll() {
        if (nodeAChannel != null) nodeAChannel.shutdownNow();
        if (nodeBChannel != null) nodeBChannel.shutdownNow();
        if (beServerChannel != null) beServerChannel.shutdownNow();
        if (nodeAServer != null) nodeAServer.shutdownNow();
        if (nodeBServer != null) nodeBServer.shutdownNow();
        if (beServer != null) beServer.shutdownNow();

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
    @DisplayName(
            "S1: Both BE Nodes register via gRPC RegisterExecutor — routing table updated correctly")
    void testBothNodesRegister() {
        RegisterExecutorResponse respA = registerNode(EXEC_A, NODE_A, SHARD_0, 0);
        assertThat(respA.getStatus().getCode()).isEqualTo(Status.Code.OK);
        assertThat(respA.getEpoch()).isGreaterThan(0L);

        RegisterExecutorResponse respB = registerNode(EXEC_B, NODE_B, SHARD_1, 0);
        assertThat(respB.getStatus().getCode()).isEqualTo(Status.Code.OK);
        assertThat(respB.getEpoch()).isGreaterThan(0L);

        ShardLookupResult lookupA = routingCoordinator.lookupOwner(new ShardId(SHARD_0));
        assertThat(lookupA.assigned()).isTrue();
        assertThat(lookupA.assignment().executorId()).isEqualTo(EXEC_A);

        ShardLookupResult lookupB = routingCoordinator.lookupOwner(new ShardId(SHARD_1));
        assertThat(lookupB.assigned()).isTrue();
        assertThat(lookupB.assignment().executorId()).isEqualTo(EXEC_B);
    }

    @Test
    @Order(2)
    @DisplayName(
            "S2: FastPath routing — lookup shard owner -> select stub -> pointInsert + pointQuery")
    void testFastPathInsertQuerySameNode() throws Exception {
        String tableId = uniqTable("s2_fastpath");
        PbStructType schema = intSchema();
        int id = 100;

        ShardLookupResult owner = routingCoordinator.lookupOwner(new ShardId(SHARD_0));
        assertThat(owner.assigned()).isTrue();
        assertThat(owner.assignment().executorId()).isEqualTo(EXEC_A);

        ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub =
                selectStub(owner.assignment().executorId());

        PointInsertResponse insResp =
                stub.pointInsert(
                        PointInsertRequest.newBuilder()
                                .setTableId(tableId)
                                .putPkValues("id", intLit(id))
                                .putRowValues("id", intLit(id))
                                .addAllPrimaryKeyCols(List.of("id"))
                                .setSchema(schema)
                                .build());
        assertThat(insResp.getSuccess()).as("pointInsert via Node A should succeed").isTrue();

        PointQueryResponse queryResp =
                stub.pointQuery(
                        PointQueryRequest.newBuilder()
                                .setTableId(tableId)
                                .putPkValues("id", intLit(id))
                                .addAllPrimaryKeyCols(List.of("id"))
                                .setSchema(schema)
                                .build());
        assertThat(queryResp.getFound())
                .as("pointQuery via Node A should find the record")
                .isTrue();

        JsonNode json = JSON.readTree(queryResp.getData().toString(StandardCharsets.UTF_8));
        assertThat(json.get("id").asInt()).isEqualTo(id);
    }

    @Test
    @Order(3)
    @DisplayName("S3: Cross-shard storage isolation — data on Node A not visible from Node B")
    void testCrossShardIsolation() {
        String tableId = uniqTable("s3_isolation");
        PbStructType schema = intSchema();

        assertThat(stubInsert(nodeAStub, tableId, 200, schema).getSuccess()).isTrue();
        assertThat(stubInsert(nodeBStub, tableId, 300, schema).getSuccess()).isTrue();

        assertThat(stubQuery(nodeAStub, tableId, 200, schema).getFound()).isTrue();
        assertThat(stubQuery(nodeAStub, tableId, 300, schema).getFound())
                .as("id=300 written to Node B should NOT be visible from Node A")
                .isFalse();

        assertThat(stubQuery(nodeBStub, tableId, 300, schema).getFound()).isTrue();
        assertThat(stubQuery(nodeBStub, tableId, 200, schema).getFound())
                .as("id=200 written to Node A should NOT be visible from Node B")
                .isFalse();
    }

    @Test
    @Order(4)
    @DisplayName("S4: Re-registration advances epoch — new epoch > old epoch")
    void testReRegistrationAdvancesEpoch() {
        ShardLookupResult before = routingCoordinator.lookupOwner(new ShardId(SHARD_0));
        assertThat(before.assigned()).isTrue();
        long oldEpoch = before.assignment().epoch().value();

        RegisterExecutorResponse resp = registerNode(EXEC_A, NODE_A, SHARD_0, 0);
        assertThat(resp.getStatus().getCode()).isEqualTo(Status.Code.OK);
        assertThat(resp.getEpoch()).isGreaterThan(oldEpoch);

        ShardLookupResult after = routingCoordinator.lookupOwner(new ShardId(SHARD_0));
        assertThat(after.assigned()).isTrue();
        assertThat(after.assignment().epoch().value()).isGreaterThan(oldEpoch);
    }

    private RegisterExecutorResponse registerNode(
            String executorId, String nodeId, int shardId, int slotIndex) {
        return clusterStub.registerExecutor(
                RegisterExecutorRequest.newBuilder()
                        .setExecutorId(executorId)
                        .setNodeId(nodeId)
                        .setShardId(shardId)
                        .setSlotIndex(slotIndex)
                        .setHost("localhost")
                        .setPort(9091)
                        .build());
    }

    private ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub selectStub(
            String executorId) {
        if (EXEC_A.equals(executorId)) return nodeAStub;
        if (EXEC_B.equals(executorId)) return nodeBStub;
        throw new IllegalArgumentException("Unknown executor: " + executorId);
    }

    private static String uniqTable(String prefix) {
        return "default." + prefix + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static PbStructType intSchema() {
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

    private static PointInsertResponse stubInsert(
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

    private static PointQueryResponse stubQuery(
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

    private static RawWiseConfig buildNodeConfig(Path warehouse) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("rawwise.catalog.warehouse-path", warehouse.toString());
        cfg.put("rawwise.storage.lmdb.initial-map-size-mb", 16L);
        cfg.put("rawwise.storage.lmdb.max-map-size-mb", 256L);
        return RawWiseConfig.fromMap(cfg);
    }
}
