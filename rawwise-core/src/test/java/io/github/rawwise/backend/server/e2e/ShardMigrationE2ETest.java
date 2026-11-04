package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingEpoch;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.backend.server.migration.ShardMigrationCoordinator;
import io.github.rawwise.common.hash.MurmurHash3;
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
import io.github.rawwise.server.KeyBatch;
import io.github.rawwise.server.PointInsertRequest;
import io.github.rawwise.server.PointInsertResponse;
import io.github.rawwise.server.PointQueryRequest;
import io.github.rawwise.server.PointQueryResponse;
import io.github.rawwise.server.ScanKeysRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * Phase 3.9-T5: datashardwriteandcoldstartmigrate E2E test.
 *
 * <p>validationcoldstartmigratecorelink(singlenode -> multinode):
 *
 * <ol>
 *   <li>S1: premigratestate — tosourcenodewrite data, scanKeys by ModHash shardscancorrect
 *   <li>S2: coldstartmigrate — ShardMigrationCoordinator will shard 0
 *       fromsourcenodemigratetogoalnode
 *   <li>S3: migrateaftervalidation — shard-0 dataingoalnodecanlookup, sourcenodenoresidual, shard-1
 *       datanot
 *   <li>S4: pathbytableupdatenew — shard 0 owner change, epoch increment
 *   <li>S5: migrateafternewwrite — tonew owner writenew key canlookup
 *   <li>S6: idempotentetc.againmigrate — sourcenodealreadyno shard-0 data, migratedCount=0
 * </ol>
 *
 * <p>follow RawWise E2E teststandard:
 *
 * <ul>
 *   <li>makeusetrue {@link ExecutorPointQueryServiceImpl}(production entry), non- mock
 *   <li>makeusetrue LMDB store(notsame warehouse path isolated)
 *   <li>makeusetrue {@link ShardMigrationCoordinator} + {@link InMemoryRoutingCoordinator}
 *   <li>makeuse InProcess gRPC(nonetworkoverhead, but usestrue gRPC transferlayer +
 *       server-streaming)
 *   <li>scanKeys/MigrateBatch RPC usestrue gRPC path
 * </ul>
 *
 * <p>shard ownership determination: notprecompute id->shard map(KeyCodec encode bytes andstring id
 * ModHash cancannotsame), andisuse scanKeys actual result driven assertion. migrateafterviapass
 * pointQuery in A/B node found/not-found reversepush id shard ownership.
 */
@DisplayName("Phase 3.9-T5: Shard Migration E2E — Cold-start single-node -> multi-node")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ShardMigrationE2ETest {

    private static final String EXEC_A = "exec-A";
    private static final String EXEC_B = "exec-B";
    private static final int SHARD_COUNT = 2;
    private static final int SHARD_0 = 0;
    private static final int SHARD_1 = 1;
    private static final int HASH_SEED = 42;
    private static final int TOTAL_KEYS = 20;

    @TempDir static Path tempDir;

    private static Path warehouseA;
    private static Path warehouseB;

    private static InMemoryShardRoutingTable routingTable;
    private static RoutingCoordinator routingCoordinator;
    private static ShardMigrationCoordinator migrationCoordinator;

    private static ExecutorPointQueryServiceImpl nodeAService;
    private static Server nodeAServer;
    private static ManagedChannel nodeAChannel;
    private static ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub nodeAStub;

    private static ExecutorPointQueryServiceImpl nodeBService;
    private static Server nodeBServer;
    private static ManagedChannel nodeBChannel;
    private static ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub nodeBStub;

    private static String previousWarehousePath;

    // Cross-test state (populated by S1, consumed by S2-S5)
    private static String migrationTableId;
    private static int preShard0KeyCount;
    private static int preShard1KeyCount;

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

        previousWarehousePath = System.getProperty("rawwise.warehouse.path");
        System.setProperty("rawwise.warehouse.path", warehouseA.toString());
        nodeAService = new ExecutorPointQueryServiceImpl(buildNodeConfig(warehouseA));
        String nodeAName = "mig-node-a-" + UUID.randomUUID();
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
        String nodeBName = "mig-node-b-" + UUID.randomUUID();
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

        // Register executors first (assignShard requires executors to be registered).
        // Both executors register on shard 0 slot — the shardId in NodeSlot is the executor's
        // physical identity, not a constraint on which shards it can own.
        NodeSlot slotA = new NodeSlot(EXEC_A, SHARD_0);
        NodeSlot slotB = new NodeSlot(EXEC_B, SHARD_0);
        routingTable.registerExecutor(EXEC_A, slotA);
        routingTable.registerExecutor(EXEC_B, slotB);
        routingTable.markExecutorReady(EXEC_A);
        routingTable.markExecutorReady(EXEC_B);

        // Initial state: all shards -> exec-A (single-node pre-migration)
        assignShard(SHARD_0, EXEC_A, slotA, RoutingEpoch.initial());
        assignShard(SHARD_1, EXEC_A, slotA, RoutingEpoch.initial());
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
    @DisplayName("S1: Pre-migration — write keys to source node A, scanKeys shards correctly")
    void testPreMigrationWriteAndScan() {
        migrationTableId = uniqTable("mig");
        PbStructType schema = intSchema();

        // Write TOTAL_KEYS records to node A (single-node pre-migration state)
        for (int id = 0; id < TOTAL_KEYS; id++) {
            PointInsertResponse resp = stubInsert(nodeAStub, migrationTableId, id, schema);
            assertThat(resp.getSuccess())
                    .as("pointInsert id=%d to node A should succeed", id)
                    .isTrue();
        }

        // Verify all queryable on A
        for (int id = 0; id < TOTAL_KEYS; id++) {
            assertThat(stubQuery(nodeAStub, migrationTableId, id, schema).getFound())
                    .as("id=%d should be queryable on node A pre-migration", id)
                    .isTrue();
        }

        // Scan ALL keys (totalShardCount=1 disables shard filtering)
        List<byte[]> allKeys = scanKeys(nodeAStub, migrationTableId, 0, 1);
        assertThat(allKeys)
                .as("scanKeys(totalShardCount=1) should return all %d keys", TOTAL_KEYS)
                .hasSize(TOTAL_KEYS);

        // Scan shard 0 keys
        List<byte[]> shard0Keys = scanKeys(nodeAStub, migrationTableId, SHARD_0, SHARD_COUNT);
        // Scan shard 1 keys
        List<byte[]> shard1Keys = scanKeys(nodeAStub, migrationTableId, SHARD_1, SHARD_COUNT);

        // Verify shard partition is correct: ModHash(keyBytes) % SHARD_COUNT matches shardId
        for (byte[] key : shard0Keys) {
            assertThat(expectedShard(key, SHARD_COUNT))
                    .as("key in shard0 scan must have ModHash %% %d == 0", SHARD_COUNT)
                    .isEqualTo(SHARD_0);
        }
        for (byte[] key : shard1Keys) {
            assertThat(expectedShard(key, SHARD_COUNT))
                    .as("key in shard1 scan must have ModHash %% %d == 1", SHARD_COUNT)
                    .isEqualTo(SHARD_1);
        }

        // Verify no overlap and full coverage
        assertThat(shard0Keys.size() + shard1Keys.size())
                .as("shard0 + shard1 keys must cover all keys")
                .isEqualTo(TOTAL_KEYS);
        Set<ByteString> shard0Set = new HashSet<>(toByteStringList(shard0Keys));
        Set<ByteString> shard1Set = new HashSet<>(toByteStringList(shard1Keys));
        shard0Set.retainAll(shard1Set);
        assertThat(shard0Set).as("shard0 and shard1 must not overlap").isEmpty();

        preShard0KeyCount = shard0Keys.size();
        preShard1KeyCount = shard1Keys.size();

        assertThat(preShard0KeyCount)
                .as("should have at least 1 shard-0 key for migration")
                .isGreaterThan(0);
        assertThat(preShard1KeyCount)
                .as("should have at least 1 shard-1 key to remain on source")
                .isGreaterThan(0);
    }

    @Test
    @Order(2)
    @DisplayName(
            "S2+S3: Cold-start migration — shard 0 moves from A to B, no residue, shard-1 untouched")
    void testColdStartMigrationShard0() {
        assertThat(migrationTableId).as("S1 must run first").isNotNull();
        PbStructType schema = intSchema();

        long oldEpoch =
                routingCoordinator.lookupOwner(new ShardId(SHARD_0)).assignment().epoch().value();

        ShardMigrationCoordinator.MigrationResult result =
                migrationCoordinator.migrateShard(
                        migrationTableId, SHARD_0, EXEC_A, EXEC_B, SHARD_COUNT);

        assertThat(result.success()).as("migration should succeed").isTrue();
        assertThat(result.migratedCount())
                .as("migratedCount should equal pre-migration shard-0 key count")
                .isEqualTo(preShard0KeyCount);
        assertThat(result.failedCount()).as("no failed migrations expected").isZero();

        // Detect shard ownership per id via pointQuery on A vs B
        List<Integer> postShard0Ids = new ArrayList<>();
        List<Integer> postShard1Ids = new ArrayList<>();
        for (int id = 0; id < TOTAL_KEYS; id++) {
            boolean onB = stubQuery(nodeBStub, migrationTableId, id, schema).getFound();
            boolean onA = stubQuery(nodeAStub, migrationTableId, id, schema).getFound();
            // Each id must be on exactly one node (no data loss, no duplication)
            assertThat(onA ^ onB)
                    .as("id=%d must be on exactly one node (A=%s, B=%s)", id, onA, onB)
                    .isTrue();
            if (onB) {
                postShard0Ids.add(id);
            } else {
                postShard1Ids.add(id);
            }
        }

        assertThat(postShard0Ids)
                .as("shard-0 ids (on B) count must match pre-migration scan")
                .hasSize(preShard0KeyCount);
        assertThat(postShard1Ids)
                .as("shard-1 ids (on A) count must match pre-migration scan")
                .hasSize(preShard1KeyCount);

        // scanKeys(shardId=0) on A should now be empty (no residue)
        List<byte[]> postShard0OnA = scanKeys(nodeAStub, migrationTableId, SHARD_0, SHARD_COUNT);
        assertThat(postShard0OnA)
                .as("node A should have no shard-0 keys after migration (no residue)")
                .isEmpty();

        // scanKeys(shardId=0) on B should have all migrated keys
        List<byte[]> postShard0OnB = scanKeys(nodeBStub, migrationTableId, SHARD_0, SHARD_COUNT);
        assertThat(postShard0OnB)
                .as("node B should have all shard-0 keys after migration")
                .hasSize(preShard0KeyCount);

        // Verify ModHash consistency: all keys on B scan(shardId=0) hash to shard 0
        for (byte[] key : postShard0OnB) {
            assertThat(expectedShard(key, SHARD_COUNT))
                    .as("migrated key on B must hash to shard 0")
                    .isEqualTo(SHARD_0);
        }

        // scanKeys(shardId=1) on A should still have all shard-1 keys (untouched)
        List<byte[]> postShard1OnA = scanKeys(nodeAStub, migrationTableId, SHARD_1, SHARD_COUNT);
        assertThat(postShard1OnA)
                .as("node A should still have all shard-1 keys (untouched by migration)")
                .hasSize(preShard1KeyCount);

        // Routing table: shard 0 owner = exec-B, epoch > old
        ShardLookupResult after = routingCoordinator.lookupOwner(new ShardId(SHARD_0));
        assertThat(after.assigned()).isTrue();
        assertThat(after.assignment().executorId())
                .as("shard 0 owner should be exec-B after migration")
                .isEqualTo(EXEC_B);
        assertThat(after.assignment().epoch().value())
                .as("epoch should increment after migration")
                .isGreaterThan(oldEpoch);
    }

    @Test
    @Order(3)
    @DisplayName("S4: Post-migration write to migrated shard — new key on new owner is queryable")
    void testPostMigrationWriteToMigratedShard() {
        assertThat(migrationTableId).as("S1 must run first").isNotNull();
        PbStructType schema = intSchema();

        // Write a new id to node B (new owner of shard 0).
        // The BE Node accepts any write regardless of hash; routing decision is made
        // by the caller (AxiomServiceImpl.routeIngestTarget in production).
        int newId = 50_000;
        assertThat(stubQuery(nodeAStub, migrationTableId, newId, schema).getFound())
                .as("new id should not pre-exist on A")
                .isFalse();
        assertThat(stubQuery(nodeBStub, migrationTableId, newId, schema).getFound())
                .as("new id should not pre-exist on B")
                .isFalse();

        PointInsertResponse resp = stubInsert(nodeBStub, migrationTableId, newId, schema);
        assertThat(resp.getSuccess()).as("post-migration insert to node B should succeed").isTrue();

        // Verify queryable on B (new owner)
        assertThat(stubQuery(nodeBStub, migrationTableId, newId, schema).getFound())
                .as("new key should be queryable on node B (new owner)")
                .isTrue();

        // Verify NOT on A (cross-node isolation)
        assertThat(stubQuery(nodeAStub, migrationTableId, newId, schema).getFound())
                .as("new key should NOT be on node A (cross-node isolation)")
                .isFalse();
    }

    @Test
    @Order(4)
    @DisplayName("S5: Idempotent re-migration — source has no shard-0 keys, migratedCount=0")
    void testIdempotentReMigration() {
        assertThat(migrationTableId).as("S1 must run first").isNotNull();

        ShardMigrationCoordinator.MigrationResult result =
                migrationCoordinator.migrateShard(
                        migrationTableId, SHARD_0, EXEC_A, EXEC_B, SHARD_COUNT);

        assertThat(result.success()).as("re-migration should succeed (idempotent)").isTrue();
        assertThat(result.migratedCount())
                .as("re-migration should migrate 0 keys (source already empty)")
                .isZero();
    }

    // ---- Helpers ----

    private static void assignShard(
            int shardId, String executorId, NodeSlot slot, RoutingEpoch epoch) {
        ShardId sid = new ShardId(shardId);
        ShardAssignment assignment = new ShardAssignment(sid, slot, executorId, epoch, false);
        routingCoordinator.assignShard(assignment);
    }

    private static int expectedShard(byte[] keyBytes, int shardCount) {
        int hash = MurmurHash3.hash32(keyBytes, 0, keyBytes.length, HASH_SEED);
        return Math.floorMod(hash, shardCount);
    }

    private static List<byte[]> scanKeys(
            ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub,
            String tableId,
            int shardId,
            int totalShardCount) {
        ScanKeysRequest request =
                ScanKeysRequest.newBuilder()
                        .setTableId(tableId)
                        .setShardId(shardId)
                        .setBatchSize(100)
                        .setTotalShardCount(totalShardCount)
                        .build();

        java.util.Iterator<KeyBatch> iter = stub.scanKeys(request);
        List<byte[]> allKeys = new ArrayList<>();
        while (iter.hasNext()) {
            KeyBatch batch = iter.next();
            for (ByteString key : batch.getKeysList()) {
                allKeys.add(key.toByteArray());
            }
        }
        return allKeys;
    }

    private static List<ByteString> toByteStringList(List<byte[]> bytes) {
        List<ByteString> result = new ArrayList<>();
        for (byte[] b : bytes) {
            result.add(ByteString.copyFrom(b));
        }
        return result;
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
