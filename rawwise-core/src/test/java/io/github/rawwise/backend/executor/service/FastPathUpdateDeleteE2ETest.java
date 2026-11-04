package io.github.rawwise.backend.executor.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.meta.PbLiteralValue;
import io.github.rawwise.meta.PbStructType;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.PointDeleteRequest;
import io.github.rawwise.server.PointDeleteResponse;
import io.github.rawwise.server.PointInsertRequest;
import io.github.rawwise.server.PointInsertResponse;
import io.github.rawwise.server.PointUpdateRequest;
import io.github.rawwise.server.PointUpdateResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("FastPath UPDATE/DELETE E2E — Spec 01 Phase 1")
class FastPathUpdateDeleteE2ETest {

    @TempDir static Path tempDir;

    private static String serverName;
    private static Server grpcServer;
    private static ManagedChannel channel;
    private static ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub;
    private static ExecutorPointQueryServiceImpl serviceImpl;

    @BeforeAll
    static void setUpAll() throws Exception {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception ignored) {
        }

        Map<String, Object> cfg = new HashMap<>();
        cfg.put("rawwise.catalog.warehouse-path", tempDir.toString());
        cfg.put("rawwise.storage.lmdb.initial-map-size-mb", 16L);
        cfg.put("rawwise.storage.lmdb.max-map-size-mb", 256L);
        RawWiseConfig config = RawWiseConfig.fromMap(cfg);

        serviceImpl = new ExecutorPointQueryServiceImpl(config);
        serverName = "fastpath-ud-e2e-" + UUID.randomUUID();
        grpcServer =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        stub = ExecutorPointQueryServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDownAll() {
        if (channel != null) channel.shutdownNow();
        if (grpcServer != null) grpcServer.shutdownNow();
    }

    @BeforeEach
    void resetTxnContext() {
        try {
            io.github.rawwise.engine.txn.state.TxnContextHolder.clear();
        } catch (Exception ignored) {
        }
    }

    @AfterEach
    void afterEach() {}

    private static String uniqTable(String prefix) {
        return "default." + prefix + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static PbLiteralValue intLit(int v) {
        return PbLiteralValue.newBuilder().setInt32Val(v).build();
    }

    private static PbLiteralValue strLit(String v) {
        return PbLiteralValue.newBuilder().setStringVal(v).build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private byte[] seedRecord(
            String tableId,
            List<String> pkCols,
            Map<String, Comparable> pkValues,
            byte[] valueBytes)
            throws Exception {
        Map<String, PbLiteralValue> pkMap = new HashMap<>();
        for (var e : pkValues.entrySet()) {
            Object v = e.getValue();
            if (v instanceof Integer i) pkMap.put(e.getKey(), intLit(i));
            else if (v instanceof Long l)
                pkMap.put(e.getKey(), PbLiteralValue.newBuilder().setInt64Val(l).build());
            else pkMap.put(e.getKey(), strLit(String.valueOf(v)));
        }
        PointUpdateRequest seedReq =
                PointUpdateRequest.newBuilder()
                        .setTableId(tableId)
                        .putAllPkValues(pkMap)
                        .addAllPrimaryKeyCols(pkCols)
                        .setData(ByteString.copyFrom(valueBytes))
                        .build();
        PointUpdateResponse seedResp = stub.pointUpdate(seedReq);
        assertThat(seedResp.getSuccess())
                .as("Seed write should succeed for tableId=%s, pk=%s", tableId, pkValues)
                .isTrue();

        TableStore ts =
                DynamicMmapStore.getInstance(tempDir.toString(), "spark")
                        .tableStore(tableId, pkCols);
        KeyCodec keyCodec = ts.getKeyCodec();
        byte[] encoded = keyCodec.encodePrimaryKey(pkValues);
        byte[] raw = ts.getRawValue(encoded);
        assertThat(raw).as("Seeded record should be present in LMDB").isNotNull();
        assertThat(NullableValueCodec.isTombstone(raw)).isFalse();
        return encoded;
    }

    @Nested
    @DisplayName("S1: Happy Path - PK etcvalue UPDATE/DELETE")
    class HappyPathTests {

        @Test
        @DisplayName("S1.1: pointUpdate datapath - already in key becover")
        void pointUpdateOverwritesExisting() throws Exception {
            String tableId = uniqTable("s1_upd");
            List<String> pkCols = List.of("id");
            Map<String, Comparable> pk = Map.of("id", 100);

            byte[] v1 = "value-v1".getBytes(StandardCharsets.UTF_8);
            byte[] v2 = "value-v2".getBytes(StandardCharsets.UTF_8);
            seedRecord(tableId, pkCols, pk, v1);

            PointUpdateRequest req =
                    PointUpdateRequest.newBuilder()
                            .setTableId(tableId)
                            .putAllPkValues(Map.of("id", intLit(100)))
                            .addAllPrimaryKeyCols(pkCols)
                            .setData(ByteString.copyFrom(v2))
                            .build();
            PointUpdateResponse resp = stub.pointUpdate(req);

            assertThat(resp.getSuccess()).isTrue();
            assertThat(resp.getError()).isEmpty();

            TableStore ts =
                    DynamicMmapStore.getInstance(tempDir.toString(), "spark")
                            .tableStore(tableId, pkCols);
            byte[] encoded = ts.getKeyCodec().encodePrimaryKey(pk);
            byte[] raw = ts.getRawValue(encoded);
            byte[] decoded = NullableValueCodec.decode(raw);
            assertThat(decoded).isEqualTo(v2);
        }

        @Test
        @DisplayName("S1.2: pointDelete deletealready in key - subsequentread null")
        void pointDeleteRemovesExisting() throws Exception {
            String tableId = uniqTable("s1_del");
            List<String> pkCols = List.of("id");
            Map<String, Comparable> pk = Map.of("id", 200);
            byte[] v = "to-be-deleted".getBytes(StandardCharsets.UTF_8);
            seedRecord(tableId, pkCols, pk, v);

            PointDeleteRequest req =
                    PointDeleteRequest.newBuilder()
                            .setTableId(tableId)
                            .putAllPkValues(Map.of("id", intLit(200)))
                            .addAllPrimaryKeyCols(pkCols)
                            .build();
            PointDeleteResponse resp = stub.pointDelete(req);

            assertThat(resp.getSuccess()).isTrue();
            assertThat(resp.getError()).isEmpty();

            TableStore ts =
                    DynamicMmapStore.getInstance(tempDir.toString(), "spark")
                            .tableStore(tableId, pkCols);
            byte[] encoded = ts.getKeyCodec().encodePrimaryKey(pk);
            byte[] raw = ts.getRawValue(encoded);
            assertThat(raw == null || NullableValueCodec.isTombstone(raw))
                    .as("Record should be deleted (null or tombstone), got raw=%s", raw)
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("S2: NotFound")
    class NotFoundTests {

        @Test
        @DisplayName("S2.1: pointDelete non-existent key returns success=false, error='not found'")
        void pointDeleteNotFound() {
            String tableId = uniqTable("s2_del");
            List<String> pkCols = List.of("id");

            PointDeleteRequest req =
                    PointDeleteRequest.newBuilder()
                            .setTableId(tableId)
                            .putAllPkValues(Map.of("id", intLit(99999)))
                            .addAllPrimaryKeyCols(pkCols)
                            .build();
            PointDeleteResponse resp = stub.pointDelete(req);

            assertThat(resp.getSuccess()).isFalse();
            assertThat(resp.getError()).contains("not found");
        }

        @Test
        @DisplayName("S2.2: pointUpdate missing pkValues data is 'missing pk values'")
        void pointUpdateMissingPk() {
            String tableId = uniqTable("s2_upd");
            PointUpdateRequest req = PointUpdateRequest.newBuilder().setTableId(tableId).build();
            PointUpdateResponse resp = stub.pointUpdate(req);

            assertThat(resp.getSuccess()).isFalse();
            assertThat(resp.getError()).contains("missing pk values");
        }
    }

    @Nested
    @DisplayName("S3: PK column (non- 'id')")
    class GeneralizedPkColumnTests {

        @Test
        @DisplayName("S3.1: PK column is 'cust_id' UPDATE/DELETE correctly encodes")
        void nonIdPkColumn() throws Exception {
            String tableId = uniqTable("s3_cust");
            List<String> pkCols = List.of("cust_id");
            Map<String, Comparable> pk = Map.of("cust_id", 555);

            byte[] v1 = "cust-v1".getBytes(StandardCharsets.UTF_8);
            byte[] v2 = "cust-v2".getBytes(StandardCharsets.UTF_8);
            seedRecord(tableId, pkCols, pk, v1);

            PointUpdateRequest upd =
                    PointUpdateRequest.newBuilder()
                            .setTableId(tableId)
                            .putAllPkValues(Map.of("cust_id", intLit(555)))
                            .addAllPrimaryKeyCols(pkCols)
                            .setData(ByteString.copyFrom(v2))
                            .build();
            assertThat(stub.pointUpdate(upd).getSuccess()).isTrue();

            TableStore ts =
                    DynamicMmapStore.getInstance(tempDir.toString(), "spark")
                            .tableStore(tableId, pkCols);
            byte[] encoded = ts.getKeyCodec().encodePrimaryKey(pk);
            byte[] raw = ts.getRawValue(encoded);
            assertThat(NullableValueCodec.decode(raw)).isEqualTo(v2);

            PointDeleteRequest del =
                    PointDeleteRequest.newBuilder()
                            .setTableId(tableId)
                            .putAllPkValues(Map.of("cust_id", intLit(555)))
                            .addAllPrimaryKeyCols(pkCols)
                            .build();
            assertThat(stub.pointDelete(del).getSuccess()).isTrue();
        }
    }

    @Nested
    @DisplayName("S4: composite primary key(column)")
    class CompositePkTests {

        @Test
        @DisplayName("S4.1: repeat PK (warehouse_id, district_id) correctly routes(TPC-C)")
        void compositePkUpdateDelete() throws Exception {
            String tableId = uniqTable("s4_district");
            List<String> pkCols = List.of("warehouse_id", "district_id");
            Map<String, Comparable> pk =
                    Map.of(
                            "warehouse_id", 1,
                            "district_id", 7);

            byte[] v1 = "district-v1".getBytes(StandardCharsets.UTF_8);
            byte[] v2 = "district-v2".getBytes(StandardCharsets.UTF_8);
            seedRecord(tableId, pkCols, pk, v1);

            PointUpdateRequest upd =
                    PointUpdateRequest.newBuilder()
                            .setTableId(tableId)
                            .putAllPkValues(
                                    Map.of(
                                            "warehouse_id", intLit(1),
                                            "district_id", intLit(7)))
                            .addAllPrimaryKeyCols(pkCols)
                            .setData(ByteString.copyFrom(v2))
                            .build();
            assertThat(stub.pointUpdate(upd).getSuccess()).isTrue();

            TableStore ts =
                    DynamicMmapStore.getInstance(tempDir.toString(), "spark")
                            .tableStore(tableId, pkCols);
            byte[] encoded = ts.getKeyCodec().encodePrimaryKey(pk);
            byte[] raw = ts.getRawValue(encoded);
            assertThat(NullableValueCodec.decode(raw)).isEqualTo(v2);

            PointDeleteRequest delMiss =
                    PointDeleteRequest.newBuilder()
                            .setTableId(tableId)
                            .putAllPkValues(
                                    Map.of(
                                            "warehouse_id", intLit(1),
                                            "district_id", intLit(999)))
                            .addAllPrimaryKeyCols(pkCols)
                            .build();
            PointDeleteResponse missResp = stub.pointDelete(delMiss);
            assertThat(missResp.getSuccess()).isFalse();
            assertThat(missResp.getError()).contains("not found");

            PointDeleteRequest delOk =
                    PointDeleteRequest.newBuilder()
                            .setTableId(tableId)
                            .putAllPkValues(
                                    Map.of(
                                            "warehouse_id", intLit(1),
                                            "district_id", intLit(7)))
                            .addAllPrimaryKeyCols(pkCols)
                            .build();
            assertThat(stub.pointDelete(delOk).getSuccess()).isTrue();
        }
    }

    @Nested
    @DisplayName("S5: writeafter readconsistency(same PK encodepath)")
    class WriteReadConsistencyTests {

        @Test
        @DisplayName(
                "S5.1: pointUpdate datapathafter writing, pointUpdate same PK go through (key encode)")
        void writeReadConsistencyViaUpdate() throws Exception {
            String tableId = uniqTable("s5_consistency");
            List<String> pkCols = List.of("id");
            Map<String, Comparable> pk = Map.of("id", 777);
            byte[] v1 = "first".getBytes(StandardCharsets.UTF_8);
            byte[] v2 = "second".getBytes(StandardCharsets.UTF_8);
            seedRecord(tableId, pkCols, pk, v1);

            PointUpdateRequest req =
                    PointUpdateRequest.newBuilder()
                            .setTableId(tableId)
                            .putAllPkValues(Map.of("id", intLit(777)))
                            .addAllPrimaryKeyCols(pkCols)
                            .setData(ByteString.copyFrom(v2))
                            .build();
            PointUpdateResponse resp = stub.pointUpdate(req);
            assertThat(resp.getSuccess()).isTrue();

            TableStore ts =
                    DynamicMmapStore.getInstance(tempDir.toString(), "spark")
                            .tableStore(tableId, pkCols);
            byte[] encoded = ts.getKeyCodec().encodePrimaryKey(pk);
            byte[] raw = ts.getRawValue(encoded);
            assertThat(raw).isNotNull();
            assertThat(NullableValueCodec.decode(raw)).isEqualTo(v2);

            PointDeleteRequest del =
                    PointDeleteRequest.newBuilder()
                            .setTableId(tableId)
                            .putAllPkValues(Map.of("id", intLit(777)))
                            .addAllPrimaryKeyCols(pkCols)
                            .build();
            assertThat(stub.pointDelete(del).getSuccess()).isTrue();

            PointDeleteResponse delAgain = stub.pointDelete(del);
            assertThat(delAgain.getSuccess()).isFalse();
            assertThat(delAgain.getError()).contains("not found");
        }
    }

    @Nested
    @DisplayName("S6: concurrent write commitTs uniqueness")
    class ConcurrentCommitTsTests {

        @Test
        @DisplayName("S6.1: concurrent pointInsert duplicate commitTs")
        void concurrentInsertsGetUniqueMonotonicCommitTs() throws Exception {
            String tableId = uniqTable("s6_ts");
            List<String> pkCols = List.of("id");
            int numWrites = 20;
            int numThreads = 4;

            io.github.rawwise.meta.PbField pbField =
                    io.github.rawwise.meta.PbField.newBuilder()
                            .setName("id")
                            .setType("INTEGER")
                            .build();
            PbStructType schema = PbStructType.newBuilder().addFields(pbField).build();

            CountDownLatch latch = new CountDownLatch(numWrites);
            var executor = Executors.newFixedThreadPool(numThreads);

            for (int i = 0; i < numWrites; i++) {
                final int id = i + 1;
                executor.submit(
                        () -> {
                            try {
                                PointInsertRequest req =
                                        PointInsertRequest.newBuilder()
                                                .setTableId(tableId)
                                                .putPkValues("id", intLit(id))
                                                .putRowValues("id", intLit(id))
                                                .addAllPrimaryKeyCols(pkCols)
                                                .setSchema(schema)
                                                .build();
                                PointInsertResponse resp = stub.pointInsert(req);
                                assertThat(resp.getSuccess())
                                        .as("Insert id=%d should succeed", id)
                                        .isTrue();
                            } finally {
                                latch.countDown();
                            }
                        });
            }
            latch.await();
            executor.shutdown();

            TableStore ts =
                    DynamicMmapStore.getInstance(tempDir.toString(), "spark")
                            .tableStore(tableId, pkCols);
            List<Long> commitTsList = new ArrayList<>();
            for (int i = 0; i < numWrites; i++) {
                byte[] encoded =
                        ts.getKeyCodec().encodePrimaryKey(Map.of("id", (Comparable) (i + 1)));
                byte[] raw = ts.getRawValue(encoded);
                assertThat(raw).as("Record id=%d should exist in LMDB", i + 1).isNotNull();
                byte[] recordBytes = NullableValueCodec.decode(raw);
                long commitTs = DefaultRecordCodec.peekCommitTimestamp(recordBytes);
                assertThat(commitTs)
                        .as("commitTs for id=%d should be > 0 (not Long.MIN_VALUE)", i + 1)
                        .isGreaterThan(0L);
                commitTsList.add(commitTs);
            }

            Set<Long> uniqueTs = Set.copyOf(commitTsList);
            assertThat(uniqueTs.size()).as("All commitTs should be unique").isEqualTo(numWrites);

            List<Long> sorted = commitTsList.stream().sorted().toList();
            assertThat(sorted.getLast())
                    .as("Max commitTs should be >> min")
                    .isGreaterThan(sorted.getFirst());
        }
    }
}
