package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.replication.CentroidCollector;
import io.github.rawwise.backend.replication.InMemoryCentroidSink;
import io.github.rawwise.backend.server.service.ClusterManagementServiceImpl;
import io.github.rawwise.engine.vector.ShardCentroids;
import io.github.rawwise.server.CentroidReportRequest;
import io.github.rawwise.server.CentroidReportResponse;
import io.github.rawwise.server.ClusterManagementServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Phase 3.8-T5: centroid self-collection E2E test.
 *
 * <p>validationcentroid collection corelink(§3.8.2, §3.8.3):
 *
 * <ol>
 *   <li>S1: manual collectAndReport - CentroidCollector abovereport -> BE Server receive ->
 *       CentroidSink persist
 *   <li>S2: vectorCount=0 rejected - emptycentroidrejected
 *   <li>S3: CentroidSink notconfig rejected - no sink whenreturn rejected
 *   <li>S4: timerabovereport - shortperiod CentroidCollector selfmulticallsabovereport
 * </ol>
 *
 * <p>makeuse InProcess gRPC + InMemoryCentroidSink, notdepend onouterpart Metastore.
 */
@DisplayName("Phase 3.8-T5: Centroid Collection E2E - Report/Persist/Query")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CentroidCollectionE2ETest {

    private static Server server;
    private static ManagedChannel channel;
    private static ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub stub;
    private static ClusterManagementServiceImpl service;
    private static InMemoryCentroidSink sink;

    @BeforeAll
    static void setUpAll() throws Exception {
        sink = new InMemoryCentroidSink();
        service = new ClusterManagementServiceImpl(null, null, "FIFO", null);
        service.setCentroidSink(sink);
        String name = "centroid-server-" + UUID.randomUUID();
        server =
                InProcessServerBuilder.forName(name)
                        .directExecutor()
                        .addService(service)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = ClusterManagementServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDownAll() {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
    }

    @Test
    @Order(1)
    @DisplayName("S1: Manual collectAndReport - centroid persisted to sink")
    void testManualReport() {
        // First test direct gRPC call to isolate issues
        CentroidReportRequest directReq =
                CentroidReportRequest.newBuilder()
                        .setShardId(0)
                        .addCentroid(0.1f)
                        .addCentroid(0.2f)
                        .addCentroid(0.3f)
                        .setVectorCount(100)
                        .setTimestamp(System.currentTimeMillis())
                        .setExecutorId("exec-A")
                        .build();
        CentroidReportResponse directResp = stub.reportCentroid(directReq);
        assertThat(directResp.getAccepted())
                .as("direct gRPC report should be accepted. Error: %s", directResp.getError())
                .isTrue();
        Optional<ShardCentroids> directResult = sink.lookup("0");
        assertThat(directResult).as("direct centroid should be persisted").isPresent();

        // Now test via CentroidCollector
        float[] centroid = {0.1f, 0.2f, 0.3f};
        CentroidCollector.CentroidSource source =
                () -> new CentroidCollector.CentroidData(5, centroid, 100L);
        CentroidCollector collector = new CentroidCollector(source, stub, "exec-A");
        collector.collectAndReport();
        Optional<ShardCentroids> result = sink.lookup("5");
        assertThat(result).as("centroid should be persisted in sink via collector").isPresent();
        assertThat(result.get().vectorCount()).isEqualTo(100L);
    }

    @Test
    @Order(2)
    @DisplayName("S2: vectorCount=0 rejected")
    void testZeroVectorCountRejected() {
        CentroidReportRequest req =
                CentroidReportRequest.newBuilder()
                        .setShardId(1)
                        .addCentroid(0.5f)
                        .setVectorCount(0)
                        .setTimestamp(System.currentTimeMillis())
                        .setExecutorId("exec-B")
                        .build();
        CentroidReportResponse resp = stub.reportCentroid(req);
        assertThat(resp.getAccepted()).as("zero vector count should be rejected").isFalse();
        assertThat(resp.getError())
                .as("error message should mention vector_count")
                .contains("vector_count");
    }

    @Test
    @Order(3)
    @DisplayName("S3: CentroidSink not configured - rejected")
    void testSinkNotConfigured() throws Exception {
        ClusterManagementServiceImpl noSinkService =
                new ClusterManagementServiceImpl(null, null, "FIFO", null);
        String name = "no-sink-server-" + UUID.randomUUID();
        Server noSinkServer =
                InProcessServerBuilder.forName(name)
                        .directExecutor()
                        .addService(noSinkService)
                        .build()
                        .start();
        ManagedChannel noSinkChannel =
                InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub noSinkStub =
                    ClusterManagementServiceGrpc.newBlockingStub(noSinkChannel);
            CentroidReportRequest req =
                    CentroidReportRequest.newBuilder()
                            .setShardId(2)
                            .addCentroid(0.5f)
                            .setVectorCount(50)
                            .setTimestamp(System.currentTimeMillis())
                            .setExecutorId("exec-C")
                            .build();
            CentroidReportResponse resp = noSinkStub.reportCentroid(req);
            assertThat(resp.getAccepted())
                    .as("should be rejected when sink not configured")
                    .isFalse();
            assertThat(resp.getError()).contains("not configured");
        } finally {
            noSinkChannel.shutdownNow();
            noSinkServer.shutdownNow();
        }
    }

    @Test
    @Order(4)
    @DisplayName("S4: Scheduled collector - multiple reports over time")
    void testScheduledCollector() throws Exception {
        float[] centroid = {0.5f, 0.5f};
        CentroidCollector.CentroidSource source =
                () -> new CentroidCollector.CentroidData(3, centroid, 200L);
        CentroidCollector collector = new CentroidCollector(source, stub, "exec-D", 1L, 0L);
        collector.start();
        Thread.sleep(2500);
        collector.stop();
        Optional<ShardCentroids> result = sink.lookup("3");
        assertThat(result).as("scheduled collector should have reported to sink").isPresent();
        assertThat(result.get().vectorCount()).isEqualTo(200L);
    }

    @Test
    @Order(5)
    @DisplayName("S5: listAll returns all reported centroids")
    void testListAll() {
        sink.save(
                new ShardCentroids(
                        "99", 1, new float[][] {{0.9f}}, 10L, 1, System.currentTimeMillis()));
        List<ShardCentroids> all = List.copyOf(sink.listAll().values());
        assertThat(all).as("listAll should contain multiple shards").hasSizeGreaterThanOrEqualTo(2);
        assertThat(all.stream().map(ShardCentroids::shardId))
                .as("should contain shard 0 and 99")
                .contains("0", "99");
    }
}
