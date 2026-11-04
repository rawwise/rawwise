package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.observability.ClusterTopologyService;
import io.github.rawwise.backend.observability.ShardMetricsRegistry;
import io.github.rawwise.backend.observability.TraceIdPropagator;
import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingEpoch;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.server.ClusterManagementServiceGrpc;
import io.github.rawwise.server.HeartbeatRequest;
import io.github.rawwise.server.HeartbeatResponse;
import io.github.rawwise.server.Status;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Phase 3.11-T4: distributedcanobservation E2E test.
 *
 * <p>validation trace ID propagates + per-shard metrics + clustertopology API(§3.11):
 *
 * <ol>
 *   <li>S1: TraceId generatesuccess + setup + acquire + clear
 *   <li>S2: ShardMetrics recordread-write/errorerror + snapshotquery
 *   <li>S3: ClusterTopology queryreturncorrect shard info
 *   <li>S4: gRPC trace ID propagates(client interceptor inject metadata)
 *   <li>S5: metrics aggregatetotopologyview
 * </ol>
 */
@DisplayName("Phase 3.11-T4: Observability E2E - TraceID + Metrics + Topology")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ObservabilityE2ETest {

    private static InMemoryShardRoutingTable routingTable;
    private static RoutingCoordinator routingCoordinator;
    private static ShardMetricsRegistry metricsRegistry;
    private static ClusterTopologyService topologyService;

    @BeforeAll
    static void setUpAll() {
        routingTable = new InMemoryShardRoutingTable();
        routingCoordinator = new InMemoryRoutingCoordinator(routingTable);
        metricsRegistry = new ShardMetricsRegistry();
        topologyService = new ClusterTopologyService(routingCoordinator, metricsRegistry);
        NodeSlot slotA = new NodeSlot("exec-A", 0);
        routingTable.registerExecutor("exec-A", slotA);
        routingTable.markExecutorReady("exec-A");
        routingTable.assignShard(
                new ShardAssignment(new ShardId(0), slotA, "exec-A", RoutingEpoch.initial()));
    }

    @Test
    @Order(1)
    @DisplayName("S1: TraceId generate + set + get + clear")
    void testTraceIdLifecycle() {
        String traceId = TraceIdPropagator.generateTraceId();
        assertThat(traceId).as("traceId should be 16 chars").hasSize(16);
        TraceIdPropagator.setTraceId(traceId);
        assertThat(TraceIdPropagator.currentTraceId()).isEqualTo(traceId);
        TraceIdPropagator.clearTraceId();
        assertThat(TraceIdPropagator.currentTraceId()).isNull();
    }

    @Test
    @Order(2)
    @DisplayName("S2: ShardMetrics record reads/writes/errors + snapshot")
    void testShardMetrics() {
        metricsRegistry.recordRead(0);
        metricsRegistry.recordRead(0);
        metricsRegistry.recordWrite(0);
        metricsRegistry.recordError(0);
        metricsRegistry.updateReplicationLag(0, 1024L);

        ShardMetricsRegistry.ShardMetrics metrics = metricsRegistry.getOrCreate(0);
        assertThat(metrics.readCount()).isEqualTo(2);
        assertThat(metrics.writeCount()).isEqualTo(1);
        assertThat(metrics.errorCount()).isEqualTo(1);
        assertThat(metrics.replicationLagBytes()).isEqualTo(1024L);
        assertThat(metricsRegistry.snapshot()).containsKey(0);
    }

    @Test
    @Order(3)
    @DisplayName("S3: ClusterTopology returns correct shard info")
    void testClusterTopology() {
        ClusterTopologyService.ClusterTopology topology = topologyService.getClusterTopology();
        assertThat(topology.shardCount()).isEqualTo(1);
        assertThat(topology.shards().get(0).shardId()).isEqualTo(0);
        assertThat(topology.shards().get(0).primaryExecutor()).isEqualTo("exec-A");
        assertThat(topology.shards().get(0).readCount()).isEqualTo(2);
        assertThat(topology.shards().get(0).writeCount()).isEqualTo(1);
    }

    @Test
    @Order(4)
    @DisplayName("S4: gRPC trace ID propagation via client interceptor")
    void testGrpcTraceIdPropagation() throws Exception {
        String serverName = "trace-test-" + UUID.randomUUID();
        Server server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(
                                new ClusterManagementServiceGrpc
                                        .ClusterManagementServiceImplBase() {
                                    @Override
                                    public void heartbeat(
                                            HeartbeatRequest request,
                                            io.grpc.stub.StreamObserver<HeartbeatResponse>
                                                    responseObserver) {
                                        String receivedTraceId = TraceIdPropagator.currentTraceId();
                                        responseObserver.onNext(
                                                HeartbeatResponse.newBuilder()
                                                        .setStatus(
                                                                Status.newBuilder()
                                                                        .setCode(Status.Code.OK)
                                                                        .setMessage(
                                                                                "traceId="
                                                                                        + receivedTraceId)
                                                                        .build())
                                                        .setServerTimestampMs(
                                                                System.currentTimeMillis())
                                                        .setNextHeartbeatIntervalMs(5000L)
                                                        .build());
                                        responseObserver.onCompleted();
                                    }
                                })
                        .build()
                        .start();
        ManagedChannel channel =
                InProcessChannelBuilder.forName(serverName)
                        .directExecutor()
                        .intercept(TraceIdPropagator.clientInterceptor())
                        .build();
        try {
            TraceIdPropagator.setTraceId("test1234");
            ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub stub =
                    ClusterManagementServiceGrpc.newBlockingStub(channel);
            HeartbeatResponse resp =
                    stub.heartbeat(
                            HeartbeatRequest.newBuilder()
                                    .setExecutorId("exec-A")
                                    .setNodeId("node-A")
                                    .build());
            assertThat(resp.getStatus().getMessage())
                    .as("server should receive traceId from client metadata")
                    .contains("test1234");
        } finally {
            TraceIdPropagator.clearTraceId();
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    @Order(5)
    @DisplayName("S5: Metrics aggregated into topology after updates")
    void testMetricsAggregatedInTopology() {
        metricsRegistry.recordRead(0);
        metricsRegistry.recordRead(0);
        metricsRegistry.recordWrite(0);
        metricsRegistry.recordCompaction(0);
        ClusterTopologyService.ClusterTopology topology = topologyService.getClusterTopology();
        ClusterTopologyService.ShardTopologyInfo shard0 = topology.shards().get(0);
        assertThat(shard0.readCount()).as("reads should be aggregated").isGreaterThanOrEqualTo(4);
        assertThat(shard0.writeCount()).as("writes should be aggregated").isGreaterThanOrEqualTo(2);
    }
}
