package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.RoutingStatus;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.backend.server.health.FailureDetector;
import io.github.rawwise.backend.server.health.HeartbeatService;
import io.github.rawwise.backend.server.service.ClusterManagementServiceImpl;
import io.github.rawwise.server.ClusterManagementServiceGrpc;
import io.github.rawwise.server.HeartbeatRequest;
import io.github.rawwise.server.HeartbeatResponse;
import io.github.rawwise.server.RegisterExecutorRequest;
import io.github.rawwise.server.RegisterExecutorResponse;
import io.github.rawwise.server.Status;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Phase 3.2-T6: healthcheckandfailuredetect E2E test.
 *
 * <p>validationcompleteentirefailuredetect -> shard re-allocatelink:
 *
 * <ol>
 *   <li>2 BE Nodes viapass gRPC registerto BE Server, eachabovereport heartbeat
 *   <li>simulate Node B "kill -9": stop HeartbeatService + close gRPC server
 *   <li>BE Server FailureDetector intimeoutwindowinnerdetectto Node B heartbeat loss
 *   <li>trigger {@code RoutingCoordinator.onExecutorDown("exec-B")} -> shard re-allocate
 *   <li>validation shard 1 notagainallocateto exec-B, and shard 0 notaffected
 * </ol>
 *
 * <p>follow RawWise E2E teststandard:
 *
 * <ul>
 *   <li>makeusetrue {@link ClusterManagementServiceImpl} + {@link
 *       InMemoryRoutingCoordinator}(production entry)
 *   <li>makeusetrue {@link HeartbeatService} + {@link FailureDetector}(non- mock)
 *   <li>makeuse InProcess gRPC(usestrue gRPC transferlayer)
 *   <li>FailureDetector makeusecompressshorttimeoutparameterbyacceleratetest(500ms scan / 1500ms
 *       timeout), productiondefaultis 10s scan / 15s timeout
 * </ul>
 */
@DisplayName(
        "Phase 3.2-T6: Health Check & Failure Detection E2E — Node kill -9 -> shard reassignment")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HealthCheckE2ETest {

    private static final String EXEC_A = "exec-A";
    private static final String EXEC_B = "exec-B";
    private static final String NODE_A = "node-A";
    private static final String NODE_B = "node-B";
    private static final int SHARD_0 = 0;
    private static final int SHARD_1 = 1;

    private static InMemoryShardRoutingTable routingTable;
    private static InMemoryRoutingCoordinator routingCoordinator;
    private static ClusterManagementServiceImpl clusterService;
    private static Server beServer;
    private static ManagedChannel beServerChannel;
    private static ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub clusterStub;

    private static HeartbeatService heartbeatA;
    private static HeartbeatService heartbeatB;
    private static FailureDetector failureDetector;

    @BeforeAll
    static void setUpAll() throws Exception {
        routingTable = new InMemoryShardRoutingTable();
        routingCoordinator = new InMemoryRoutingCoordinator(routingTable);
        clusterService = new ClusterManagementServiceImpl(null, null, "FIFO", routingCoordinator);

        String beServerName = "be-server-" + UUID.randomUUID();
        beServer =
                InProcessServerBuilder.forName(beServerName)
                        .directExecutor()
                        .addService(clusterService)
                        .build()
                        .start();
        beServerChannel = InProcessChannelBuilder.forName(beServerName).directExecutor().build();
        clusterStub = ClusterManagementServiceGrpc.newBlockingStub(beServerChannel);

        failureDetector = new FailureDetector(clusterService, routingCoordinator, 500L, 1500L);
        failureDetector.start();
    }

    @AfterAll
    static void tearDownAll() {
        if (heartbeatA != null) {
            heartbeatA.stop();
        }
        if (heartbeatB != null) {
            heartbeatB.stop();
        }
        if (failureDetector != null) {
            failureDetector.stop();
        }
        if (beServerChannel != null) {
            beServerChannel.shutdownNow();
        }
        if (beServer != null) {
            beServer.shutdownNow();
        }
    }

    @Test
    @Order(1)
    @DisplayName(
            "S1: Both BE Nodes register — routing table assigns shard 0->exec-A, shard 1->exec-B")
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
    @DisplayName("S2: Both BE Nodes start heartbeats — lastHeartbeat updated on BE Server")
    void testHeartbeatsStart() throws Exception {
        heartbeatA = new HeartbeatService(clusterStub, EXEC_A, NODE_A, SHARD_0, 500L);
        heartbeatB = new HeartbeatService(clusterStub, EXEC_B, NODE_B, SHARD_1, 500L);
        heartbeatA.start();
        heartbeatB.start();

        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertThat(clusterService.getLastHeartbeatMs(EXEC_A))
                                    .as("exec-A lastHeartbeat should be recorded")
                                    .isNotNull();
                            assertThat(clusterService.getLastHeartbeatMs(EXEC_B))
                                    .as("exec-B lastHeartbeat should be recorded")
                                    .isNotNull();
                        });

        long now = System.currentTimeMillis();
        assertThat(now - clusterService.getLastHeartbeatMs(EXEC_A)).isLessThan(2000L);
        assertThat(now - clusterService.getLastHeartbeatMs(EXEC_B)).isLessThan(2000L);
    }

    @Test
    @Order(3)
    @DisplayName("S3: Manual heartbeat RPC — BE Server returns OK + server timestamp")
    void testManualHeartbeatRpc() {
        HeartbeatResponse response =
                clusterStub.heartbeat(
                        HeartbeatRequest.newBuilder()
                                .setExecutorId(EXEC_A)
                                .setNodeId(NODE_A)
                                .setShardId(SHARD_0)
                                .setTimestampMs(System.currentTimeMillis())
                                .build());
        assertThat(response.getStatus().getCode()).isEqualTo(Status.Code.OK);
        assertThat(response.getServerTimestampMs()).isGreaterThan(0L);
        assertThat(response.getNextHeartbeatIntervalMs()).isEqualTo(5000L);
    }

    @Test
    @Order(4)
    @DisplayName(
            "S4: Simulate Node B kill -9 — stop heartbeat -> FailureDetector detects timeout -> shard 1 reassigned away from exec-B")
    void testNodeKillTriggersShardReassignment() {
        heartbeatB.stop();
        heartbeatB = null;

        await().atMost(6, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            ShardLookupResult shard1Lookup =
                                    routingCoordinator.lookupOwner(new ShardId(SHARD_1));
                            assertThat(shard1Lookup.hasAssignment())
                                    .as(
                                            "shard 1 should still have an assignment after reassignment")
                                    .isTrue();
                            assertThat(shard1Lookup.assignment().executorId())
                                    .as(
                                            "shard 1 should NOT be assigned to failed exec-B after FailureDetector triggers onExecutorDown")
                                    .isNotEqualTo(EXEC_B);
                        });

        ShardLookupResult shard0Lookup = routingCoordinator.lookupOwner(new ShardId(SHARD_0));
        assertThat(shard0Lookup.assigned())
                .as("shard 0 (on healthy exec-A) should remain assigned")
                .isTrue();
        assertThat(shard0Lookup.assignment().executorId())
                .as("shard 0 should still be on exec-A")
                .isEqualTo(EXEC_A);

        assertThat(clusterService.getRegisteredExecutorIds())
                .as(
                        "exec-B should be removed from heartbeat registry after FailureDetector cleanup")
                .doesNotContain(EXEC_B);
    }

    @Test
    @Order(5)
    @DisplayName(
            "S5: After reassignment — shard 1 routed to surviving exec-A (single-node fallback, RECOVERING state)")
    void testShard1ReassignedToSurvivor() {
        ShardLookupResult shard1Lookup = routingCoordinator.lookupOwner(new ShardId(SHARD_1));
        assertThat(shard1Lookup.hasAssignment())
                .as("shard 1 should have an assignment after reassignment")
                .isTrue();
        assertThat(shard1Lookup.assignment().executorId())
                .as("shard 1 should be reassigned to surviving exec-A")
                .isEqualTo(EXEC_A);
        assertThat(shard1Lookup.status())
                .as(
                        "shard 1 status should be RECOVERING (recovering=true triggers data recovery phase)")
                .isEqualTo(RoutingStatus.RECOVERING);
    }

    @Test
    @Order(6)
    @DisplayName("S6: Re-registered exec-B triggers another epoch advancement on shard 1")
    void testReRegistrationAdvancesEpoch() {
        ShardLookupResult before = routingCoordinator.lookupOwner(new ShardId(SHARD_1));
        long oldEpoch = before.assignment().epoch().value();

        RegisterExecutorResponse resp = registerNode(EXEC_B, NODE_B, SHARD_1, 0);
        assertThat(resp.getStatus().getCode()).isEqualTo(Status.Code.OK);
        assertThat(resp.getEpoch()).isGreaterThan(oldEpoch);

        ShardLookupResult after = routingCoordinator.lookupOwner(new ShardId(SHARD_1));
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
}
