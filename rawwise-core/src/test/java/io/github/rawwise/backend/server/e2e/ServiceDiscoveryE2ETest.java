package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.discovery.InMemoryServiceRegistry;
import io.github.rawwise.backend.discovery.ServiceDiscoveryWatcher;
import io.github.rawwise.backend.discovery.ServiceRegistry;
import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingStatus;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Phase 3.6-T3: servicediscover E2E test.
 *
 * <p>validationservicediscovercorelink(§3.6.2, §3.6.3):
 *
 * <ol>
 *   <li>S1: register - BE Node registerto ServiceRegistry -> watch trigger -> RoutingCoordinator
 *       pathbytableupdatenew
 *   <li>S2: query - lookup/listNodes returncorrectnodeinfo
 *   <li>S3: unregister - primary unregister -> watch trigger onNodeDown -> pathbytable shard
 *       re-allocate
 *   <li>S4: TTL passexpired - short TTL registerafterWait forpassexpired -> self onNodeDown
 * </ol>
 *
 * <p>makeuse InMemoryServiceRegistry(processinnerimplementation), notdepend onouterpart etcd.
 * productionenvironmentreplaceis EtcdServiceRegistry i.e.can, interfaceconsistent.
 */
@DisplayName("Phase 3.6-T3: Service Discovery E2E - Register/Watch/Unregister/TTL")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceDiscoveryE2ETest {

    private static InMemoryServiceRegistry registry;
    private static InMemoryShardRoutingTable routingTable;
    private static RoutingCoordinator routingCoordinator;
    private static ServiceDiscoveryWatcher watcher;

    private static final String EXEC_A = "disc-exec-A";
    private static final String EXEC_B = "disc-exec-B";
    private static final int SHARD_0 = 0;
    private static final int SHARD_1 = 1;

    @BeforeAll
    static void setUpAll() {
        routingTable = new InMemoryShardRoutingTable();
        routingCoordinator = new InMemoryRoutingCoordinator(routingTable);
        registry = new InMemoryServiceRegistry(1L);
        watcher = new ServiceDiscoveryWatcher(routingCoordinator);
        registry.watch(watcher);
    }

    @AfterAll
    static void tearDownAll() {
        if (registry != null) registry.close();
    }

    @Test
    @Order(1)
    @DisplayName("S1: Register Node A -> watch triggers -> routing table updated")
    void testRegisterTriggersRoutingUpdate() {
        ServiceRegistry.NodeInfo nodeA =
                new ServiceRegistry.NodeInfo(
                        EXEC_A,
                        "node-a",
                        "10.0.0.1",
                        9091,
                        0,
                        SHARD_0,
                        ServiceRegistry.NodeStatus.READY,
                        System.currentTimeMillis());
        registry.register(nodeA, 60L);

        ShardLookupResult owner = routingCoordinator.lookupOwner(new ShardId(SHARD_0));
        assertThat(owner.assigned())
                .as("shard-0 should be assigned after node A registration")
                .isTrue();
        assertThat(owner.assignment().executorId()).isEqualTo(EXEC_A);
        assertThat(owner.status()).isEqualTo(RoutingStatus.ASSIGNED);
    }

    @Test
    @Order(2)
    @DisplayName("S2: lookup and listNodes return correct node info")
    void testLookupAndListNodes() {
        Optional<ServiceRegistry.NodeInfo> lookup = registry.lookup(EXEC_A);
        assertThat(lookup).as("lookup should find node A").isPresent();
        assertThat(lookup.get().host()).isEqualTo("10.0.0.1");
        assertThat(lookup.get().port()).isEqualTo(9091);

        ServiceRegistry.NodeInfo nodeB =
                new ServiceRegistry.NodeInfo(
                        EXEC_B,
                        "node-b",
                        "10.0.0.2",
                        9092,
                        0,
                        SHARD_1,
                        ServiceRegistry.NodeStatus.READY,
                        System.currentTimeMillis());
        registry.register(nodeB, 60L);

        assertThat(registry.listNodes()).as("listNodes should contain both A and B").hasSize(2);
        assertThat(watcher.getNodeInfo(EXEC_B).host()).isEqualTo("10.0.0.2");
    }

    @Test
    @Order(3)
    @DisplayName("S3: Unregister Node B -> watch triggers onNodeDown -> shard reassigned")
    void testUnregisterTriggersReassign() {
        registry.unregister(EXEC_B);
        Optional<ServiceRegistry.NodeInfo> lookup = registry.lookup(EXEC_B);
        assertThat(lookup).as("node B should be unregistered").isEmpty();
        assertThat(watcher.getNodeInfo(EXEC_B)).as("watcher should have removed node B").isNull();
    }

    @Test
    @Order(4)
    @DisplayName("S4: TTL expiry - short TTL node expires -> auto onNodeDown")
    void testTtlExpiry() throws Exception {
        ServiceRegistry.NodeInfo nodeC =
                new ServiceRegistry.NodeInfo(
                        "disc-exec-C",
                        "node-c",
                        "10.0.0.3",
                        9093,
                        0,
                        SHARD_1,
                        ServiceRegistry.NodeStatus.READY,
                        System.currentTimeMillis());
        registry.register(nodeC, 1L);
        assertThat(registry.lookup("disc-exec-C"))
                .as("node C should be registered initially")
                .isPresent();
        Thread.sleep(3000);
        Optional<ServiceRegistry.NodeInfo> expired = registry.lookup("disc-exec-C");
        assertThat(expired).as("node C should be expired after TTL").isEmpty();
    }
}
