package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.routing.GlobalHashRingRouter;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Phase 4-T3: allconsistentconsistent hash ring E2E test.
 *
 * <p>validation L1 cross Node consistentconsistent hash routingbycorelink:
 *
 * <ol>
 *   <li>S1: basic pathby - singlenode, allhas shard pathbytoshouldnode
 *   <li>S2: nodeadd - secondnodeadd, partial shard migratetonewnode
 *   <li>S3: migratemostsmall - nodeaddafteronlymigrateaffected shard(non-allcountre-allocate)
 *   <li>S4: nodeleave - nodeleaveafter shard migratereturn remainingnode
 *   <li>S5: distribution uniformity - multinodewhen shard
 *       distributionforuniform(virtualnodeguarantee)
 * </ol>
 */
@DisplayName("Phase 4-T3: Global Consistent Hash Ring E2E")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConsistentHashE2ETest {

    private static final int SHARD_COUNT = 16;
    private static final int VIRTUAL_NODES = 160;

    @Test
    @Order(1)
    @DisplayName("S1: Single node - all shards route to it")
    void testSingleNodeRouting() {
        GlobalHashRingRouter router = new GlobalHashRingRouter(SHARD_COUNT, VIRTUAL_NODES);
        router.addNode("exec-A");
        for (int i = 0; i < SHARD_COUNT; i++) {
            assertThat(router.routeShard(i))
                    .as("shard %d should route to exec-A", i)
                    .isEqualTo("exec-A");
        }
        assertThat(router.getNodeCount()).isEqualTo(1);
    }

    @Test
    @Order(2)
    @DisplayName("S2: Node join - some shards migrate to new node")
    void testNodeJoinTriggersMigration() {
        GlobalHashRingRouter router = new GlobalHashRingRouter(SHARD_COUNT, VIRTUAL_NODES);
        router.addNode("exec-A");
        List<GlobalHashRingRouter.ShardMigration> migrations = router.onNodeJoin("exec-B");
        assertThat(migrations).as("join should trigger migrations").isNotEmpty();
        assertThat(router.getNodeCount()).isEqualTo(2);
        for (GlobalHashRingRouter.ShardMigration m : migrations) {
            assertThat(m.toExecutor()).as("migrated shard should go to exec-B").isEqualTo("exec-B");
            assertThat(m.fromExecutor()).as("source should be exec-A").isEqualTo("exec-A");
        }
    }

    @Test
    @Order(3)
    @DisplayName("S3: Minimal migration - join only moves affected shards, not all")
    void testMinimalMigration() {
        GlobalHashRingRouter router = new GlobalHashRingRouter(SHARD_COUNT, VIRTUAL_NODES);
        router.addNode("exec-A");
        router.onNodeJoin("exec-B");
        List<GlobalHashRingRouter.ShardMigration> migrations3 = router.onNodeJoin("exec-C");
        assertThat(migrations3.size())
                .as("third node join should migrate fewer than total shards")
                .isLessThan(SHARD_COUNT);
        for (GlobalHashRingRouter.ShardMigration m : migrations3) {
            assertThat(m.toExecutor()).as("migrated shard should go to exec-C").isEqualTo("exec-C");
            assertThat(m.fromExecutor())
                    .as("source should be exec-A or exec-B, not exec-C")
                    .isIn("exec-A", "exec-B");
        }
    }

    @Test
    @Order(4)
    @DisplayName("S4: Node leave - shards migrate back to remaining node")
    void testNodeLeaveTriggersMigration() {
        GlobalHashRingRouter router = new GlobalHashRingRouter(SHARD_COUNT, VIRTUAL_NODES);
        router.addNode("exec-A");
        router.addNode("exec-B");
        List<GlobalHashRingRouter.ShardMigration> migrations = router.onNodeLeave("exec-B");
        assertThat(router.getNodeCount()).isEqualTo(1);
        for (GlobalHashRingRouter.ShardMigration m : migrations) {
            assertThat(m.fromExecutor()).as("leaving node should be source").isEqualTo("exec-B");
            assertThat(m.toExecutor()).as("target should be exec-A").isEqualTo("exec-A");
        }
        for (int i = 0; i < SHARD_COUNT; i++) {
            assertThat(router.routeShard(i)).isEqualTo("exec-A");
        }
    }

    @Test
    @Order(5)
    @DisplayName("S5: Distribution balance - 4 nodes, shards roughly evenly distributed")
    void testDistributionBalance() {
        GlobalHashRingRouter router = new GlobalHashRingRouter(SHARD_COUNT, VIRTUAL_NODES);
        router.addNode("exec-A");
        router.addNode("exec-B");
        router.addNode("exec-C");
        router.addNode("exec-D");
        Map<Integer, String> routing = router.snapshotRouting();
        Map<String, Long> counts = new java.util.HashMap<>();
        for (String exec : routing.values()) {
            counts.merge(exec, 1L, Long::sum);
        }
        assertThat(counts.size()).as("all 4 nodes should have shards").isEqualTo(4);
        long maxCount = counts.values().stream().mapToLong(Long::longValue).max().orElse(0);
        long minCount = counts.values().stream().mapToLong(Long::longValue).min().orElse(0);
        assertThat(maxCount - minCount)
                .as("distribution imbalance should be small (max-min <= %d)", SHARD_COUNT / 2)
                .isLessThanOrEqualTo(SHARD_COUNT / 2);
    }

    @Test
    @Order(6)
    @DisplayName("S6: Routing stability - uninvolved shards keep same owner after join")
    void testRoutingStability() {
        GlobalHashRingRouter router = new GlobalHashRingRouter(SHARD_COUNT, VIRTUAL_NODES);
        router.addNode("exec-A");
        router.addNode("exec-B");
        Map<Integer, String> before = router.snapshotRouting();
        List<GlobalHashRingRouter.ShardMigration> migrations = router.onNodeJoin("exec-C");
        Map<Integer, String> after = router.snapshotRouting();
        for (int shardId : before.keySet()) {
            if (migrations.stream().noneMatch(m -> m.shardId() == shardId)) {
                assertThat(after.get(shardId))
                        .as("unmigrated shard %d should keep same owner", shardId)
                        .isEqualTo(before.get(shardId));
            }
        }
    }
}
