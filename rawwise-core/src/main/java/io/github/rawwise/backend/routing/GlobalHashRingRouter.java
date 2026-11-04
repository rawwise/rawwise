package io.github.rawwise.backend.routing;

import io.github.rawwise.common.hash.ConsistentHashRing;
import io.github.rawwise.common.hash.DefaultConsistentHashRing;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * Global consistent hash ring router (L1 node shard routing).
 *
 * <p>Architecture:
 *
 * <pre>
 * Old approach: ModHash(key) % shardCount -> shardId -> RoutingTable -> executorId
 * New approach: ConsistentHashRing<ShardId> -> executorId
 * - shardId -> hash -> ring node -> physical executorId
 * - node join/leave triggers shard migration (consistent hash feature)
 * </pre>
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>{@link #routeShard(int)} returns the executorId for a shardId (via consistent hash ring)
 *   <li>{@link #onNodeJoin(String)} node join: updates ring + returns list of shards that need
 *       migration to the new node
 *   <li>{@link #onNodeLeave(String)} node leave: updates ring + returns list of shards that need
 *       migration from the leaving node
 *   <li>default virtual node count is 160 (consistent with {@link DefaultConsistentHashRing}),
 *       ensuring uniform distribution
 *   <li>Integrates with {@link RoutingCoordinator}: routing results used for write/query routing
 * </ul>
 *
 * <p>Advantages over ModHash:
 *
 * <ul>
 *   <li>node join/leave requires minimal data migration (only affected shards, not full
 *       reassignment)
 *   <li>guarantees uniform shard distribution across nodes
 *   <li>supports heterogeneous node clusters
 * </ul>
 */
@Slf4j
public class GlobalHashRingRouter {
    private static final int DEFAULT_VIRTUAL_NODES = 160;
    private static final int DEFAULT_SHARD_COUNT = 8;

    private final ConsistentHashRing<String> ring;
    private final int shardCount;

    public GlobalHashRingRouter() {
        this(DEFAULT_SHARD_COUNT, DEFAULT_VIRTUAL_NODES);
    }

    public GlobalHashRingRouter(int shardCount, int virtualNodesPerNode) {
        this.shardCount = shardCount <= 0 ? DEFAULT_SHARD_COUNT : shardCount;
        this.ring = new DefaultConsistentHashRing<>(virtualNodesPerNode, false);
        log.info(
                "GlobalHashRingRouter initialized: shardCount={}, virtualNodes={}",
                this.shardCount,
                virtualNodesPerNode);
    }

    /**
     * Register a node to the consistent hash ring.
     *
     * <p>BE Node calls this method on startup to join the ring. After joining, some shards are
     * re-routed to the new node.
     *
     * @param executorId executor identifier
     */
    public void addNode(String executorId) {
        Objects.requireNonNull(executorId, "executorId");
        try {
            ring.addNode(executorId);
            log.info("Node added to hash ring: executorId={}", executorId);
        } catch (IllegalArgumentException e) {
            log.warn("Node already exists in hash ring, ignoring: executorId={}", executorId);
        }
    }

    /**
     * Remove a node from the consistent hash ring.
     *
     * <p>BE Node calls this method before shutdown. After leaving, its shards are re-routed to
     * other nodes.
     *
     * @param executorId executor identifier
     */
    public void removeNode(String executorId) {
        Objects.requireNonNull(executorId, "executorId");
        ring.removeNode(executorId);
        log.info("Node removed from hash ring: executorId={}", executorId);
    }

    /**
     * Route a shardId to an executorId (via consistent hash ring).
     *
     * @param shardId shard identifier
     * @return executorId, or null if ring is empty
     */
    public String routeShard(int shardId) {
        return ring.getNode(shardId);
    }

    /**
     * Node join triggers shard migration.
     *
     * <p>Semantics:
     *
     * <ol>
     *   <li>Record shard-to-executor mapping before join (snapshot)
     *   <li>Add the new node to the ring
     *   <li>Compare old and new mappings, output shards with changed routing
     *   <li>Return migration plan: each affected shard from oldExecutor -> newExecutor
     * </ol>
     *
     * @param newExecutorId newly joined executorId
     * @return list of migration plans (shardId, fromExecutor, toExecutor)
     */
    public synchronized List<ShardMigration> onNodeJoin(String newExecutorId) {
        Objects.requireNonNull(newExecutorId, "newExecutorId");
        Map<Integer, String> before = snapshotRouting();
        ring.addNode(newExecutorId);
        Map<Integer, String> after = snapshotRouting();
        List<ShardMigration> migrations = computeMigrations(before, after);
        log.info("onNodeJoin: executorId={}, migrations={}", newExecutorId, migrations.size());
        return migrations;
    }

    /**
     * Node leave triggers shard migration.
     *
     * <p>Semantics:
     *
     * <ol>
     *   <li>Record shard-to-executor mapping before leave (snapshot)
     *   <li>Remove the leaving node from the ring
     *   <li>Compare old and new mappings, output shards with changed routing
     *   <li>Return migration plan: each affected shard from leavingExecutor -> newExecutor
     * </ol>
     *
     * @param leavingExecutorId the executorId that is leaving
     * @return list of migration plans (shardId, fromExecutor, toExecutor)
     */
    public synchronized List<ShardMigration> onNodeLeave(String leavingExecutorId) {
        Objects.requireNonNull(leavingExecutorId, "leavingExecutorId");
        Map<Integer, String> before = snapshotRouting();
        ring.removeNode(leavingExecutorId);
        Map<Integer, String> after = snapshotRouting();
        List<ShardMigration> migrations = computeMigrations(before, after);
        log.info("onNodeLeave: executorId={}, migrations={}", leavingExecutorId, migrations.size());
        return migrations;
    }

    /**
     * Returns a snapshot of all current shard routing mappings.
     *
     * @return shardId -> executorId mapping
     */
    public Map<Integer, String> snapshotRouting() {
        Map<Integer, String> snapshot = new HashMap<>();
        for (int i = 0; i < shardCount; i++) {
            String executor = ring.getNode(i);
            if (executor != null) {
                snapshot.put(i, executor);
            }
        }
        return snapshot;
    }

    /** Returns the node count in the ring. */
    public int getNodeCount() {
        return ring.getNodeCount();
    }

    /** Returns the total shard count. */
    public int getShardCount() {
        return shardCount;
    }

    private List<ShardMigration> computeMigrations(
            Map<Integer, String> before, Map<Integer, String> after) {
        List<ShardMigration> migrations = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : after.entrySet()) {
            int shardId = entry.getKey();
            String newExecutor = entry.getValue();
            String oldExecutor = before.get(shardId);
            if (oldExecutor == null || !oldExecutor.equals(newExecutor)) {
                migrations.add(new ShardMigration(shardId, oldExecutor, newExecutor));
            }
        }
        return migrations;
    }

    /**
     * Shard migration plan.
     *
     * @param shardId shard identifier
     * @param fromExecutor source executorId (can be null, indicates new assignment)
     * @param toExecutor target executorId
     */
    public record ShardMigration(int shardId, String fromExecutor, String toExecutor) {
        public ShardMigration {
            Objects.requireNonNull(toExecutor, "toExecutor");
        }

        public boolean isNewAssignment() {
            return fromExecutor == null;
        }

        @Override
        public String toString() {
            return "ShardMigration{shard="
                    + shardId
                    + ", "
                    + fromExecutor
                    + " -> "
                    + toExecutor
                    + "}";
        }
    }
}
