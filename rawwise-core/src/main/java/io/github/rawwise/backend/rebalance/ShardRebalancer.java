package io.github.rawwise.backend.rebalance;

import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardRoutingTable;
import io.github.rawwise.backend.server.migration.ShardMigrationCoordinator;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Rebalances shard assignments when nodes join or leave the cluster.
 *
 * <p>On join: migrates shards from heavily loaded nodes to the new node to balance the
 * distribution. On leave: migrates shards from the departing node to the remaining nodes.
 *
 * <p>Migration is delegated to {@link ShardMigrationCoordinator} which handles the actual data
 * movement via gRPC scan/migrate RPCs.
 */
@Slf4j
public class ShardRebalancer {

    private final RoutingCoordinator routingCoordinator;
    private final ShardRoutingTable routingTable;
    private final ShardMigrationCoordinator migrationCoordinator;

    public ShardRebalancer(
            RoutingCoordinator routingCoordinator,
            ShardRoutingTable routingTable,
            ShardMigrationCoordinator migrationCoordinator) {
        this.routingCoordinator = routingCoordinator;
        this.routingTable = routingTable;
        this.migrationCoordinator = migrationCoordinator;
    }

    /**
     * Rebalance shards when a new executor joins the cluster.
     *
     * <p>Migrates shards from executors that have more shards than the joining executor to achieve
     * a more balanced distribution.
     *
     * @param joiningExecutorId the executor that just joined
     * @param tableId the table to rebalance
     * @return rebalance result
     */
    public RebalanceResult rebalanceOnJoin(String joiningExecutorId, String tableId) {
        Collection<ShardAssignment> assignments = routingTable.assignments();
        if (assignments.isEmpty()) {
            return RebalanceResult.success("No shards to rebalance", 0);
        }

        Map<String, Integer> shardCounts = new HashMap<>();
        for (ShardAssignment a : assignments) {
            shardCounts.merge(a.executorId(), 1, Integer::sum);
        }

        int joiningCount = shardCounts.getOrDefault(joiningExecutorId, 0);
        int totalShards = assignments.size();
        int migratedCount = 0;

        for (ShardAssignment a : assignments) {
            if (a.executorId().equals(joiningExecutorId)) {
                continue;
            }
            int sourceCount = shardCounts.getOrDefault(a.executorId(), 0);
            if (sourceCount > joiningCount) {
                log.info(
                        "Rebalance on join: migrating shard {} from {} to {}",
                        a.shardId().value(),
                        a.executorId(),
                        joiningExecutorId);
                ShardMigrationCoordinator.MigrationResult result =
                        migrationCoordinator.migrateShard(
                                tableId,
                                a.shardId().value(),
                                a.executorId(),
                                joiningExecutorId,
                                Math.max(1, totalShards));
                if (result.success()) {
                    migratedCount++;
                    joiningCount++;
                    shardCounts.merge(a.executorId(), -1, Integer::sum);
                    shardCounts.merge(joiningExecutorId, 1, Integer::sum);
                } else {
                    log.warn(
                            "Rebalance migration failed for shard {}: {}",
                            a.shardId().value(),
                            result.error());
                }
            }
        }

        String msg =
                "Rebalanced on join: migrated "
                        + migratedCount
                        + " shard(s) to "
                        + joiningExecutorId;
        log.info(msg);
        return RebalanceResult.success(msg, migratedCount);
    }

    /**
     * Rebalance shards when an executor leaves the cluster.
     *
     * <p>Migrates all shards from the leaving executor to the remaining executors in round-robin
     * fashion.
     *
     * @param leavingExecutorId the executor that is leaving
     * @param tableId the table to rebalance
     * @return rebalance result
     */
    public RebalanceResult rebalanceOnLeave(String leavingExecutorId, String tableId) {
        Collection<ShardAssignment> assignments = routingTable.assignments();

        List<ShardAssignment> toMigrate =
                assignments.stream().filter(a -> a.executorId().equals(leavingExecutorId)).toList();

        if (toMigrate.isEmpty()) {
            return RebalanceResult.success("No shards to migrate from leaving executor", 0);
        }

        Set<String> allExecutors = routingTable.registeredExecutors();
        List<String> targetExecutors =
                allExecutors.stream().filter(eid -> !eid.equals(leavingExecutorId)).toList();

        if (targetExecutors.isEmpty()) {
            String msg = "No available target executors for migration from " + leavingExecutorId;
            log.warn(msg);
            return RebalanceResult.failed(msg);
        }

        int totalShards = assignments.size();
        int migratedCount = 0;
        int targetIndex = 0;

        for (ShardAssignment a : toMigrate) {
            String targetExecutor = targetExecutors.get(targetIndex % targetExecutors.size());
            targetIndex++;
            log.info(
                    "Rebalance on leave: migrating shard {} from {} to {}",
                    a.shardId().value(),
                    leavingExecutorId,
                    targetExecutor);
            ShardMigrationCoordinator.MigrationResult result =
                    migrationCoordinator.migrateShard(
                            tableId,
                            a.shardId().value(),
                            leavingExecutorId,
                            targetExecutor,
                            Math.max(1, totalShards));
            if (result.success()) {
                migratedCount++;
            } else {
                log.warn(
                        "Rebalance migration failed for shard {}: {}",
                        a.shardId().value(),
                        result.error());
            }
        }

        String msg =
                "Rebalanced on leave: migrated "
                        + migratedCount
                        + " shard(s) from "
                        + leavingExecutorId;
        log.info(msg);
        return RebalanceResult.success(msg, migratedCount);
    }

    /**
     * Result of a rebalance operation.
     *
     * @param success whether the rebalance succeeded
     * @param message human-readable message
     * @param migratedCount number of shards migrated
     */
    public record RebalanceResult(boolean success, String message, int migratedCount) {
        public static RebalanceResult success(String message, int migratedCount) {
            return new RebalanceResult(true, message, migratedCount);
        }

        public static RebalanceResult failed(String message) {
            return new RebalanceResult(false, message, 0);
        }
    }
}
