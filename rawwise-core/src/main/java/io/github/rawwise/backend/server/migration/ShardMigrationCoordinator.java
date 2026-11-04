package io.github.rawwise.backend.server.migration;

import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingEpoch;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.backend.routing.ShardRoutingTable;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.KeyBatch;
import io.github.rawwise.server.MigrateBatchRequest;
import io.github.rawwise.server.MigrateBatchResponse;
import io.github.rawwise.server.ScanKeysRequest;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShardMigrationCoordinator {

    private static final int DEFAULT_BATCH_SIZE = 1000;

    private final Map<String, ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub>
            executorStubs;
    private final RoutingCoordinator routingCoordinator;
    private final ShardRoutingTable routingTable;

    public ShardMigrationCoordinator(
            Map<String, ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub>
                    executorStubs,
            RoutingCoordinator routingCoordinator,
            ShardRoutingTable routingTable) {
        this.executorStubs = executorStubs;
        this.routingCoordinator = routingCoordinator;
        this.routingTable = routingTable;
    }

    public MigrationResult migrateShard(
            String tableId,
            int shardId,
            String sourceExecutorId,
            String targetExecutorId,
            int totalShardCount) {
        log.info(
                "Starting shard migration: tableId={}, shardId={}, source={}, target={}, totalShards={}",
                tableId,
                shardId,
                sourceExecutorId,
                targetExecutorId,
                totalShardCount);

        ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub sourceStub =
                executorStubs.get(sourceExecutorId);
        ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub targetStub =
                executorStubs.get(targetExecutorId);

        if (sourceStub == null) {
            return MigrationResult.failed("Source stub not found: " + sourceExecutorId);
        }
        if (targetStub == null) {
            return MigrationResult.failed("Target stub not found: " + targetExecutorId);
        }

        int totalMigrated = 0;
        int totalFailed = 0;
        int batchCount = 0;

        try {
            ScanKeysRequest scanRequest =
                    ScanKeysRequest.newBuilder()
                            .setTableId(tableId)
                            .setShardId(shardId)
                            .setBatchSize(DEFAULT_BATCH_SIZE)
                            .setTotalShardCount(totalShardCount)
                            .build();

            Iterator<KeyBatch> scanner = sourceStub.scanKeys(scanRequest);

            while (scanner.hasNext()) {
                KeyBatch batch = scanner.next();
                int batchSize = batch.getKeysCount();
                if (batchSize == 0) continue;

                // 2. MigrateBatch to target (write only, delete_after_migrate=false)
                MigrateBatchRequest writeRequest =
                        MigrateBatchRequest.newBuilder()
                                .setTableId(tableId)
                                .setTargetShardId(shardId)
                                .addAllKeys(batch.getKeysList())
                                .addAllValues(batch.getValuesList())
                                .setDeleteAfterMigrate(false)
                                .build();

                MigrateBatchResponse writeResponse = targetStub.migrateBatch(writeRequest);
                totalMigrated += writeResponse.getMigratedCount();
                totalFailed += writeResponse.getFailedCount();
                batchCount++;

                if (!writeResponse.getError().isEmpty()) {
                    log.warn(
                            "Migration batch {} write partial error: {}",
                            batchCount,
                            writeResponse.getError());
                }

                // 3. Delete from source (only if write succeeded)
                if (writeResponse.getMigratedCount() > 0) {
                    MigrateBatchRequest deleteRequest =
                            MigrateBatchRequest.newBuilder()
                                    .setTableId(tableId)
                                    .setTargetShardId(shardId)
                                    .addAllKeys(batch.getKeysList())
                                    .setDeleteAfterMigrate(true)
                                    .build();
                    sourceStub.migrateBatch(deleteRequest);
                }

                log.debug(
                        "Batch {}: migrated={}, failed={}, hasMore={}",
                        batchCount,
                        writeResponse.getMigratedCount(),
                        writeResponse.getFailedCount(),
                        batch.getHasMore());
            }

            updateRoutingTable(shardId, targetExecutorId);

            log.info(
                    "Shard migration completed: tableId={}, shardId={}, migrated={}, failed={}, batches={}",
                    tableId,
                    shardId,
                    totalMigrated,
                    totalFailed,
                    batchCount);

            return MigrationResult.success(totalMigrated, totalFailed, batchCount);
        } catch (Exception e) {
            log.error(
                    "Shard migration failed: tableId={}, shardId={}, error={}",
                    tableId,
                    shardId,
                    e.getMessage(),
                    e);
            return MigrationResult.failed(e.getMessage());
        }
    }

    private void updateRoutingTable(int shardId, String targetExecutorId) {
        ShardId sid = new ShardId(shardId);
        ShardLookupResult current = routingCoordinator.lookupOwner(sid);
        RoutingEpoch newEpoch =
                current.assigned() ? current.assignment().epoch().next() : RoutingEpoch.initial();

        // Look up the target executor's registered NodeSlot — validateExecutorPlacement
        // requires the assignment's NodeSlot to match the executor's registered slot.
        Optional<NodeSlot> targetSlotOpt = routingTable.nodeSlotOf(targetExecutorId);
        if (targetSlotOpt.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot update routing table: target executor not registered: "
                            + targetExecutorId);
        }
        NodeSlot slot = targetSlotOpt.get();

        ShardAssignment newAssignment =
                new ShardAssignment(sid, slot, targetExecutorId, newEpoch, false);
        routingCoordinator.assignShard(newAssignment);
        log.info(
                "Routing table updated: shardId={} → executor={} slot={} epoch={}",
                shardId,
                targetExecutorId,
                slot,
                newEpoch.value());
    }

    public record MigrationResult(
            int migratedCount, int failedCount, int batchCount, boolean success, String error) {
        public static MigrationResult success(int migrated, int failed, int batches) {
            return new MigrationResult(migrated, failed, batches, true, "");
        }

        public static MigrationResult failed(String error) {
            return new MigrationResult(0, 0, 0, false, error);
        }
    }
}
