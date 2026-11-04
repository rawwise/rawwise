package io.github.rawwise.frontend.ir.routing;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public record RoutingPlan(
        UUID planId,
        Instant createdAt,
        String tableId,
        PlanType planType,
        List<PlanStage> stages,
        RoutingStats stats)
        implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    public enum PlanType {
        POINT_QUERY,
        BATCH_POINT_QUERY,
        FULL_SCAN,
        FALLBACK
    }

    public record PlanStage(
            int stageOrder, String description, int shardId, String executorId, StageStatus status)
            implements Serializable {}

    public enum StageStatus {
        PENDING,
        EXECUTING,
        COMPLETED,
        FAILED,
        SKIPPED
    }

    public static RoutingPlan pointQuery(
            String tableId, int shardId, String executorId, RoutingStats stats) {
        return new RoutingPlan(
                UUID.randomUUID(),
                Instant.now(),
                tableId,
                PlanType.POINT_QUERY,
                List.of(new PlanStage(0, "point-lookup", shardId, executorId, StageStatus.PENDING)),
                stats);
    }

    public static RoutingPlan batchPointQuery(
            String tableId, List<ShardBatch> batches, RoutingStats stats) {
        List<PlanStage> stages = new ArrayList<>();
        for (int i = 0; i < batches.size(); i++) {
            ShardBatch b = batches.get(i);
            stages.add(
                    new PlanStage(
                            i,
                            "batch-shard-" + b.shardId(),
                            b.shardId(),
                            b.executorId(),
                            StageStatus.PENDING));
        }
        return new RoutingPlan(
                UUID.randomUUID(),
                Instant.now(),
                tableId,
                PlanType.BATCH_POINT_QUERY,
                List.copyOf(stages),
                stats);
    }

    public static RoutingPlan fallback(String tableId, String reason) {
        RoutingStats stats = new RoutingStats(0, 0, 0, 0, false, reason);
        return new RoutingPlan(
                UUID.randomUUID(), Instant.now(), tableId, PlanType.FALLBACK, List.of(), stats);
    }

    public RoutingPlan withStageStatus(int stageOrder, StageStatus newStatus) {
        List<PlanStage> updated =
                stages.stream()
                        .map(
                                s ->
                                        s.stageOrder() == stageOrder
                                                ? new PlanStage(
                                                        s.stageOrder(),
                                                        s.description(),
                                                        s.shardId(),
                                                        s.executorId(),
                                                        newStatus)
                                                : s)
                        .toList();
        return new RoutingPlan(planId, createdAt, tableId, planType, updated, stats);
    }
}
