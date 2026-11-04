package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.axiom.pipeline.entity.EntityType;
import lombok.Builder;

@Builder
public record EntityCentroid(
        String entityId,
        EntityType entityType,
        String sampleId,
        ShardId shardId,
        long lastUpdatedEpochMs) {}
