package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.Map;
import lombok.Builder;

@Builder
public record HybridSearchRequest(
        String tableId,
        String query,
        int topK,
        Map<String, Double> weights,
        Integer rrfK,
        Integer graphMaxHops,
        boolean multiQuery,
        String agentId,
        String memoryTier,
        Long snapshotId) {}
