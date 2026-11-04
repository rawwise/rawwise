package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.Collections;
import java.util.List;
import lombok.Builder;

@Builder
public record HybridSearchResponse(List<Hit> hits, LatencyBreakdown latencyBreakdown) {

    public HybridSearchResponse {
        hits = hits == null ? List.of() : Collections.unmodifiableList(hits);
    }

    @Builder
    public record Hit(String viewId, double score, String summary, Provenance provenance) {}

    @Builder
    public record Provenance(String rawKey, List<String> chain, String retrievalPath) {
        public Provenance {
            chain = chain == null ? List.of() : Collections.unmodifiableList(chain);
        }
    }

    @Builder
    public record LatencyBreakdown(
            long vectorMs, long keywordMs, long graphMs, long rrfMs, long totalMs) {}
}
