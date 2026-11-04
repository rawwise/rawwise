package io.github.rawwise.axiom.memory;

import java.util.Map;

public record MemoryEntry(
        String entryId,
        String agentId,
        MemoryTier memoryTier,
        Depth depth,
        Map<String, Object> content,
        double importance,
        double decayFactor,
        long createdAt,
        long updatedAt,
        Double relevanceScore) {
    public MemoryEntry {
        content = content == null ? Map.of() : Map.copyOf(content);
    }

    /**
     * Backward-compatible 9-arg constructor for callers that don't carry a cosine relevance score
     * (store, list Entries, reflect). The recall path uses the 10-arg form to expose JVector's real
     * cosine similarity, so downstream HTTP clients can distinguish recall ranking from importance.
     */
    public MemoryEntry(
            String entryId,
            String agentId,
            MemoryTier memoryTier,
            Depth depth,
            Map<String, Object> content,
            double importance,
            double decayFactor,
            long createdAt,
            long updatedAt) {
        this(
                entryId,
                agentId,
                memoryTier,
                depth,
                content,
                importance,
                decayFactor,
                createdAt,
                updatedAt,
                null);
    }
}
