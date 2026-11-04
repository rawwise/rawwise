package io.github.rawwise.axiom.memory.semantic;

public record ExtractionResult(
        String agentId,
        String graphId,
        int nodesExtracted,
        int edgesExtracted,
        int entriesProcessed,
        String lastProcessedCursor,
        long elapsedMs) {}
