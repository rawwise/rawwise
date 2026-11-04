package io.github.rawwise.axiom.memory;

public record MemoryStatus(
        String agentId,
        int workingCount,
        int episodicCount,
        int semanticCount,
        int proceduralCount,
        int totalCount) {}
