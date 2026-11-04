package io.github.rawwise.axiom.orchestrator;

public record ElectionHandle(
        String electionId,
        String leaderAgentId,
        long term,
        String fencingToken,
        long acquiredAt,
        long expiresAt) {}
