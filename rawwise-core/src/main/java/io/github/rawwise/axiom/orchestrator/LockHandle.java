package io.github.rawwise.axiom.orchestrator;

public record LockHandle(
        String resource,
        String agentId,
        String fencingToken,
        long acquiredAt,
        long expiresAt,
        int holdCount) {}
