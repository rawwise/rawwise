package io.github.rawwise.axiom.orchestrator;

public record LeaseHandle(
        String leaseId,
        String agentId,
        String resourceId,
        String fencingToken,
        long acquiredAt,
        long expiresAt,
        long renewalCount,
        long originalTtlMs) {}
