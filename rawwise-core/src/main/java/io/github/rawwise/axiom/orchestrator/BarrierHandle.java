package io.github.rawwise.axiom.orchestrator;

import java.util.List;

public record BarrierHandle(
        String barrierId,
        int parties,
        List<String> arrivedAgents,
        boolean triggered,
        long createdAt,
        long expiresAt) {
    public BarrierHandle {
        arrivedAgents = List.copyOf(arrivedAgents);
    }
}
