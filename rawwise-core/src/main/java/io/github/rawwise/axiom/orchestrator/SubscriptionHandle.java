package io.github.rawwise.axiom.orchestrator;

public record SubscriptionHandle(
        String subscriptionId, String topic, String agentId, long createdAt) {}
