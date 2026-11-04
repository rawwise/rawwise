package io.github.rawwise.axiom.orchestrator;

public record PubSubMessage(
        String msgId, String topic, String publisherAgentId, long timestamp, byte[] payload) {}
