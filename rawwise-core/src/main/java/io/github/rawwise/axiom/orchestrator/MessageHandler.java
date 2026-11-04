package io.github.rawwise.axiom.orchestrator;

public interface MessageHandler {
    void onMessage(String topic, byte[] message);
}
