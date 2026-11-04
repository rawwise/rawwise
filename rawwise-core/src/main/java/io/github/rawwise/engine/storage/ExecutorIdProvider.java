package io.github.rawwise.engine.storage;

public interface ExecutorIdProvider {
    String resolveExecutorId();

    String resolveNodeId();
}
