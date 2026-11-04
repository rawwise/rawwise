package io.github.rawwise.axiom.pipeline.reformulation;

public interface LlmClient extends AutoCloseable {

    String name();

    String complete(String prompt, String modelName, int maxTokens) throws LlmException;

    @Override
    default void close() {}
}
