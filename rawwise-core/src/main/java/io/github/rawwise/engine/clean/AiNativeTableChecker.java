package io.github.rawwise.engine.clean;

@FunctionalInterface
public interface AiNativeTableChecker {
    boolean isAiNative(String tableIdentifier);
}
