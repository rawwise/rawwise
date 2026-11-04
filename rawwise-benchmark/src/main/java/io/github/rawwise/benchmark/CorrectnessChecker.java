package io.github.rawwise.benchmark;

public interface CorrectnessChecker {
    String check(String queryId, Object actual);
}
