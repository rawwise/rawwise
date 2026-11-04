package io.github.rawwise.benchmark;

import java.util.Set;

public record BenchmarkConfig(
        int scaleFactor,
        String engineType,
        Set<String> queryIds,
        int warmupRounds,
        int measureRounds,
        int timeoutSeconds,
        int threads) {

    public BenchmarkConfig(
            int scaleFactor,
            String engineType,
            Set<String> queryIds,
            int warmupRounds,
            int measureRounds,
            int timeoutSeconds) {
        this(scaleFactor, engineType, queryIds, warmupRounds, measureRounds, timeoutSeconds, 1);
    }
}
