package io.github.rawwise.benchmark;

public record QueryBenchmarkResult(
        String queryId,
        int iterations,
        LatencyStats latencyUs,
        long rows,
        String correctness,
        String irPath,
        String fallbackReason,
        long irOverheadMs,
        long executionOnlyMs,
        String engineType) {}
