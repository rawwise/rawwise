package io.github.rawwise.benchmark;

import java.util.List;

public interface BenchmarkSuite {
    List<QueryBenchmarkResult> run(BenchmarkConfig config);
}
