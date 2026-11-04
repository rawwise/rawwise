package io.github.rawwise.benchmark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record BenchmarkReport(BenchmarkConfig config, List<QueryBenchmarkResult> results) {

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .enable(SerializationFeature.INDENT_OUTPUT);

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize benchmark report", e);
        }
    }

    public static BenchmarkReport merge(List<BenchmarkReport> reports) {
        if (reports.isEmpty()) {
            throw new IllegalArgumentException("Cannot merge empty report list");
        }
        BenchmarkConfig config = reports.get(0).config();

        Map<String, List<QueryBenchmarkResult>> resultsByName = new LinkedHashMap<>();

        for (BenchmarkReport report : reports) {
            for (QueryBenchmarkResult r : report.results()) {
                resultsByName.computeIfAbsent(r.queryId(), k -> new ArrayList<>()).add(r);
            }
        }

        List<QueryBenchmarkResult> merged = new ArrayList<>();
        for (var entry : resultsByName.entrySet()) {
            String name = entry.getKey();
            List<QueryBenchmarkResult> group = entry.getValue();

            long totalRows = group.stream().mapToLong(QueryBenchmarkResult::rows).sum();
            double weightedMedian = weightedAvg(group, r -> r.latencyUs().median(), totalRows);
            double weightedP95 = weightedAvg(group, r -> r.latencyUs().p95(), totalRows);
            double weightedP99 = weightedAvg(group, r -> r.latencyUs().p99(), totalRows);
            LatencyStats stats = new LatencyStats(weightedMedian, weightedP95, weightedP99);

            double weightedAvgMs =
                    group.stream().mapToDouble(r -> r.executionOnlyMs() * r.rows()).sum()
                            / totalRows;

            merged.add(
                    new QueryBenchmarkResult(
                            name,
                            0,
                            stats,
                            totalRows,
                            "true",
                            "N/A",
                            null,
                            0,
                            (long) weightedAvgMs,
                            "ycsb"));
        }

        return new BenchmarkReport(config, merged);
    }

    private static double weightedAvg(
            List<QueryBenchmarkResult> group,
            java.util.function.ToDoubleFunction<QueryBenchmarkResult> fn,
            long totalRows) {
        if (totalRows == 0) return 0;
        return group.stream().mapToDouble(r -> fn.applyAsDouble(r) * r.rows()).sum() / totalRows;
    }
}
