package io.github.rawwise.benchmark.tpcds;

import io.github.rawwise.benchmark.BenchmarkConfig;
import io.github.rawwise.benchmark.BenchmarkSuite;
import io.github.rawwise.benchmark.LatencyStats;
import io.github.rawwise.benchmark.QueryBenchmarkResult;
import io.github.rawwise.client.repl.FrontendHttpClient;
import io.github.rawwise.frontend.api.dto.QueryResponseDto;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class TpcdsBenchmarkSuite implements BenchmarkSuite {

    private static final Logger LOG = Logger.getLogger(TpcdsBenchmarkSuite.class.getName());

    private final String frontendHost;
    private final int frontendPort;
    private final String authToken;
    private final TpcdsQuerySet querySet;
    private final TpcdsCorrectnessChecker correctnessChecker;

    public TpcdsBenchmarkSuite(String frontendHost, int frontendPort, TpcdsQuerySet querySet) {
        this(frontendHost, frontendPort, null, querySet, null);
    }

    public TpcdsBenchmarkSuite(
            String frontendHost, int frontendPort, String authToken, TpcdsQuerySet querySet) {
        this(frontendHost, frontendPort, authToken, querySet, null);
    }

    public TpcdsBenchmarkSuite(
            String frontendHost,
            int frontendPort,
            String authToken,
            TpcdsQuerySet querySet,
            TpcdsCorrectnessChecker correctnessChecker) {
        this.frontendHost = frontendHost;
        this.frontendPort = frontendPort;
        this.authToken = authToken;
        this.querySet = querySet;
        this.correctnessChecker = correctnessChecker;
    }

    @Override
    public List<QueryBenchmarkResult> run(BenchmarkConfig config) {
        try (FrontendHttpClient client = new FrontendHttpClient(frontendHost, frontendPort)) {
            if (!client.readyCheck()) {
                throw new RuntimeException(
                        "Frontend not ready at " + frontendHost + ":" + frontendPort);
            }

            List<QueryBenchmarkResult> results = new ArrayList<>();
            for (String queryId : config.queryIds()) {
                results.add(runQuery(client, queryId, config));
            }
            return results;
        }
    }

    private static final String TPCDS_DB = "tpcds";

    private QueryBenchmarkResult runQuery(
            FrontendHttpClient client, String queryId, BenchmarkConfig config) {
        String sql = querySet.getQuery(queryId, TPCDS_DB);

        for (int i = 0; i < config.warmupRounds(); i++) {
            int round = i + 1;
            LOG.fine(() -> "Warmup " + queryId + " round " + round);
            QueryResponseDto response = client.executeSync(sql, TPCDS_DB, config.timeoutSeconds());
            if (!"SUCCEEDED".equals(response.getState())) {
                LOG.warning("Warmup query " + queryId + " failed: " + response.getState());
            }
        }

        List<Long> latencies = new ArrayList<>();
        long totalRows = 0;
        String correctness = "SUCCEEDED";
        QueryResponseDto lastSuccessResponse = null;

        for (int i = 0; i < config.measureRounds(); i++) {
            int round = i + 1;
            LOG.fine(() -> "Measure " + queryId + " round " + round);
            long start = System.nanoTime();
            QueryResponseDto response = client.executeSync(sql, TPCDS_DB, config.timeoutSeconds());
            long elapsedNs = System.nanoTime() - start;
            long elapsedUs = elapsedNs / 1_000;

            if ("SUCCEEDED".equals(response.getState())) {
                latencies.add(elapsedUs);
                lastSuccessResponse = response;
                if (response.getData() != null && response.getData().getRowCount() != null) {
                    totalRows = response.getData().getRowCount();
                }
            } else {
                LOG.warning("Measure query " + queryId + " failed: " + response.getState());
                correctness = "QUERY_FAILED: " + response.getState();
            }
        }

        if ("SUCCEEDED".equals(correctness)
                && correctnessChecker != null
                && lastSuccessResponse != null) {
            correctness = correctnessChecker.check(queryId, lastSuccessResponse);
            LOG.info("Correctness check for " + queryId + ": " + correctness);
        }

        LatencyStats stats = computeStats(latencies);
        return new QueryBenchmarkResult(
                queryId,
                config.measureRounds(),
                stats,
                totalRows,
                correctness,
                "UNKNOWN",
                null,
                0,
                latencies.isEmpty()
                        ? 0L
                        : (long) latencies.stream().mapToLong(l -> l).average().orElse(0),
                config.engineType());
    }

    private static LatencyStats computeStats(List<Long> latencies) {
        if (latencies.isEmpty()) {
            return new LatencyStats(0, 0, 0);
        }
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        int n = sorted.size();
        double median = percentile(sorted, 50);
        double p95 = percentile(sorted, 95);
        double p99 = percentile(sorted, 99);
        return new LatencyStats(median, p95, p99);
    }

    private static double percentile(List<Long> sorted, double percentile) {
        int n = sorted.size();
        double index = percentile / 100.0 * (n - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) {
            return sorted.get(lower);
        }
        double weight = index - lower;
        return sorted.get(lower) * (1 - weight) + sorted.get(upper) * weight;
    }
}
