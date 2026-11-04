package io.github.rawwise.benchmark.chbenchmark;

import io.github.rawwise.benchmark.BenchmarkConfig;
import io.github.rawwise.benchmark.QueryBenchmarkResult;
import java.util.List;
import java.util.Set;

/**
 * CH-benCHmark standalone test launcher(for quick verification, no Main class needed).
 *
 * <pre>{@code
 * java -cp rawwise-benchmark.jar io.github.rawwise.benchmark.chbenchmark.ChBenchmarkSuiteTest
 * localhost 8080 1 2 10 30
 * }</pre>
 */
public class ChBenchmarkSuiteTest {

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        int warehouses = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        int threads = args.length > 3 ? Integer.parseInt(args[3]) : 2;
        int warmupSecs = args.length > 4 ? Integer.parseInt(args[4]) : 10;
        int measureSecs = args.length > 5 ? Integer.parseInt(args[5]) : 30;
        int clientId = args.length > 6 ? Integer.parseInt(args[6]) : 0;
        int totalClients = args.length > 7 ? Integer.parseInt(args[7]) : 1;

        ChBenchmarkSuite suite =
                new ChBenchmarkSuite(
                        host, port, totalClients > 1, clientId, totalClients, null, false, 120);
        BenchmarkConfig config =
                new BenchmarkConfig(
                        warehouses,
                        "chbenchmark",
                        Set.of("all"),
                        warmupSecs,
                        measureSecs,
                        120,
                        threads);

        System.out.println(
                "Starting CH-benCHmark: host="
                        + host
                        + ":"
                        + port
                        + " warehouses="
                        + warehouses
                        + " threads="
                        + threads
                        + " warmup="
                        + warmupSecs
                        + "s measure="
                        + measureSecs
                        + "s");

        long start = System.currentTimeMillis();
        List<QueryBenchmarkResult> results = suite.run(config);
        long elapsed = System.currentTimeMillis() - start;

        for (QueryBenchmarkResult r : results) {
            System.out.println(
                    "RESULT: "
                            + r.queryId()
                            + " iterations="
                            + r.iterations()
                            + " p50="
                            + r.latencyUs().median()
                            + "us"
                            + " p95="
                            + r.latencyUs().p95()
                            + "us"
                            + " p99="
                            + r.latencyUs().p99()
                            + "us"
                            + " correctness="
                            + r.correctness());
        }
        System.out.println("Total elapsed: " + (elapsed / 1000.0) + "s");
    }
}
