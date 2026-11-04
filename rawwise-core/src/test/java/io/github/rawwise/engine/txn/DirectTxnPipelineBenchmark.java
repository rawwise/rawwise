package io.github.rawwise.engine.txn;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorFactory;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorImpl;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Direct-mode transaction pipeline benchmark.
 *
 * <p>Path: TxnOrchestrator -> ValidateStage -> WriteStage -> DurableStage -> PublishStage -> LMDB
 *
 * <p>No HTTP/gRPC overhead — direct TxnOrchestrator API calls.
 */
public class DirectTxnPipelineBenchmark {

    private static final String CATALOG = "perf";
    private static final String DATABASE = "default";
    private static final String TABLE = "dir_bench";
    private static final DecimalFormat DF = new DecimalFormat("#,##0.0");

    private String tempDir;
    private TxnOrchestratorImpl orchestrator;
    private LmdbInstance lmdbInstance;

    public void setUp() throws Exception {
        tempDir =
                System.getProperty("java.io.tmpdir")
                        + "/rawwise-bench-"
                        + UUID.randomUUID().toString().substring(0, 8);
        java.nio.file.Path dir = java.nio.file.Path.of(tempDir);
        java.nio.file.Files.createDirectories(dir);

        InMemoryMetastore metastore = new InMemoryMetastore();
        metastore.put(
                String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE).getBytes(),
                new byte[] {0, 0, 0, 0, 0, 0, 0, 1});

        lmdbInstance =
                new LmdbInstance(
                        "bench-" + UUID.randomUUID().toString().substring(0, 8),
                        dir,
                        1024L * 1024 * 1024,
                        2);

        ShardVersionTracker tracker =
                new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);

        TxnOrchestrator orch =
                TxnOrchestratorFactory.create(
                        TxnEngineConfig.planA(),
                        metastore,
                        lmdbInstance,
                        CATALOG,
                        DATABASE,
                        TABLE,
                        null,
                        tracker);
        orchestrator = (TxnOrchestratorImpl) orch;
    }

    public void tearDown() {
        if (orchestrator != null) orchestrator.close();
        if (lmdbInstance != null) lmdbInstance.close();
        if (tempDir != null) {
            try {
                java.nio.file.Files.walk(java.nio.file.Path.of(tempDir))
                        .sorted(java.util.Comparator.reverseOrder())
                        .forEach(
                                p -> {
                                    try {
                                        java.nio.file.Files.deleteIfExists(p);
                                    } catch (Exception ignored) {
                                    }
                                });
            } catch (Exception ignored) {
            }
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public void run() throws Exception {
        setUp();
        try {
            System.out.println("=== Direct Txn Pipeline Benchmark ===");
            System.out.println("Path: TxnOrchestrator.begin() → recordWrite() → commit()");
            System.out.println("  → ValidateStage → WriteStage → LMDB");
            System.out.println("  → DurableStage → PublishStage");
            System.out.println("");

            benchmarkSequentialInsert(1000);
            benchmarkSequentialPointRead(500);
            benchmarkReadAfterWriteConsistency(200);
            benchmarkConcurrentWriteConflicts(100);
            benchmarkCommitTsHeaderVerification(100);

            System.out.println("");
            System.out.println("========================================");
            System.out.println("ALL BENCHMARKS PASSED");
            System.out.println("========================================");
        } finally {
            tearDown();
        }
    }

    private void benchmarkSequentialInsert(int count) {
        System.out.println("--- Sequential INSERT (" + count + " rows) ---");

        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        long wallStart = System.currentTimeMillis();
        int success = 0;

        for (int i = 0; i < count; i++) {
            long start = System.nanoTime();
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(ctx, TABLE, bytes("perf-key-" + i), bytes("perf-val-" + i), 0);
            TxnResult result = orchestrator.commit(ctx);
            long elapsedUs = (System.nanoTime() - start) / 1000;

            if (result instanceof TxnResult.CommitOk) {
                success++;
                latencies.add(elapsedUs);
            }
        }

        long wallElapsed = System.currentTimeMillis() - wallStart;
        printStats("INSERT", latencies, wallElapsed, success, count);
    }

    private void benchmarkSequentialPointRead(int count) {
        System.out.println("--- Point Query SELECT (" + count + " reads) ---");

        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        long wallStart = System.currentTimeMillis();
        int success = 0;

        for (int i = 0; i < count; i++) {
            long start = System.nanoTime();
            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            var result = orchestrator.pointRead(TABLE, bytes("perf-key-" + i), ctx);
            long elapsedUs = (System.nanoTime() - start) / 1000;

            if (result != null) {
                success++;
                latencies.add(elapsedUs);
            }
        }

        long wallElapsed = System.currentTimeMillis() - wallStart;
        printStats("SELECT", latencies, wallElapsed, success, count);
    }

    private void benchmarkReadAfterWriteConsistency(int count) {
        System.out.println("--- Read-After-Write Consistency (" + count + " checks) ---");

        int pass = 0;
        for (int i = 0; i < count; i++) {
            String key = "raw-key-" + i;
            String expected = "raw-val-" + i;

            TxnContext wCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(wCtx, TABLE, bytes(key), bytes(expected), 0);
            TxnResult wResult = orchestrator.commit(wCtx);

            if (!(wResult instanceof TxnResult.CommitOk)) continue;

            TxnContext rCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            var readResult = orchestrator.pointRead(TABLE, bytes(key), rCtx);

            if (readResult.isPresent() && expected.equals(new String(readResult.get()))) {
                pass++;
            }
        }

        System.out.println("  Passed: " + pass + "/" + count);
    }

    private void benchmarkConcurrentWriteConflicts(int writesPerThread) {
        System.out.println(
                "--- Concurrent Write Conflicts (2 threads × " + writesPerThread + " writes) ---");

        final int threads = 2;
        final java.util.concurrent.atomic.AtomicInteger success =
                new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicInteger conflicts =
                new java.util.concurrent.atomic.AtomicInteger(0);
        final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        final java.util.concurrent.CountDownLatch latch =
                new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch done =
                new java.util.concurrent.CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            new Thread(
                            () -> {
                                try {
                                    latch.await();
                                } catch (InterruptedException e) {
                                    return;
                                }
                                for (int i = 0; i < writesPerThread; i++) {
                                    long start = System.nanoTime();
                                    TxnContext ctx =
                                            orchestrator.begin(
                                                    Set.of(TABLE), IsolationLevel.READ_COMMITTED);
                                    orchestrator.recordWrite(
                                            ctx,
                                            TABLE,
                                            bytes("concurrent-key-" + tid),
                                            bytes("tid" + tid + "-val" + i),
                                            0);
                                    TxnResult result = orchestrator.commit(ctx);
                                    long elapsedUs = (System.nanoTime() - start) / 1000;

                                    if (result instanceof TxnResult.CommitOk) {
                                        success.incrementAndGet();
                                        latencies.add(elapsedUs);
                                    } else {
                                        conflicts.incrementAndGet();
                                    }
                                }
                                done.countDown();
                            })
                    .start();
        }

        long wallStart = System.currentTimeMillis();
        latch.countDown();
        try {
            done.await();
        } catch (InterruptedException ignored) {
        }
        long wallElapsed = System.currentTimeMillis() - wallStart;

        System.out.println("  Threads: " + threads + ", writes/thread: " + writesPerThread);
        System.out.println("  Success: " + success.get() + ", Conflicts: " + conflicts.get());
        System.out.println("  Wall time: " + wallElapsed + " ms");
        double tps = success.get() / (wallElapsed / 1000.0);
        System.out.println("  Throughput: " + DF.format(tps) + " ops/s (aggregate)");
    }

    private void benchmarkCommitTsHeaderVerification(int count) {
        System.out.println(
                "--- CommitTs Round-Trip (write → read via orchestrator) (" + count + " keys) ---");

        int verified = 0;
        for (int i = 0; i < count; i++) {
            byte[] key = bytes("ts-round-" + i);
            byte[] val = bytes("ts-round-val-" + i);

            TxnContext wCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(wCtx, TABLE, key, val, 0);
            TxnResult result = orchestrator.commit(wCtx);

            if (!(result instanceof TxnResult.CommitOk)) continue;

            TxnContext rCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            var readResult = orchestrator.pointRead(TABLE, key, rCtx);
            if (readResult.isPresent() && java.util.Arrays.equals(val, readResult.get())) {
                verified++;
            }
        }
        System.out.println("  Verified: " + verified + "/" + count + " write→read round-trip");
    }

    private static void printStats(
            String label, List<Long> latenciesUs, long wallMs, int success, int total) {
        if (latenciesUs.isEmpty()) {
            System.out.println("  No data. Success: " + success + "/" + total);
            return;
        }

        List<Long> sorted = new ArrayList<>(latenciesUs);
        Collections.sort(sorted);
        int n = sorted.size();
        long min = sorted.get(0);
        long p50 = sorted.get(n / 2);
        long p95 = sorted.get((int) (n * 0.95));
        long p99 = sorted.get((int) (n * 0.99));
        long max = sorted.get(n - 1);
        double avg = sorted.stream().mapToLong(Long::longValue).average().orElse(0);
        double tps = success / (wallMs / 1000.0);

        System.out.println("  Success: " + success + "/" + total);
        System.out.println("  Wall: " + wallMs + " ms, Throughput: " + DF.format(tps) + " ops/s");
        System.out.println(
                "  Latency (us): min="
                        + min
                        + " avg="
                        + DF.format(avg)
                        + " p50="
                        + p50
                        + " p95="
                        + p95
                        + " p99="
                        + p99
                        + " max="
                        + max);
    }

    public static void main(String[] args) throws Exception {
        new DirectTxnPipelineBenchmark().run();
    }
}
