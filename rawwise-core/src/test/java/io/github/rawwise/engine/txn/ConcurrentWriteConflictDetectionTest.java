package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorFactory;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorImpl;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Concurrent Write Conflict Detection (Plan A v10) with Real LMDB")
class ConcurrentWriteConflictDetectionTest {

    private static final String CATALOG = "conflict_catalog";
    private static final String DATABASE = "conflict_db";
    private static final String TABLE = "conflict_table";
    private static boolean lmdbAvailable = true;

    @TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private LmdbInstance lmdbInstance;
    private TxnOrchestrator orchestrator;

    @BeforeAll
    static void checkLmdbAvailability() {
        Path probeDir = null;
        try {
            probeDir = Files.createTempDirectory("lmdb-probe-" + UUID.randomUUID());
            LmdbInstance probe =
                    new LmdbInstance("probe-" + UUID.randomUUID(), probeDir, 1024L * 1024, 1);
            probe.close();
        } catch (Throwable t) {
            lmdbAvailable = false;
            System.err.println(
                    "[ConcurrentWriteConflictDetectionTest] LMDB native library not available, "
                            + "skipping all tests: "
                            + t.getMessage());
        } finally {
            if (probeDir != null) {
                try (var stream = Files.walk(probeDir)) {
                    stream.sorted(Comparator.reverseOrder())
                            .forEach(
                                    p -> {
                                        try {
                                            Files.deleteIfExists(p);
                                        } catch (Exception ignored) {
                                        }
                                    });
                } catch (Exception ignored) {
                }
            }
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        assumeTrue(lmdbAvailable, "LMDB native library not available on this platform");

        metastore = new InMemoryMetastore();
        String clockKey = String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE);
        byte[] zeroBytes = ByteBuffer.allocate(Long.BYTES).putLong(0L).array();
        metastore.put(clockKey.getBytes(StandardCharsets.UTF_8), zeroBytes);

        lmdbInstance =
                new LmdbInstance(
                        "test-instance-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024, 5);

        orchestrator =
                TxnOrchestratorFactory.create(
                        TxnEngineConfig.planA(), metastore, lmdbInstance, CATALOG, DATABASE, TABLE);
    }

    @AfterEach
    void tearDown() {
        if (orchestrator instanceof TxnOrchestratorImpl impl) {
            impl.close();
        }
        if (lmdbInstance != null) {
            lmdbInstance.close();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private long commitInitialValue(byte[] key, byte[] value) {
        TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(ctx, TABLE, key, value, 0);
        TxnResult result = orchestrator.commit(ctx);
        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
        return ((TxnResult.CommitOk) result).commitTs();
    }

    @Nested
    @DisplayName("Scenario A: Write-Write Conflict (Core OCC)")
    class WriteWriteConflictScenario {

        @Test
        @DisplayName(
                "T2 commits first -> T1 gets CommitConflict with OCC conflict reason; T1 write does not take effect")
        void testWriteWriteConflictT2FirstThenT1Conflict() {
            byte[] key = bytes("K1");
            byte[] initialValue = bytes("initial");

            long setupCommitTs = commitInitialValue(key, initialValue);

            TxnContext t1Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> t1Read = orchestrator.pointRead(TABLE, key, t1Ctx);
            assertThat(t1Read).isPresent();
            assertThat(t1Read.get()).isEqualTo(initialValue);

            TxnContext t2Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> t2Read = orchestrator.pointRead(TABLE, key, t2Ctx);
            assertThat(t2Read).isPresent();
            assertThat(t2Read.get()).isEqualTo(initialValue);

            byte[] val2 = bytes("val2");
            orchestrator.recordWrite(t2Ctx, TABLE, key, val2, setupCommitTs);
            TxnResult t2Result = orchestrator.commit(t2Ctx);
            assertThat(t2Result).isInstanceOf(TxnResult.CommitOk.class);

            byte[] val1 = bytes("val1");
            orchestrator.recordWrite(t1Ctx, TABLE, key, val1, setupCommitTs);
            TxnResult t1Result = orchestrator.commit(t1Ctx);
            assertThat(t1Result).isInstanceOf(TxnResult.CommitConflict.class);
            assertThat(((TxnResult.CommitConflict) t1Result).reason()).contains("OCC conflict");

            TxnContext verifyCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> verifyRead = orchestrator.pointRead(TABLE, key, verifyCtx);
            assertThat(verifyRead).isPresent();
            assertThat(verifyRead.get()).isEqualTo(val2);
            orchestrator.commit(verifyCtx);
        }
    }

    @Nested
    @DisplayName("Scenario B: Multi-thread Concurrent Write Same Key")
    class MultiThreadSameKeyScenario {

        @Test
        @DisplayName("3 threads write same key: only 1 succeeds, 2 get OCC conflict")
        void testThreeThreadsWriteSameKeyOnlyOneSucceeds() throws Exception {
            byte[] key = bytes("K1");
            byte[] initialValue = bytes("initial");

            long setupCommitTs = commitInitialValue(key, initialValue);

            int threadCount = 3;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch allReadDone = new CountDownLatch(threadCount);
            CountDownLatch firstCommitDone = new CountDownLatch(1);
            CountDownLatch allDone = new CountDownLatch(threadCount);
            AtomicInteger firstSlot = new AtomicInteger(-1);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger conflictCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(
                        () -> {
                            try {
                                startLatch.await();

                                TxnContext ctx =
                                        orchestrator.begin(
                                                Set.of(TABLE), IsolationLevel.READ_COMMITTED);
                                Optional<byte[]> readValue =
                                        orchestrator.pointRead(TABLE, key, ctx);
                                if (!readValue.isPresent()
                                        || !Arrays.equals(readValue.get(), initialValue)) {
                                    errorCount.incrementAndGet();
                                    allReadDone.countDown();
                                    allDone.countDown();
                                    return;
                                }

                                byte[] newValue = bytes("thread-" + threadId + "-value");
                                orchestrator.recordWrite(ctx, TABLE, key, newValue, setupCommitTs);

                                allReadDone.countDown();
                                allReadDone.await(10, TimeUnit.SECONDS);

                                if (firstSlot.compareAndSet(-1, threadId)) {
                                    TxnResult result = orchestrator.commit(ctx);
                                    if (result instanceof TxnResult.CommitOk) {
                                        successCount.incrementAndGet();
                                    } else {
                                        errorCount.incrementAndGet();
                                    }
                                    firstCommitDone.countDown();
                                } else {
                                    firstCommitDone.await(10, TimeUnit.SECONDS);
                                    TxnResult result = orchestrator.commit(ctx);
                                    if (result instanceof TxnResult.CommitConflict) {
                                        conflictCount.incrementAndGet();
                                    } else if (result instanceof TxnResult.CommitOk) {
                                        successCount.incrementAndGet();
                                    } else {
                                        errorCount.incrementAndGet();
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                errorCount.incrementAndGet();
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                            } finally {
                                allDone.countDown();
                            }
                        });
            }

            startLatch.countDown();
            assertThat(allDone.await(30, TimeUnit.SECONDS))
                    .as("All threads should complete within 30 seconds")
                    .isTrue();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            assertThat(errorCount.get())
                    .as("No thread should encounter unexpected errors")
                    .isZero();
            assertThat(successCount.get())
                    .as("Exactly one thread should commit successfully")
                    .isEqualTo(1);
            assertThat(conflictCount.get())
                    .as("Exactly two threads should get OCC conflict")
                    .isEqualTo(2);

            int winnerId = firstSlot.get();
            byte[] expectedWinnerValue = bytes("thread-" + winnerId + "-value");

            TxnContext verifyCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> verifyRead = orchestrator.pointRead(TABLE, key, verifyCtx);
            assertThat(verifyRead).isPresent();
            assertThat(verifyRead.get()).isEqualTo(expectedWinnerValue);
            orchestrator.commit(verifyCtx);
        }
    }

    @Nested
    @DisplayName("Scenario C: Concurrent Write Different Keys (No Conflict)")
    class DifferentKeysScenario {

        @Test
        @DisplayName("3 threads write different keys: all 3 succeed, 0 conflicts")
        void testThreeThreadsWriteDifferentKeysAllSucceed() throws Exception {
            int threadCount = 3;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch allDone = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger conflictCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(
                        () -> {
                            try {
                                startLatch.await();

                                TxnContext ctx =
                                        orchestrator.begin(
                                                Set.of(TABLE), IsolationLevel.READ_COMMITTED);
                                byte[] key = bytes("diff-key-" + threadId);
                                byte[] value = bytes("diff-value-" + threadId);
                                orchestrator.recordWrite(ctx, TABLE, key, value, 0);

                                TxnResult result = orchestrator.commit(ctx);
                                if (result instanceof TxnResult.CommitOk) {
                                    successCount.incrementAndGet();
                                } else if (result instanceof TxnResult.CommitConflict) {
                                    conflictCount.incrementAndGet();
                                } else {
                                    errorCount.incrementAndGet();
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                errorCount.incrementAndGet();
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                            } finally {
                                allDone.countDown();
                            }
                        });
            }

            startLatch.countDown();
            assertThat(allDone.await(30, TimeUnit.SECONDS))
                    .as("All threads should complete within 30 seconds")
                    .isTrue();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            assertThat(errorCount.get()).isZero();
            assertThat(successCount.get()).isEqualTo(3);
            assertThat(conflictCount.get()).isEqualTo(0);

            for (int t = 0; t < threadCount; t++) {
                byte[] key = bytes("diff-key-" + t);
                byte[] expectedValue = bytes("diff-value-" + t);
                TxnContext verifyCtx =
                        orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
                Optional<byte[]> verifyRead = orchestrator.pointRead(TABLE, key, verifyCtx);
                assertThat(verifyRead).isPresent();
                assertThat(verifyRead.get()).isEqualTo(expectedValue);
                orchestrator.commit(verifyCtx);
            }
        }
    }

    @Nested
    @DisplayName("Scenario D: Serial Transactions (Read-After-Commit)")
    class SerialTransactionsScenario {

        @Test
        @DisplayName("T1 commits v1 -> T2 reads v1 -> T2 commits v2: both CommitOk")
        void testSerialTransactionsReadAfterCommit() {
            byte[] key = bytes("K1");
            byte[] v1 = bytes("v1");
            byte[] v2 = bytes("v2");

            TxnContext t1Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(t1Ctx, TABLE, key, v1, 0);
            TxnResult t1Result = orchestrator.commit(t1Ctx);
            assertThat(t1Result).isInstanceOf(TxnResult.CommitOk.class);
            long t1CommitTs = ((TxnResult.CommitOk) t1Result).commitTs();

            TxnContext t2Ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> t2Read = orchestrator.pointRead(TABLE, key, t2Ctx);
            assertThat(t2Read).isPresent();
            assertThat(t2Read.get()).isEqualTo(v1);

            orchestrator.recordWrite(t2Ctx, TABLE, key, v2, t1CommitTs);
            TxnResult t2Result = orchestrator.commit(t2Ctx);
            assertThat(t2Result).isInstanceOf(TxnResult.CommitOk.class);

            TxnContext verifyCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> verifyRead = orchestrator.pointRead(TABLE, key, verifyCtx);
            assertThat(verifyRead).isPresent();
            assertThat(verifyRead.get()).isEqualTo(v2);
            orchestrator.commit(verifyCtx);
        }
    }
}
