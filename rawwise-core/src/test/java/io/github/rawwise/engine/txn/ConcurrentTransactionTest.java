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

@DisplayName("Concurrent Transaction Isolation E2E (Real LMDB)")
class ConcurrentTransactionTest {

    private static final String CATALOG = "concurrent_catalog";
    private static final String DATABASE = "concurrent_db";
    private static final String TABLE = "concurrent_table";
    private static boolean lmdbAvailable = true;

    @TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private LmdbInstance lmdbInstance;
    private TxnOrchestrator orchestrator;

    @BeforeAll
    static void checkLmdbAvailability() {
        Path probeDir = null;
        try {
            probeDir = Files.createTempDirectory("ct-lmdb-probe-" + UUID.randomUUID());
            LmdbInstance probe =
                    new LmdbInstance("probe-" + UUID.randomUUID(), probeDir, 1024L * 1024, 1);
            probe.close();
        } catch (Throwable t) {
            lmdbAvailable = false;
            System.err.println(
                    "[ConcurrentTransactionTest] LMDB not available, skipping: " + t.getMessage());
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
                        "ct-instance-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024, 5);

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
    @DisplayName("S2: Concurrency")
    class ConcurrencyScenarios {

        @Test
        @DisplayName(
                "3 concurrent writes to same key: only first succeeds, others get OCC conflict")
        void testConcurrentWritesToSameKeyOccConflict() throws Exception {
            byte[] key = bytes("occ-key");
            byte[] initialValue = bytes("initial-value");
            long initialCommitTs = commitInitialValue(key, initialValue);

            int threadCount = 3;
            CountDownLatch startLatch = new CountDownLatch(1);
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
                                    allDone.countDown();
                                    return;
                                }

                                byte[] newValue = bytes("thread-" + threadId + "-value");
                                orchestrator.recordWrite(
                                        ctx, TABLE, key, newValue, initialCommitTs);

                                if (firstSlot.compareAndSet(-1, threadId)) {
                                    TxnResult result = orchestrator.commit(ctx);
                                    if (result instanceof TxnResult.CommitOk) {
                                        successCount.incrementAndGet();
                                    } else {
                                        errorCount.incrementAndGet();
                                    }
                                    firstCommitDone.countDown();
                                } else {
                                    firstCommitDone.await(5, TimeUnit.SECONDS);
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
            assertThat(allDone.await(30, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            assertThat(errorCount.get()).isZero();
            assertThat(successCount.get()).isEqualTo(1);
            assertThat(conflictCount.get()).isEqualTo(2);
        }

        @Test
        @DisplayName("3 concurrent writes to different keys: all succeed")
        void testConcurrentWritesToDifferentKeysAllSucceed() throws Exception {
            int threadCount = 3;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch allDone = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
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
            assertThat(allDone.await(30, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            assertThat(errorCount.get()).isZero();
            assertThat(successCount.get()).isEqualTo(3);
        }

        @Test
        @DisplayName("Concurrent read during uncommitted write: no dirty read")
        void testConcurrentReadDuringWriteNoDirtyRead() {
            byte[] key = bytes("dirty-read-key");
            byte[] originalValue = bytes("original-value");

            commitInitialValue(key, originalValue);

            TxnContext writerCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            byte[] uncommittedValue = bytes("uncommitted-value");
            orchestrator.recordWrite(writerCtx, TABLE, key, uncommittedValue, 0);

            TxnContext readerCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> readResult = orchestrator.pointRead(TABLE, key, readerCtx);

            assertThat(readResult).isPresent();
            assertThat(readResult.get()).isEqualTo(originalValue);
            assertThat(readResult.get()).isNotEqualTo(uncommittedValue);

            orchestrator.abort(readerCtx);
            orchestrator.abort(writerCtx);
        }

        @Test
        @DisplayName("Concurrent read after commit: committed data is visible")
        void testConcurrentReadAfterCommitVisible() {
            byte[] key = bytes("visibility-key");
            byte[] value = bytes("committed-value");

            TxnContext writerCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(writerCtx, TABLE, key, value, 0);
            TxnResult writeResult = orchestrator.commit(writerCtx);
            assertThat(writeResult).isInstanceOf(TxnResult.CommitOk.class);

            TxnContext readerCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> readResult = orchestrator.pointRead(TABLE, key, readerCtx);

            assertThat(readResult).isPresent();
            assertThat(readResult.get()).isEqualTo(value);

            orchestrator.commit(readerCtx);
        }

        @Test
        @DisplayName(
                "SERIALIZABLE isolation: concurrent insert in range triggers conflict on commit")
        void testSerializableIsolationPhantomDetection() {
            byte[] k1 = bytes("k1");
            byte[] k2 = bytes("k2");
            byte[] k4 = bytes("k4");
            byte[] k5 = bytes("k5");

            TxnContext setupCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(setupCtx, TABLE, k1, bytes("v1"), 0);
            orchestrator.recordWrite(setupCtx, TABLE, k2, bytes("v2"), 0);
            orchestrator.recordWrite(setupCtx, TABLE, k4, bytes("v4"), 0);
            orchestrator.recordWrite(setupCtx, TABLE, k5, bytes("v5"), 0);
            assertThat(orchestrator.commit(setupCtx)).isInstanceOf(TxnResult.CommitOk.class);

            TxnContext txnA = orchestrator.begin(Set.of(TABLE), IsolationLevel.SERIALIZABLE);

            Optional<byte[]> k1Value = orchestrator.pointRead(TABLE, k1, txnA);
            assertThat(k1Value).isPresent();
            assertThat(k1Value.get()).isEqualTo(bytes("v1"));

            var rangeResults = orchestrator.rangeRead(TABLE, k1, k5, txnA);
            assertThat(rangeResults).hasSize(4);

            TxnContext txnB = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            byte[] k3 = bytes("k3");
            orchestrator.recordWrite(txnB, TABLE, k3, bytes("v3"), 0);
            TxnResult txnBResult = orchestrator.commit(txnB);
            assertThat(txnBResult).isInstanceOf(TxnResult.CommitOk.class);

            orchestrator.recordWrite(txnA, TABLE, k1, bytes("updated-v1"), 0);

            TxnResult txnAResult = orchestrator.commit(txnA);
            assertThat(txnAResult).isInstanceOf(TxnResult.CommitConflict.class);
        }
    }

    @Nested
    @DisplayName("S1: Happy Path")
    class HappyPathScenarios {

        @Test
        @DisplayName("Sequential transactions with read-your-writes semantics")
        void testSequentialTransactionsWithReadYourWrites() {
            byte[] key = bytes("ryw-key");
            byte[] value = bytes("ryw-value");

            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(ctx, TABLE, key, value, 0);

            Optional<byte[]> readResult = orchestrator.pointRead(TABLE, key, ctx);
            assertThat(readResult).isPresent();
            assertThat(readResult.get()).isEqualTo(value);

            TxnResult result = orchestrator.commit(ctx);
            assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
        }
    }

    @Nested
    @DisplayName("S3: Fault Recovery")
    class FaultRecoveryScenarios {

        @Test
        @DisplayName(
                "Uncommitted transaction auto-rollback on abort: data not visible to subsequent reads")
        void testUncommittedTransactionAutoRollbackOnAbort() {
            byte[] key = bytes("abort-key");
            byte[] value = bytes("abort-value");

            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(ctx, TABLE, key, value, 0);

            Optional<byte[]> selfRead = orchestrator.pointRead(TABLE, key, ctx);
            assertThat(selfRead).isPresent();
            assertThat(selfRead.get()).isEqualTo(value);

            TxnResult abortResult = orchestrator.abort(ctx);
            assertThat(abortResult).isInstanceOf(TxnResult.AbortResult.class);

            TxnContext readerCtx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> postAbortRead = orchestrator.pointRead(TABLE, key, readerCtx);
            assertThat(postAbortRead).isEmpty();

            orchestrator.commit(readerCtx);
        }
    }
}
