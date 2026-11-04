package io.github.rawwise.engine.txn.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("CommitLedger and GlobalCommitMarker lifecycle E2E test")
class CommitLedgerLifecycleTest {

    private Metastore metastore;
    private CommitLedgerStore ledgerStore;
    private GlobalCommitMarkerStore markerStore;

    private static final String TABLE_ID = "test_db.test_table";
    private static final String CATALOG = "test_catalog";
    private static final String DATABASE = "test_db";
    private static final String TABLE = "test_table";

    @BeforeEach
    void setUp() throws IOException {
        metastore = new InMemoryMetastore();
        ledgerStore = new MetastoreCommitLedgerStore(metastore);
        markerStore = new MetastoreGlobalCommitMarkerStore(metastore);
    }

    private CommitLedger createOpenLedger(
            String idempotencyKey, String batchId, int expectedShardCount) {
        LeaseInfo lease = new LeaseInfo("driver-1", System.currentTimeMillis() + 30000L, 1L);
        return CommitLedger.open(
                idempotencyKey, batchId, TABLE_ID, expectedShardCount, 100L, lease);
    }

    private GlobalCommitMarker createCommittedMarker(String batchId, String idempotencyKey) {
        WriterInfo writer = new WriterInfo("writer-1", "node-1", "attempt-1");
        long now = System.currentTimeMillis();
        return new GlobalCommitMarker(
                "v1",
                CATALOG,
                DATABASE,
                TABLE,
                batchId,
                1L,
                200L,
                1L,
                1,
                MarkerState.COMMITTED,
                now,
                now,
                writer,
                List.of(),
                "manifest-digest-1234",
                idempotencyKey,
                0L);
    }

    private CommitLedger advanceToState(String idempotencyKey, BatchState target)
            throws IOException {
        CommitLedger ledger = createOpenLedger(idempotencyKey, "batch-" + idempotencyKey, 2);
        assertThat(ledgerStore.save(ledger)).isTrue();
        BatchState[] path = {
            BatchState.SHARDS_PREPARED,
            BatchState.DATA_DURABLE,
            BatchState.GLOBAL_MARKER_COMMITTED,
            BatchState.PUBLISHING,
            BatchState.COMMITTED,
            BatchState.ACKED
        };
        CommitLedger current = ledger;
        for (BatchState s : path) {
            if (current.state() == target) {
                break;
            }
            current = ledgerStore.updateState(idempotencyKey, s);
        }
        return current;
    }

    @Test
    @DisplayName(
            "S1: CommitLedger lifecycle OPEN->SHARDS_PREPARED->DATA_DURABLE->GLOBAL_MARKER_COMMITTED->PUBLISHING->COMMITTED->ACKED")
    void testCommitLedgerFullLifecycle() throws IOException {
        CommitLedger ledger = createOpenLedger("idem-full", "batch-full", 3);
        assertThat(ledger.state()).isEqualTo(BatchState.OPEN);
        assertThat(ledger.expectedShardCount()).isEqualTo(3);
        assertThat(ledger.preparedShardCount()).isZero();
        assertThat(ledger.publishedShardCount()).isZero();

        assertThat(ledgerStore.save(ledger)).isTrue();

        CommitLedger loaded = ledgerStore.load("idem-full").orElseThrow();
        assertThat(loaded.state()).isEqualTo(BatchState.OPEN);
        assertThat(loaded.idempotencyKey()).isEqualTo("idem-full");
        assertThat(loaded.batchId()).isEqualTo("batch-full");
        assertThat(loaded.tableId()).isEqualTo(TABLE_ID);

        CommitLedger sp = ledgerStore.updateState("idem-full", BatchState.SHARDS_PREPARED);
        assertThat(sp.state()).isEqualTo(BatchState.SHARDS_PREPARED);

        CommitLedger dd = ledgerStore.updateState("idem-full", BatchState.DATA_DURABLE);
        assertThat(dd.state()).isEqualTo(BatchState.DATA_DURABLE);

        CommitLedger gmc = ledgerStore.updateState("idem-full", BatchState.GLOBAL_MARKER_COMMITTED);
        assertThat(gmc.state()).isEqualTo(BatchState.GLOBAL_MARKER_COMMITTED);
        assertThat(gmc.state().isPastPointOfNoReturn()).isTrue();

        CommitLedger pub = ledgerStore.updateState("idem-full", BatchState.PUBLISHING);
        assertThat(pub.state()).isEqualTo(BatchState.PUBLISHING);

        CommitLedger committed = ledgerStore.updateState("idem-full", BatchState.COMMITTED);
        assertThat(committed.state()).isEqualTo(BatchState.COMMITTED);
        assertThat(committed.state().isReadable()).isTrue();

        CommitLedger acked = ledgerStore.updateState("idem-full", BatchState.ACKED);
        assertThat(acked.state()).isEqualTo(BatchState.ACKED);
        assertThat(acked.state().isTerminal()).isTrue();

        CommitLedger finalLoaded = ledgerStore.load("idem-full").orElseThrow();
        assertThat(finalLoaded.state()).isEqualTo(BatchState.ACKED);
        assertThat(finalLoaded.createdAtMs()).isEqualTo(ledger.createdAtMs());
        assertThat(finalLoaded.updatedAtMs()).isGreaterThanOrEqualTo(ledger.createdAtMs());
    }

    @Test
    @DisplayName("S1: GlobalCommitMarker createandload, verify CAS NotExists andfield")
    void testGlobalCommitMarkerCreateAndLoad() throws IOException {
        GlobalCommitMarker marker = createCommittedMarker("batch-marker-1", "idem-marker-1");

        assertThat(markerStore.create(marker)).isTrue();

        GlobalCommitMarker loaded = markerStore.load("batch-marker-1").orElseThrow();
        assertThat(loaded.batchId()).isEqualTo("batch-marker-1");
        assertThat(loaded.catalog()).isEqualTo(CATALOG);
        assertThat(loaded.database()).isEqualTo(DATABASE);
        assertThat(loaded.table()).isEqualTo(TABLE);
        assertThat(loaded.txnId()).isEqualTo(1L);
        assertThat(loaded.commitTs()).isEqualTo(200L);
        assertThat(loaded.snapshotId()).isEqualTo(1L);
        assertThat(loaded.schemaVersion()).isEqualTo(1);
        assertThat(loaded.state()).isEqualTo(MarkerState.COMMITTED);
        assertThat(loaded.isCommitted()).isTrue();
        assertThat(loaded.manifestDigest()).isEqualTo("manifest-digest-1234");
        assertThat(loaded.idempotencyKey()).isEqualTo("idem-marker-1");
        assertThat(loaded.previousSnapshotId()).isZero();
        assertThat(loaded.writer().writerId()).isEqualTo("writer-1");
        assertThat(loaded.writer().nodeId()).isEqualTo("node-1");
        assertThat(loaded.writer().attemptId()).isEqualTo("attempt-1");
        assertThat(loaded.tableId()).isEqualTo(CATALOG + "." + DATABASE + "." + TABLE);

        List<GlobalCommitMarker> byTable = markerStore.loadByTable(CATALOG, DATABASE, TABLE);
        assertThat(byTable).hasSize(1);
        assertThat(byTable.get(0).batchId()).isEqualTo("batch-marker-1");
    }

    @Test
    @DisplayName(
            "S1: CommitLedger idempotent — duplicate save same idempotencyKey No.secondary false")
    void testCommitLedgerIdempotentSave() throws IOException {
        CommitLedger ledger = createOpenLedger("idem-dup", "batch-dup", 2);

        assertThat(ledgerStore.save(ledger)).isTrue();

        assertThat(ledgerStore.save(ledger)).isFalse();

        Optional<CommitLedger> loaded = ledgerStore.load("idem-dup");
        assertThat(loaded).isPresent();
        assertThat(loaded.get().state()).isEqualTo(BatchState.OPEN);
    }

    @Test
    @DisplayName("S2: concurrent CommitLedger — 2 same save same idempotencyKey, itemsuccess")
    void testConcurrentCommitLedgerSave() throws Exception {
        int threadCount = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            CommitLedger ledger =
                                    createOpenLedger("idem-concurrent", "batch-concurrent", 2);
                            boolean ok = ledgerStore.save(ledger);
                            if (ok) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(1);
        assertThat(failureCount.get()).isEqualTo(1);

        CommitLedger loaded = ledgerStore.load("idem-concurrent").orElseThrow();
        assertThat(loaded.state()).isEqualTo(BatchState.OPEN);
    }

    @Test
    @DisplayName(
            "S2: concurrentcreate GlobalCommitMarker — 2 same create same batchId, itemsuccess")
    void testConcurrentGlobalCommitMarkerCreate() throws Exception {
        int threadCount = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            GlobalCommitMarker marker =
                                    createCommittedMarker("batch-race", "idem-race");
                            boolean ok = markerStore.create(marker);
                            if (ok) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(1);
        assertThat(failureCount.get()).isEqualTo(1);

        GlobalCommitMarker loaded = markerStore.load("batch-race").orElseThrow();
        assertThat(loaded.isCommitted()).isTrue();
    }

    @Test
    @DisplayName(
            "S3: crash recovery — CommitLedger in SHARDS_PREPARED status after , can commitorin")
    void testCommitLedgerRecoveryFromShardsPrepared() throws IOException {
        CommitLedger ledger = createOpenLedger("idem-recover-sp", "batch-recover-sp", 2);
        assertThat(ledgerStore.save(ledger)).isTrue();
        ledgerStore.updateState("idem-recover-sp", BatchState.SHARDS_PREPARED);

        CommitLedgerStore recoveredStore = new MetastoreCommitLedgerStore(metastore);

        CommitLedger loaded = recoveredStore.load("idem-recover-sp").orElseThrow();
        assertThat(loaded.state()).isEqualTo(BatchState.SHARDS_PREPARED);
        assertThat(loaded.state().isAbortable()).isTrue();

        CommitLedger committed =
                recoveredStore.updateState("idem-recover-sp", BatchState.DATA_DURABLE);
        assertThat(committed.state()).isEqualTo(BatchState.DATA_DURABLE);

        CommitLedgerStore recoveredStore2 = new MetastoreCommitLedgerStore(metastore);
        CommitLedger loaded2 = recoveredStore2.load("idem-recover-sp").orElseThrow();
        assertThat(loaded2.state()).isEqualTo(BatchState.DATA_DURABLE);
        assertThat(loaded2.state().isAbortable()).isTrue();
        CommitLedger aborted = recoveredStore2.updateState("idem-recover-sp", BatchState.ABORTED);
        assertThat(aborted.state()).isEqualTo(BatchState.ABORTED);
        assertThat(aborted.state().isTerminal()).isTrue();
    }

    @Test
    @DisplayName(
            "S3: crash recovery — CommitLedger in GLOBAL_MARKER_COMMITTED status (canrollbackboundary), after canbefore cannotrollback")
    void testCommitLedgerRecoveryFromGlobalMarkerCommitted() throws IOException {
        CommitLedger ledger = createOpenLedger("idem-recover-gmc", "batch-recover-gmc", 2);
        assertThat(ledgerStore.save(ledger)).isTrue();
        ledgerStore.updateState("idem-recover-gmc", BatchState.SHARDS_PREPARED);
        ledgerStore.updateState("idem-recover-gmc", BatchState.DATA_DURABLE);
        ledgerStore.updateState("idem-recover-gmc", BatchState.GLOBAL_MARKER_COMMITTED);

        CommitLedgerStore recoveredStore = new MetastoreCommitLedgerStore(metastore);

        CommitLedger loaded = recoveredStore.load("idem-recover-gmc").orElseThrow();
        assertThat(loaded.state()).isEqualTo(BatchState.GLOBAL_MARKER_COMMITTED);
        assertThat(loaded.state().isPastPointOfNoReturn()).isTrue();
        assertThat(loaded.state().isAbortable()).isFalse();

        assertThatThrownBy(() -> recoveredStore.updateState("idem-recover-gmc", BatchState.ABORTED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Illegal batch state transition");

        CommitLedger committed =
                recoveredStore.updateState("idem-recover-gmc", BatchState.COMMITTED);
        assertThat(committed.state()).isEqualTo(BatchState.COMMITTED);
        assertThat(committed.state().isReadable()).isTrue();
    }

    @Test
    @DisplayName(
            "S3: crash recovery — GlobalCommitMarker alreadycreateafter , load after isCommitted() true")
    void testGlobalCommitMarkerRecovery() throws IOException {
        GlobalCommitMarker marker =
                createCommittedMarker("batch-marker-recover", "idem-marker-recover");
        assertThat(markerStore.create(marker)).isTrue();

        GlobalCommitMarkerStore recoveredStore = new MetastoreGlobalCommitMarkerStore(metastore);

        GlobalCommitMarker loaded = recoveredStore.load("batch-marker-recover").orElseThrow();
        assertThat(loaded.isCommitted()).isTrue();
        assertThat(loaded.state()).isEqualTo(MarkerState.COMMITTED);
        assertThat(loaded.batchId()).isEqualTo("batch-marker-recover");
        assertThat(loaded.committedAtMs()).isGreaterThan(0L);
    }

    @Test
    @DisplayName("S4: boundary — from OPEN and SHARDS_PREPARED statuscan in (isAbortable true)")
    void testCommitLedgerAbortFromAbortableState() throws IOException {
        CommitLedger openLedger = createOpenLedger("idem-abort-open", "batch-abort-open", 2);
        assertThat(openLedger.state()).isEqualTo(BatchState.OPEN);
        assertThat(openLedger.state().isAbortable()).isTrue();
        assertThat(ledgerStore.save(openLedger)).isTrue();
        CommitLedger abortedFromOpen =
                ledgerStore.updateState("idem-abort-open", BatchState.ABORTED);
        assertThat(abortedFromOpen.state()).isEqualTo(BatchState.ABORTED);
        assertThat(abortedFromOpen.state().isTerminal()).isTrue();

        CommitLedger spLedger = createOpenLedger("idem-abort-sp", "batch-abort-sp", 2);
        assertThat(ledgerStore.save(spLedger)).isTrue();
        ledgerStore.updateState("idem-abort-sp", BatchState.SHARDS_PREPARED);
        CommitLedger loaded = ledgerStore.load("idem-abort-sp").orElseThrow();
        assertThat(loaded.state().isAbortable()).isTrue();
        CommitLedger abortedFromSp = ledgerStore.updateState("idem-abort-sp", BatchState.ABORTED);
        assertThat(abortedFromSp.state()).isEqualTo(BatchState.ABORTED);

        CommitLedger ddLedger = createOpenLedger("idem-abort-dd", "batch-abort-dd", 2);
        assertThat(ledgerStore.save(ddLedger)).isTrue();
        ledgerStore.updateState("idem-abort-dd", BatchState.SHARDS_PREPARED);
        ledgerStore.updateState("idem-abort-dd", BatchState.DATA_DURABLE);
        CommitLedger loadedDd = ledgerStore.load("idem-abort-dd").orElseThrow();
        assertThat(loadedDd.state()).isEqualTo(BatchState.DATA_DURABLE);
        assertThat(loadedDd.state().isAbortable()).isTrue();
        CommitLedger abortedFromDd = ledgerStore.updateState("idem-abort-dd", BatchState.ABORTED);
        assertThat(abortedFromDd.state()).isEqualTo(BatchState.ABORTED);
    }

    @Test
    @DisplayName(
            "S4: boundary — canrollbackboundaryaftercannotin (GLOBAL_MARKER_COMMITTED/PUBLISHING isAbortable false)")
    void testCommitLedgerCannotAbortPastPointOfNoReturn() throws IOException {
        CommitLedger gmcLedger = createOpenLedger("idem-noabort-gmc", "batch-noabort-gmc", 2);
        assertThat(ledgerStore.save(gmcLedger)).isTrue();
        ledgerStore.updateState("idem-noabort-gmc", BatchState.SHARDS_PREPARED);
        ledgerStore.updateState("idem-noabort-gmc", BatchState.DATA_DURABLE);
        ledgerStore.updateState("idem-noabort-gmc", BatchState.GLOBAL_MARKER_COMMITTED);

        CommitLedger loadedGmc = ledgerStore.load("idem-noabort-gmc").orElseThrow();
        assertThat(loadedGmc.state()).isEqualTo(BatchState.GLOBAL_MARKER_COMMITTED);
        assertThat(loadedGmc.state().isAbortable()).isFalse();
        assertThat(loadedGmc.state().isPastPointOfNoReturn()).isTrue();

        assertThatThrownBy(() -> ledgerStore.updateState("idem-noabort-gmc", BatchState.ABORTED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Illegal batch state transition");

        ledgerStore.updateState("idem-noabort-gmc", BatchState.PUBLISHING);
        CommitLedger loadedPub = ledgerStore.load("idem-noabort-gmc").orElseThrow();
        assertThat(loadedPub.state()).isEqualTo(BatchState.PUBLISHING);
        assertThat(loadedPub.state().isAbortable()).isFalse();
        assertThat(loadedPub.state().isPastPointOfNoReturn()).isTrue();

        assertThatThrownBy(() -> ledgerStore.updateState("idem-noabort-gmc", BatchState.ABORTED))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName(
            "S4: boundary — expectedShardCount=0 be CommitLedger construct denied(rule: 1 itemshard)")
    void testCommitLedgerWithZeroExpectedShardCount() {
        assertThatThrownBy(() -> createOpenLedger("idem-zero", "batch-zero", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expectedShardCount must be >= 1");

        assertThatThrownBy(() -> createOpenLedger("idem-neg", "batch-neg", -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expectedShardCount must be >= 1");

        CommitLedger single = createOpenLedger("idem-one", "batch-one", 1);
        assertThat(single.expectedShardCount()).isEqualTo(1);
        assertThat(single.allShardsPrepared()).isFalse();
        assertThat(single.allShardsPublished()).isFalse();
    }
}
