package io.github.rawwise.backend.wal;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("GroupCommitWalPersister - WAL Group Committest")
class GroupCommitWalPersisterTest {

    private WALRecord createRecord(long seq) {
        return new DefaultWALRecord(
                seq, ("data-" + seq).getBytes(), System.nanoTime(), 1L, "table1", "DATA");
    }

    private List<WALRecord> createRecords(int count) {
        List<WALRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(createRecord(i));
        }
        return records;
    }

    @Nested
    @DisplayName("singleitemrecordpersistedtest")
    class SingleRecordTest {

        @Test
        @DisplayName("singleitemrecordpersistedsuccess")
        void testSingleRecordPersistenceSucceeds() {
            WAL wal = mock(WAL.class);
            when(wal.append(any(WALRecord.class), anyBoolean())).thenReturn(0L);
            GroupCommitWalPersister persister = new GroupCommitWalPersister(wal, 5000, 100);
            persister.start();

            try {
                assertThatCode(() -> persister.persistWithGroupCommit(1L, createRecords(1)))
                        .doesNotThrowAnyException();

                verify(wal, timeout(1000)).append(any(WALRecord.class), eq(true));
            } finally {
                persister.stop();
            }
        }
    }

    @Nested
    @DisplayName("sametransactionmultiitemrecordtest")
    class MultipleRecordsTest {

        @Test
        @DisplayName("sametransactionmultiitemrecordpersistedsuccess")
        void testMultipleRecordsFromSameTransactionSucceed() {
            WAL wal = mock(WAL.class);
            when(wal.append(any(WALRecord.class), anyBoolean())).thenReturn(0L);
            GroupCommitWalPersister persister = new GroupCommitWalPersister(wal, 5000, 100);
            persister.start();

            try {
                List<WALRecord> records = createRecords(3);

                assertThatCode(() -> persister.persistWithGroupCommit(1L, records))
                        .doesNotThrowAnyException();

                verify(wal, timeout(1000).times(3)).append(any(WALRecord.class), anyBoolean());
                verify(wal, timeout(1000)).append(any(WALRecord.class), eq(true));
                verify(wal, timeout(1000).times(2)).append(any(WALRecord.class), eq(false));
            } finally {
                persister.stop();
            }
        }
    }

    @Nested
    @DisplayName("fsyncfail test")
    class FsyncFailureTest {

        @Test
        @DisplayName("fsyncfail WalSyncFailedExceptiontoallwaitoftransaction")
        void testFsyncFailurePropagatesToAllWaitingTransactions() throws Exception {
            WAL wal = mock(WAL.class);
            doThrow(new WALWriteException("fsync failed"))
                    .when(wal)
                    .append(any(WALRecord.class), anyBoolean());
            GroupCommitWalPersister persister = new GroupCommitWalPersister(wal, 5000, 100);
            persister.start();

            try {
                int txnCount = 3;
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                AtomicInteger exceptionCount = new AtomicInteger(0);

                for (int i = 0; i < txnCount; i++) {
                    final long txnId = i + 1;
                    CompletableFuture<Void> future =
                            CompletableFuture.runAsync(
                                    () -> {
                                        try {
                                            persister.persistWithGroupCommit(
                                                    txnId, createRecords(1));
                                        } catch (WalSyncFailedException e) {
                                            exceptionCount.incrementAndGet();
                                        }
                                    });
                    futures.add(future);
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(5, TimeUnit.SECONDS);

                assertThat(exceptionCount.get()).isEqualTo(txnCount);
            } finally {
                persister.stop();
            }
        }

        @Test
        @DisplayName("WalSyncFailedExceptionpackage correctoftxnId")
        void testWalSyncFailedExceptionContainsCorrectTxnId() {
            WAL wal = mock(WAL.class);
            doThrow(new WALWriteException("fsync failed"))
                    .when(wal)
                    .append(any(WALRecord.class), anyBoolean());
            GroupCommitWalPersister persister = new GroupCommitWalPersister(wal, 5000, 100);
            persister.start();

            try {
                long txnId = 42L;
                WalSyncFailedException exception =
                        catchThrowableOfType(
                                () -> persister.persistWithGroupCommit(txnId, createRecords(1)),
                                WalSyncFailedException.class);

                assertThat(exception.getTxnId()).isEqualTo(txnId);
                assertThat(exception.getMessage()).contains(String.valueOf(txnId));
            } finally {
                persister.stop();
            }
        }
    }

    @Nested
    @DisplayName("timeouttest")
    class TimeoutTest {

        @Test
        @DisplayName("throws whenWalSyncFailedException")
        void testTimeoutThrowsWalSyncFailedException() {
            WAL wal = mock(WAL.class);
            GroupCommitWalPersister persister = new GroupCommitWalPersister(wal, 100, 100);

            WalSyncFailedException exception =
                    catchThrowableOfType(
                            () -> persister.persistWithGroupCommit(1L, createRecords(1)),
                            WalSyncFailedException.class);

            assertThat(exception.getTxnId()).isEqualTo(1L);
            assertThat(exception.getMessage()).contains("Timeout");
        }
    }

    @Nested
    @DisplayName("stop test")
    class StopDrainTest {

        @Test
        @DisplayName("stop of handletransaction")
        void testStopDrainsRemainingPendingTransactions() throws Exception {
            WAL wal = mock(WAL.class);
            when(wal.append(any(WALRecord.class), anyBoolean())).thenReturn(0L);
            GroupCommitWalPersister persister = new GroupCommitWalPersister(wal, 5000, 100);
            persister.start();

            AtomicReference<Exception> exception = new AtomicReference<>();
            Thread t =
                    new Thread(
                            () -> {
                                try {
                                    persister.persistWithGroupCommit(1L, createRecords(1));
                                } catch (WalSyncFailedException e) {
                                    exception.set(e);
                                }
                            });
            t.start();

            Thread.sleep(50);
            persister.stop();

            t.join(5000);
            assertThat(t.isAlive()).isFalse();
            assertThat(exception.get()).isNull();
            verify(wal, atLeastOnce()).append(any(WALRecord.class), anyBoolean());
        }

        @Test
        @DisplayName("stop multiple handletransaction")
        void testStopDrainsMultiplePendingTransactions() throws Exception {
            WAL wal = mock(WAL.class);
            when(wal.append(any(WALRecord.class), anyBoolean())).thenReturn(0L);
            GroupCommitWalPersister persister = new GroupCommitWalPersister(wal, 5000, 100);
            persister.start();

            int txnCount = 5;
            ExecutorService executor = Executors.newFixedThreadPool(txnCount);
            AtomicInteger successCount = new AtomicInteger(0);

            try {
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (int i = 0; i < txnCount; i++) {
                    final long txnId = i + 1;
                    futures.add(
                            CompletableFuture.runAsync(
                                    () -> {
                                        try {
                                            persister.persistWithGroupCommit(
                                                    txnId, createRecords(1));
                                            successCount.incrementAndGet();
                                        } catch (WalSyncFailedException e) {
                                        }
                                    },
                                    executor));
                }

                Thread.sleep(100);
                persister.stop();

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(5, TimeUnit.SECONDS);

                assertThat(successCount.get()).isEqualTo(txnCount);
                verify(wal, atLeast(txnCount)).append(any(WALRecord.class), anyBoolean());
            } finally {
                executor.shutdown();
            }
        }
    }

    @Nested
    @DisplayName("Group Commitbatch test")
    class GroupCommitBatchTest {

        @Test
        @DisplayName("multipleconcurrent transactions same fsync")
        void testMultipleTransactionsShareSingleFsync() throws Exception {
            WAL wal = mock(WAL.class);
            when(wal.append(any(WALRecord.class), anyBoolean())).thenReturn(0L);
            GroupCommitWalPersister persister = new GroupCommitWalPersister(wal, 5000, 100);
            persister.start();

            try {
                int txnCount = 5;
                CountDownLatch startLatch = new CountDownLatch(1);
                List<CompletableFuture<Void>> futures = new ArrayList<>();

                for (int i = 0; i < txnCount; i++) {
                    final long txnId = i + 1;
                    futures.add(
                            CompletableFuture.runAsync(
                                    () -> {
                                        try {
                                            startLatch.await();
                                            persister.persistWithGroupCommit(
                                                    txnId, createRecords(1));
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                        }
                                    }));
                }

                startLatch.countDown();

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(5, TimeUnit.SECONDS);

                verify(wal, atLeast(txnCount)).append(any(WALRecord.class), anyBoolean());
                verify(wal, atLeastOnce()).append(any(WALRecord.class), eq(true));
            } finally {
                persister.stop();
            }
        }
    }
}
