package io.github.rawwise.backend.wal;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.engine.storage.DurabilityLevel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("GroupCommit WAL E2Etest")
class GroupCommitE2ETest {

    @TempDir Path tempDir;

    private WALConfig createGroupCommitConfig(String dir, int batchSize, long flushIntervalMs) {
        return new WALConfig(
                dir,
                "wal",
                1024 * 1024,
                true,
                8192,
                "NONE",
                "testStore",
                "executor-1",
                "",
                1000L,
                30000L,
                true,
                batchSize,
                flushIntervalMs);
    }

    private WALConfig createDisabledConfig(String dir) {
        return new WALConfig(
                dir,
                "wal",
                1024 * 1024,
                true,
                8192,
                "NONE",
                "testStore",
                "executor-1",
                "",
                1000L,
                30000L,
                false,
                1000,
                100L);
    }

    private WALRecord createTestRecord() {
        return new DefaultWALRecord(
                0, "test_data".getBytes(), System.nanoTime(), 1L, "table1", "DATA");
    }

    private WALRecord createTestRecord(String tableId, int index) {
        return new DefaultWALRecord(
                0, ("data_" + index).getBytes(), System.nanoTime(), 1L, tableId, "DATA");
    }

    private WALConsumer getConsumer(WAL wal) throws Exception {
        var consumerField = WAL.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        return (WALConsumer) consumerField.get(wal);
    }

    private void waitForConsumerRunning(WAL wal) throws Exception {
        WALConsumer consumer = getConsumer(wal);
        long deadline = System.currentTimeMillis() + 5000;
        while (!consumer.isRunning() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(consumer.isRunning()).isTrue();
    }

    private void waitForCommittedSequence(WALConsumer consumer, long targetSeq) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        while (consumer.getLastCommittedSequence() < targetSeq
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(consumer.getLastCommittedSequence()).isGreaterThanOrEqualTo(targetSeq);
    }

    @Nested
    @DisplayName("GroupCommit appendandforceSync E2Etest")
    class GroupCommitAppendForceSyncTest {

        @Test
        @DisplayName("appendWithGroupCommitafter writingforceSyncedSequence")
        void testAppendWithGroupCommitAdvancesForceSyncedSequence() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(tempDir.resolve("gc_append").toString(), 5, 200L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                WALConsumer consumer = getConsumer(wal);

                long seq = wal.appendWithGroupCommit(createTestRecord());

                assertThat(seq).isGreaterThanOrEqualTo(0);
                assertThat(consumer.getLastForceSyncedSequence()).isGreaterThanOrEqualTo(seq);
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("appendWithGroupCommitwritemultiitemrecordafterallforceSyncedSequence")
        void testMultipleAppendsAllForceSynced() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(tempDir.resolve("gc_multi").toString(), 2, 200L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                WALConsumer consumer = getConsumer(wal);

                long maxSeq = -1;
                for (int i = 0; i < 5; i++) {
                    maxSeq = wal.appendWithGroupCommit(createTestRecord("table1", i));
                }

                assertThat(consumer.getLastForceSyncedSequence()).isGreaterThanOrEqualTo(maxSeq);
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("GroupCommitbatchthresholdtriggerE2Etest")
    class GroupCommitBatchThresholdTest {

        @Test
        @DisplayName("tobatchSizeThreshold triggerforceSync")
        void testForceSyncTriggeredByBatchSize() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(tempDir.resolve("gc_batch").toString(), 3, 5000L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                WALConsumer consumer = getConsumer(wal);

                wal.append(createTestRecord("t", 0), false);
                wal.append(createTestRecord("t", 1), false);

                waitForCommittedSequence(consumer, 1);

                long seq2 = wal.appendWithGroupCommit(createTestRecord("t", 2));

                assertThat(consumer.getLastForceSyncedSequence()).isGreaterThanOrEqualTo(seq2);
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("not yet tobatchSizeThreshold forceSyncbyflushInterval")
        void testForceSyncNotTriggeredBeforeBatchSize() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(
                            tempDir.resolve("gc_batch_partial").toString(), 10, 300L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                WALConsumer consumer = getConsumer(wal);

                long seq = wal.appendWithGroupCommit(createTestRecord("t", 0));

                assertThat(consumer.getLastForceSyncedSequence()).isGreaterThanOrEqualTo(seq);
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("GroupCommit flush triggerE2Etest")
    class GroupCommitFlushIntervalTest {

        @Test
        @DisplayName("flushIntervaltimeoutaftertriggerforceSync")
        void testForceSyncTriggeredByFlushInterval() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(tempDir.resolve("gc_flush").toString(), 1000, 100L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                WALConsumer consumer = getConsumer(wal);

                long seq = wal.appendWithGroupCommit(createTestRecord("t", 0));

                assertThat(consumer.getLastForceSyncedSequence()).isGreaterThanOrEqualTo(seq);

                Thread.sleep(250);

                long seq2 = wal.appendWithGroupCommit(createTestRecord("t", 1));

                assertThat(consumer.getLastForceSyncedSequence()).isGreaterThanOrEqualTo(seq2);
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("singleitemrecordinflushIntervalafter beforceSync")
        void testSingleRecordForceSyncAfterFlushInterval() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(
                            tempDir.resolve("gc_flush_single").toString(), 1000, 100L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                WALConsumer consumer = getConsumer(wal);

                long seq = wal.appendWithGroupCommit(createTestRecord("t", 0));

                assertThat(consumer.getLastForceSyncedSequence()).isGreaterThanOrEqualTo(seq);

                long forceSyncedBefore = consumer.getLastForceSyncedSequence();

                Thread.sleep(200);

                assertThat(consumer.getLastForceSyncedSequence())
                        .isGreaterThanOrEqualTo(forceSyncedBefore);
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("GroupCommit deniedE2Etest")
    class GroupCommitDisabledRejectionTest {

        @Test
        @DisplayName("groupCommitEnabled=false appendWithGroupCommit IllegalStateException")
        void testAppendWithGroupCommitThrowsWhenDisabled() throws Exception {
            WALConfig config = createDisabledConfig(tempDir.resolve("gc_disabled").toString());
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);

                assertThatThrownBy(() -> wal.appendWithGroupCommit(createTestRecord()))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Group commit is not enabled");
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("groupCommitEnabled=false isGroupCommitEnabledreturns false")
        void testIsGroupCommitEnabledReturnsFalseWhenDisabled() throws Exception {
            WALConfig config =
                    createDisabledConfig(tempDir.resolve("gc_disabled_check").toString());
            WAL wal = new WAL(config);
            try {
                assertThat(wal.isGroupCommitEnabled()).isFalse();
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("groupCommitEnabled=true isGroupCommitEnabledreturns true")
        void testIsGroupCommitEnabledReturnsTrueWhenEnabled() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(
                            tempDir.resolve("gc_enabled_check").toString(), 5, 100L);
            WAL wal = new WAL(config);
            try {
                assertThat(wal.isGroupCommitEnabled()).isTrue();
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("GroupCommiton closedrain E2Etest")
    class GroupCommitShutdownDrainTest {

        @Test
        @DisplayName("WALon closepending batchbeforceSync")
        void testPendingBatchForceSyncedOnShutdown() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(tempDir.resolve("gc_drain").toString(), 1000, 5000L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                WALConsumer consumer = getConsumer(wal);

                wal.append(createTestRecord("t", 0), false);
                wal.append(createTestRecord("t", 1), false);
                wal.append(createTestRecord("t", 2), false);

                waitForCommittedSequence(consumer, 2);

                long forceSyncedBefore = consumer.getLastForceSyncedSequence();

                wal.close();

                assertThat(consumer.getLastForceSyncedSequence())
                        .isGreaterThanOrEqualTo(forceSyncedBefore);
            } finally {
                if (wal.isRunning()) {
                    wal.close();
                }
            }
        }

        @Test
        @DisplayName("WALon closeonShutdownexecutedrain forceSync")
        void testOnShutdownDrainsPendingBatch() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(
                            tempDir.resolve("gc_drain_shutdown").toString(), 1000, 5000L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                WALConsumer consumer = getConsumer(wal);

                wal.append(createTestRecord("t", 0), false);
                wal.append(createTestRecord("t", 1), false);

                waitForCommittedSequence(consumer, 1);

                long forceSyncedBefore = consumer.getLastForceSyncedSequence();

                consumer.onShutdown();

                assertThat(consumer.getLastForceSyncedSequence())
                        .isGreaterThanOrEqualTo(forceSyncedBefore);
            } finally {
                if (wal.isRunning()) {
                    wal.close();
                }
            }
        }
    }

    @Nested
    @DisplayName("GroupCommitconcurrent writeE2Etest")
    class GroupCommitConcurrentWriteTest {

        @Test
        @DisplayName("multi-threaded concurrentappendWithGroupCommitnodata allsequencebeforceSync")
        void testConcurrentAppendWithGroupCommitNoDataLoss() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(tempDir.resolve("gc_concurrent").toString(), 5, 100L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                WALConsumer consumer = getConsumer(wal);

                int threadCount = 8;
                int recordsPerThread = 10;
                CountDownLatch startLatch = new CountDownLatch(1);
                CountDownLatch doneLatch = new CountDownLatch(threadCount);
                List<Long> allSequences = Collections.synchronizedList(new ArrayList<>());
                AtomicBoolean hasUnexpectedException = new AtomicBoolean(false);

                for (int t = 0; t < threadCount; t++) {
                    final int threadIdx = t;
                    new Thread(
                                    () -> {
                                        try {
                                            startLatch.await();
                                            for (int i = 0; i < recordsPerThread; i++) {
                                                long seq =
                                                        wal.appendWithGroupCommit(
                                                                createTestRecord(
                                                                        "table_" + threadIdx, i));
                                                allSequences.add(seq);
                                            }
                                        } catch (WALWriteException e) {
                                            // acceptable during shutdown race
                                        } catch (Exception e) {
                                            hasUnexpectedException.set(true);
                                        } finally {
                                            doneLatch.countDown();
                                        }
                                    })
                            .start();
                }

                startLatch.countDown();
                boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
                assertThat(completed).isTrue();
                assertThat(hasUnexpectedException.get()).isFalse();

                assertThat(allSequences).hasSize(threadCount * recordsPerThread);

                long maxSeq = allSequences.stream().mapToLong(Long::longValue).max().orElse(-1);
                assertThat(consumer.getLastForceSyncedSequence()).isGreaterThanOrEqualTo(maxSeq);
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("concurrentappendandappendWithGroupCommit noexception")
        void testMixedConcurrentAppendAndGroupCommit() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(
                            tempDir.resolve("gc_mixed_concurrent").toString(), 3, 100L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);

                int threadCount = 4;
                CountDownLatch startLatch = new CountDownLatch(1);
                CountDownLatch doneLatch = new CountDownLatch(threadCount * 2);
                AtomicBoolean hasUnexpectedException = new AtomicBoolean(false);

                for (int i = 0; i < threadCount; i++) {
                    final int idx = i;
                    new Thread(
                                    () -> {
                                        try {
                                            startLatch.await();
                                            wal.append(createTestRecord("append_t", idx), false);
                                        } catch (Exception e) {
                                            hasUnexpectedException.set(true);
                                        } finally {
                                            doneLatch.countDown();
                                        }
                                    })
                            .start();

                    new Thread(
                                    () -> {
                                        try {
                                            startLatch.await();
                                            wal.appendWithGroupCommit(
                                                    createTestRecord("gc_t", idx));
                                        } catch (Exception e) {
                                            hasUnexpectedException.set(true);
                                        } finally {
                                            doneLatch.countDown();
                                        }
                                    })
                            .start();
                }

                startLatch.countDown();
                boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
                assertThat(completed).isTrue();
                assertThat(hasUnexpectedException.get()).isFalse();
            } finally {
                wal.close();
            }
        }
    }

    @Nested
    @DisplayName("WalWriteAdapter groupCommitpathE2Etest")
    class WalWriteAdapterGroupCommitPathTest {

        @Test
        @DisplayName("groupCommitEnabled=true WalWriteAdaptercallappendWithGroupCommit")
        void testWalWriteAdapterCallsAppendWithGroupCommit() throws Exception {
            WALConfig config =
                    createGroupCommitConfig(tempDir.resolve("adapter_gc").toString(), 5, 100L);
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                WALConsumer consumer = getConsumer(wal);

                WalWriteAdapter adapter =
                        new WalWriteAdapter(wal, DurabilityLevel.NODE, new AtomicLong(0));

                assertThat(adapter.isGroupCommitEnabled()).isTrue();

                byte[] walData = "adapter_test_data".getBytes();
                WALRecord walRecord =
                        new DefaultWALRecord(
                                -1, walData, System.nanoTime(), -1L, "adapterTable", "DATA");

                long seq = wal.appendWithGroupCommit(walRecord);

                assertThat(seq).isGreaterThanOrEqualTo(0);
                assertThat(consumer.getLastForceSyncedSequence()).isGreaterThanOrEqualTo(seq);
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("groupCommitEnabled=false WalWriteAdaptercallappend")
        void testWalWriteAdapterCallsAppendWhenGroupCommitDisabled() throws Exception {
            WALConfig config = createDisabledConfig(tempDir.resolve("adapter_no_gc").toString());
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);

                WalWriteAdapter adapter =
                        new WalWriteAdapter(wal, DurabilityLevel.NODE, new AtomicLong(0));

                assertThat(adapter.isGroupCommitEnabled()).isFalse();

                byte[] walData = "adapter_test_data".getBytes();
                WALRecord walRecord =
                        new DefaultWALRecord(
                                -1, walData, System.nanoTime(), -1L, "adapterTable", "DATA");

                long seq = wal.append(walRecord, false);

                assertThat(seq).isGreaterThanOrEqualTo(0);
            } finally {
                wal.close();
            }
        }

        @Test
        @DisplayName("WalWriteAdapterinWALwritefailthrows whenRecoverableWriteException")
        void testWalWriteAdapterThrowsOnWalFailure() throws Exception {
            WALConfig config = createDisabledConfig(tempDir.resolve("adapter_fail").toString());
            WAL wal = new WAL(config);
            try {
                waitForConsumerRunning(wal);
                wal.close();

                WalWriteAdapter adapter =
                        new WalWriteAdapter(wal, DurabilityLevel.NODE, new AtomicLong(0));

                assertThatThrownBy(
                                () -> {
                                    byte[] walData = "adapter_test_data".getBytes();
                                    WALRecord walRecord =
                                            new DefaultWALRecord(
                                                    -1,
                                                    walData,
                                                    System.nanoTime(),
                                                    -1L,
                                                    "adapterTable",
                                                    "DATA");
                                    wal.append(walRecord, false);
                                })
                        .isInstanceOf(WALWriteException.class);
            } finally {
                if (wal.isRunning()) {
                    wal.close();
                }
            }
        }
    }

    @Nested
    @DisplayName("WALConfig groupCommit verifyE2Etest")
    class WALConfigValidationTest {

        @Test
        @DisplayName("groupCommitBatchSize<=0throws whenIllegalArgumentException")
        void testGroupCommitBatchSizeZeroThrows() {
            assertThatThrownBy(
                            () ->
                                    new WALConfig(
                                            tempDir.resolve("cfg_batch_zero").toString(),
                                            "wal",
                                            1024 * 1024,
                                            true,
                                            8192,
                                            "NONE",
                                            "testStore",
                                            "executor-1",
                                            "",
                                            1000L,
                                            30000L,
                                            true,
                                            0,
                                            100L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("groupCommitBatchSize must be > 0");
        }

        @Test
        @DisplayName("groupCommitBatchSizeis throws whenIllegalArgumentException")
        void testGroupCommitBatchSizeNegativeThrows() {
            assertThatThrownBy(
                            () ->
                                    new WALConfig(
                                            tempDir.resolve("cfg_batch_neg").toString(),
                                            "wal",
                                            1024 * 1024,
                                            true,
                                            8192,
                                            "NONE",
                                            "testStore",
                                            "executor-1",
                                            "",
                                            1000L,
                                            30000L,
                                            true,
                                            -5,
                                            100L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("groupCommitBatchSize must be > 0");
        }

        @Test
        @DisplayName("groupCommitFlushIntervalMs<=0throws whenIllegalArgumentException")
        void testGroupCommitFlushIntervalZeroThrows() {
            assertThatThrownBy(
                            () ->
                                    new WALConfig(
                                            tempDir.resolve("cfg_flush_zero").toString(),
                                            "wal",
                                            1024 * 1024,
                                            true,
                                            8192,
                                            "NONE",
                                            "testStore",
                                            "executor-1",
                                            "",
                                            1000L,
                                            30000L,
                                            true,
                                            10,
                                            0L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("groupCommitFlushIntervalMs must be > 0");
        }

        @Test
        @DisplayName("groupCommitFlushIntervalMsis throws whenIllegalArgumentException")
        void testGroupCommitFlushIntervalNegativeThrows() {
            assertThatThrownBy(
                            () ->
                                    new WALConfig(
                                            tempDir.resolve("cfg_flush_neg").toString(),
                                            "wal",
                                            1024 * 1024,
                                            true,
                                            8192,
                                            "NONE",
                                            "testStore",
                                            "executor-1",
                                            "",
                                            1000L,
                                            30000L,
                                            true,
                                            10,
                                            -100L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("groupCommitFlushIntervalMs must be > 0");
        }

        @Test
        @DisplayName(
                "groupCommitFlushIntervalMs>appendTimeoutMsthrows whenIllegalArgumentException")
        void testGroupCommitFlushIntervalExceedsAppendTimeoutThrows() {
            assertThatThrownBy(
                            () ->
                                    new WALConfig(
                                            tempDir.resolve("cfg_flush_exceed").toString(),
                                            "wal",
                                            1024 * 1024,
                                            true,
                                            8192,
                                            "NONE",
                                            "testStore",
                                            "executor-1",
                                            "",
                                            1000L,
                                            5000L,
                                            true,
                                            10,
                                            10000L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("groupCommitFlushIntervalMs must be <= appendTimeoutMs");
        }

        @Test
        @DisplayName("groupCommitEnabled=false no does not triggerverify")
        void testInvalidParamsIgnoredWhenGroupCommitDisabled() {
            assertThatCode(
                            () ->
                                    new WALConfig(
                                            tempDir.resolve("cfg_disabled_invalid").toString(),
                                            "wal",
                                            1024 * 1024,
                                            true,
                                            8192,
                                            "NONE",
                                            "testStore",
                                            "executor-1",
                                            "",
                                            1000L,
                                            30000L,
                                            false,
                                            0,
                                            -1L))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("groupCommitEnabled=true WALConfig create")
        void testValidGroupCommitConfigCreatesSuccessfully() {
            assertThatCode(
                            () ->
                                    new WALConfig(
                                            tempDir.resolve("cfg_valid").toString(),
                                            "wal",
                                            1024 * 1024,
                                            true,
                                            8192,
                                            "NONE",
                                            "testStore",
                                            "executor-1",
                                            "",
                                            1000L,
                                            30000L,
                                            true,
                                            10,
                                            100L))
                    .doesNotThrowAnyException();
        }
    }
}
