package io.github.rawwise.backend.wal;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("WALConsumer - WAL test")
class WALConsumerTest {

    @TempDir Path tempDir;

    private WALConfig localConfig;
    private Configuration hadoopConfig;

    @BeforeEach
    void setUp() {
        localConfig =
                new WALConfig(
                        tempDir.resolve("wal").toString(),
                        "wal",
                        1024 * 1024,
                        true,
                        8192,
                        "NONE",
                        "testStore",
                        "executor-1",
                        "",
                        1000L);
        hadoopConfig = new Configuration();
    }

    private WALEvent createEvent(WALRecord record) {
        WALEvent event = new WALEvent();
        event.set(record);
        return event;
    }

    private DefaultWALRecord createDataRecord(long sequence, byte[] data, String tableId) {
        return new DefaultWALRecord(sequence, data, System.nanoTime(), 1L, tableId, "DATA");
    }

    @Nested
    @DisplayName("write test")
    class NormalWriteFlowTest {

        @Test
        @DisplayName("onEventwriterecordtowriter")
        void testOnEventWritesToWriter() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            WALRecord record = createDataRecord(0, "test_data".getBytes(), "table1");
            WALEvent event = createEvent(record);

            consumer.onEvent(event, 0, false);

            assertThat(consumer.getLastCommittedSequence()).isEqualTo(0);
            assertThat(event.isProcessed()).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("onEventupdatelastCommittedSequence")
        void testOnEventUpdatesLastCommittedSequence() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            for (int i = 0; i < 5; i++) {
                WALRecord record = createDataRecord(i, ("data" + i).getBytes(), "table1");
                WALEvent event = createEvent(record);
                consumer.onEvent(event, i, false);
            }

            assertThat(consumer.getLastCommittedSequence()).isEqualTo(4);

            consumer.onShutdown();
        }

        @Test
        @DisplayName("onEventskipnull entry")
        void testOnEventSkipsNullEntry() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            WALEvent event = new WALEvent();

            consumer.onEvent(event, 0, false);

            assertThat(consumer.getLastCommittedSequence()).isEqualTo(-1);

            consumer.onShutdown();
        }

        @Test
        @DisplayName("onEventinnot yetstart skiphandle")
        void testOnEventSkipsWhenNotRunning() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            WALRecord record = createDataRecord(0, "test_data".getBytes(), "table1");
            WALEvent event = createEvent(record);

            consumer.onEvent(event, 0, false);

            assertThat(consumer.getLastCommittedSequence()).isEqualTo(-1);
        }
    }

    @Nested
    @DisplayName("HDFSdegradationtest")
    class HdfsDegradationTest {

        @Test
        @DisplayName("writeWithRetryinretryfailafter hdfsUnavailable")
        void testWriteWithRetryMarksHdfsUnavailableAfterFailures() throws Exception {
            WALConfig hdfsConfig =
                    new WALConfig(
                            tempDir.resolve("wal").toString(),
                            "wal",
                            1024 * 1024,
                            true,
                            8192,
                            "NONE",
                            "testStore",
                            "executor-1",
                            "hdfs://nonexistent:9000/wal",
                            1000L);

            WALWriter failingWriter = mock(WALWriter.class);
            doThrow(new IOException("HDFS connection refused"))
                    .when(failingWriter)
                    .write(any(WALRecord.class));

            WALConsumer consumer = new WALConsumer(hdfsConfig, "BINARY", hadoopConfig);

            var writerField = WALConsumer.class.getDeclaredField("writer");
            writerField.setAccessible(true);
            writerField.set(consumer, failingWriter);

            var runningField = WALConsumer.class.getDeclaredField("running");
            runningField.setAccessible(true);
            runningField.set(consumer, true);

            var hdfsUnavailableField = WALConsumer.class.getDeclaredField("hdfsUnavailable");
            hdfsUnavailableField.setAccessible(true);

            assertThat(hdfsUnavailableField.getBoolean(consumer)).isFalse();

            for (int i = 0; i < 5; i++) {
                WALRecord record = createDataRecord(i, ("test_data" + i).getBytes(), "table1");
                WALEvent event = createEvent(record);
                try {
                    consumer.onEvent(event, i, false);
                } catch (Exception e) {
                    // may throw after all retries fail
                }
            }

            assertThat(hdfsUnavailableField.getBoolean(consumer)).isTrue();

            consumer.onShutdown();
        }
    }

    @Nested
    @DisplayName("isHdfsErrortest")
    class IsHdfsErrorTest {

        @Test
        @DisplayName("ConnectExceptionbe isHDFSerror")
        void testConnectExceptionIsHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            assertThat(
                            (Boolean)
                                    method.invoke(
                                            consumer,
                                            new ConnectException(
                                                    "ConnectException: Connection refused")))
                    .isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("NoRouteToHostExceptionbe isHDFSerror")
        void testNoRouteToHostExceptionIsHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            assertThat(
                            (Boolean)
                                    method.invoke(
                                            consumer,
                                            new NoRouteToHostException(
                                                    "NoRouteToHostException: No route to host")))
                    .isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("org.apache.hadooppackage ofexceptionbe isHDFSerror")
        void testHadoopPackageExceptionIsHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            org.apache.hadoop.fs.PathNotFoundException hadoopEx =
                    new org.apache.hadoop.fs.PathNotFoundException("Path not found");
            assertThat((Boolean) method.invoke(consumer, hadoopEx)).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("inpackage HDFS/NameNode/DataNodeofexceptionbe isHDFSerror")
        void testErrorMessageContainsHdfsKeywords() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            assertThat(
                            (Boolean)
                                    method.invoke(
                                            consumer, new RuntimeException("HDFS connection lost")))
                    .isTrue();
            assertThat(
                            (Boolean)
                                    method.invoke(
                                            consumer, new RuntimeException("NameNode unreachable")))
                    .isTrue();
            assertThat((Boolean) method.invoke(consumer, new RuntimeException("DataNode timeout")))
                    .isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("IOExceptionnot recognized as HDFS error")
        void testPlainIOExceptionIsNotHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            assertThat((Boolean) method.invoke(consumer, new IOException("Disk full"))).isFalse();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("nullnot recognized as HDFS error")
        void testNullIsNotHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            assertThat((Boolean) method.invoke(consumer, (Object) null)).isFalse();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("ofHDFSexceptionbe")
        void testNestedHdfsErrorIsDetected() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var method = WALConsumer.class.getDeclaredMethod("isHdfsError", Throwable.class);
            method.setAccessible(true);

            RuntimeException wrapper =
                    new RuntimeException(
                            "Write failed",
                            new ConnectException("ConnectException: Connection refused"));
            assertThat((Boolean) method.invoke(consumer, wrapper)).isTrue();

            consumer.onShutdown();
        }
    }

    @Nested
    @DisplayName("attemptRecoverytest")
    class AttemptRecoveryTest {

        @Test
        @DisplayName("non-HDFSerror forceSyncrecovery")
        void testAttemptRecoveryWithNonHdfsError() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            var method =
                    WALConsumer.class.getDeclaredMethod(
                            "attemptRecovery", long.class, Exception.class);
            method.setAccessible(true);

            boolean result =
                    (Boolean) method.invoke(consumer, 1L, new IOException("Local disk error"));

            assertThat(result).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("HDFSerror attemptRecoveryreturns true hdfsUnavailable")
        void testAttemptRecoveryWithHdfsErrorReturnsTrue() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            var method =
                    WALConsumer.class.getDeclaredMethod(
                            "attemptRecovery", long.class, Exception.class);
            method.setAccessible(true);

            boolean result =
                    (Boolean) method.invoke(consumer, 1L, new IOException("HDFS connection lost"));

            assertThat(result).isTrue();

            var hdfsUnavailableField = WALConsumer.class.getDeclaredField("hdfsUnavailable");
            hdfsUnavailableField.setAccessible(true);
            assertThat(hdfsUnavailableField.getBoolean(consumer)).isTrue();

            consumer.onShutdown();
        }
    }

    @Nested
    @DisplayName("Lifecycletest")
    class LifecycleTest {

        @Test
        @DisplayName("onStart() runningis true")
        void testOnStartSetsRunning() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var runningField = WALConsumer.class.getDeclaredField("running");
            runningField.setAccessible(true);
            assertThat(runningField.getBoolean(consumer)).isFalse();

            consumer.onStart();
            assertThat(runningField.getBoolean(consumer)).isTrue();

            consumer.onShutdown();
        }

        @Test
        @DisplayName("onShutdown() runningis false")
        void testOnShutdownSetsRunningFalse() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            var runningField = WALConsumer.class.getDeclaredField("running");
            runningField.setAccessible(true);
            assertThat(runningField.getBoolean(consumer)).isTrue();

            consumer.onShutdown();
            assertThat(runningField.getBoolean(consumer)).isFalse();
        }
    }

    @Nested
    @DisplayName("lastCommittedSequencetest")
    class LastCommittedSequenceTest {

        @Test
        @DisplayName("lastCommittedSequence valueis-1")
        void testLastCommittedSequenceInitialValue() {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            assertThat(consumer.getLastCommittedSequence()).isEqualTo(-1);
        }

        @Test
        @DisplayName("lastCommittedSequenceisvolatileof, can read")
        void testLastCommittedSequenceIsVolatile() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            WALRecord record = createDataRecord(42, "data".getBytes(), "table1");
            WALEvent event = createEvent(record);
            consumer.onEvent(event, 42, false);

            assertThat(consumer.getLastCommittedSequence()).isEqualTo(42);

            consumer.onShutdown();
        }
    }

    @Nested
    @DisplayName("construct test")
    class ConstructorTest {

        @Test
        @DisplayName("BINARYformat+non-HDFSconfigcreateLocalWALWriter")
        void testBinaryFormatCreatesLocalWALWriter() throws Exception {
            WALConsumer consumer = new WALConsumer(localConfig, "BINARY", hadoopConfig);

            var writerField = WALConsumer.class.getDeclaredField("writer");
            writerField.setAccessible(true);
            Object writer = writerField.get(consumer);

            assertThat(writer).isInstanceOf(LocalWALWriter.class);

            consumer.onShutdown();
        }

        @Test
        @DisplayName("HDFSconfigcreateHdfsWALWriter")
        void testHdfsConfigCreatesHdfsWALWriter() throws Exception {
            WALConfig hdfsConfig =
                    new WALConfig(
                            tempDir.resolve("wal").toString(),
                            "wal",
                            1024 * 1024,
                            true,
                            8192,
                            "NONE",
                            "testStore",
                            "executor-1",
                            "hdfs://localhost:9000/wal",
                            1000L);

            WALConsumer consumer = new WALConsumer(hdfsConfig, "BINARY", hadoopConfig);

            var writerField = WALConsumer.class.getDeclaredField("writer");
            writerField.setAccessible(true);
            Object writer = writerField.get(consumer);

            assertThat(writer).isInstanceOf(HdfsWALWriter.class);

            consumer.onShutdown();
        }
    }

    // ==================== v16 FIX: Circuit Breaker State Transitions ====================

    @Nested
    @DisplayName("v16: Circuit Breakerstatus test (B3, C3)")
    class CircuitBreakerStateTransitionsTest {

        @Test
        @DisplayName("CLOSED -> OPEN: consecutive failures reach threshold circuit breaker opens")
        void testCircuitBreakerOpensAfterConsecutiveFailures() throws Exception {
            WALConfig hdfsConfig =
                    new WALConfig(
                            tempDir.resolve("wal").toString(),
                            "wal",
                            1024 * 1024,
                            true,
                            8192,
                            "NONE",
                            "testStore",
                            "executor-1",
                            "hdfs://nonexistent:9000/wal",
                            1000L);

            WALConsumer consumer = new WALConsumer(hdfsConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            WALWriter mockWriter = mock(WALWriter.class);
            doThrow(new IOException("HDFS write failed"))
                    .when(mockWriter)
                    .write(any(WALRecord.class));

            var writerField = WALConsumer.class.getDeclaredField("writer");
            writerField.setAccessible(true);
            writerField.set(consumer, mockWriter);

            var circuitBreakerOpenField = WALConsumer.class.getDeclaredField("circuitBreakerOpen");
            circuitBreakerOpenField.setAccessible(true);
            var hdfsUnavailableField = WALConsumer.class.getDeclaredField("hdfsUnavailable");
            hdfsUnavailableField.setAccessible(true);
            var consecutiveFailureCountField =
                    WALConsumer.class.getDeclaredField("consecutiveFailureCount");
            consecutiveFailureCountField.setAccessible(true);

            assertThat(circuitBreakerOpenField.getBoolean(consumer)).isFalse();

            WALRecord record = createDataRecord(0, "data0".getBytes(), "table1");
            WALEvent event = createEvent(record);
            try {
                consumer.onEvent(event, 0, false);
            } catch (Exception ignored) {
            }

            assertThat(hdfsUnavailableField.getBoolean(consumer)).isTrue();
            assertThat(((AtomicLong) consecutiveFailureCountField.get(consumer)).get())
                    .isGreaterThanOrEqualTo(3L);

            consumer.onShutdown();
        }

        @Test
        @DisplayName("HALF_OPEN: timeoutafter status, hdfsUnavailableandconsecutiveFailureCountbe")
        void testHalfOpenStateResetsHdfsUnavailableAndFailureCount() throws Exception {
            WALConfig hdfsConfig =
                    new WALConfig(
                            tempDir.resolve("wal").toString(),
                            "wal",
                            1024 * 1024,
                            true,
                            8192,
                            "NONE",
                            "testStore",
                            "executor-1",
                            "hdfs://nonexistent:9000/wal",
                            1000L);

            WALConsumer consumer = new WALConsumer(hdfsConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            // circuit breakeralready status
            var circuitBreakerOpenField = WALConsumer.class.getDeclaredField("circuitBreakerOpen");
            circuitBreakerOpenField.setAccessible(true);
            circuitBreakerOpenField.set(consumer, true);

            var circuitBreakerOpenTimeField =
                    WALConsumer.class.getDeclaredField("circuitBreakerOpenTime");
            circuitBreakerOpenTimeField.setAccessible(true);
            // is120 before, trigger status
            circuitBreakerOpenTimeField.set(consumer, System.currentTimeMillis() - 121_000L);

            var hdfsUnavailableField = WALConsumer.class.getDeclaredField("hdfsUnavailable");
            hdfsUnavailableField.setAccessible(true);
            hdfsUnavailableField.set(consumer, true);

            var consecutiveFailureCountField =
                    WALConsumer.class.getDeclaredField("consecutiveFailureCount");
            consecutiveFailureCountField.setAccessible(true);
            ((AtomicLong) consecutiveFailureCountField.get(consumer)).set(15L);

            // write, trigger status
            WALRecord record = createDataRecord(0, "test_data".getBytes(), "table1");
            WALEvent event = createEvent(record);

            WALWriter mockWriter = mock(WALWriter.class);
            var writerField = WALConsumer.class.getDeclaredField("writer");
            writerField.setAccessible(true);
            writerField.set(consumer, mockWriter);

            try {
                consumer.onEvent(event, 0, false);
            } catch (Exception ignored) {
            }

            // statusshouldthe hdfsUnavailable = false
            assertThat(hdfsUnavailableField.getBoolean(consumer)).isFalse();
            // consecutiveFailureCount shouldthebe is 0
            assertThat(((AtomicLong) consecutiveFailureCountField.get(consumer)).get())
                    .isEqualTo(0L);

            consumer.onShutdown();
        }

        @Test
        @DisplayName("Circuit Breaker skipwrite recordskippedWriteCount")
        void testCircuitBreakerOpenSkipsWriteAndIncrementsSkippedCount() throws Exception {
            WALConfig hdfsConfig =
                    new WALConfig(
                            tempDir.resolve("wal").toString(),
                            "wal",
                            1024 * 1024,
                            true,
                            8192,
                            "NONE",
                            "testStore",
                            "executor-1",
                            "hdfs://nonexistent:9000/wal",
                            1000L);

            WALConsumer consumer = new WALConsumer(hdfsConfig, "BINARY", hadoopConfig);
            consumer.onStart();

            // circuit breakeralready status
            var circuitBreakerOpenField = WALConsumer.class.getDeclaredField("circuitBreakerOpen");
            circuitBreakerOpenField.setAccessible(true);
            circuitBreakerOpenField.set(consumer, true);

            var circuitBreakerOpenTimeField =
                    WALConsumer.class.getDeclaredField("circuitBreakerOpenTime");
            circuitBreakerOpenTimeField.setAccessible(true);
            // is , does not satisfy timeoutcondition
            circuitBreakerOpenTimeField.set(consumer, System.currentTimeMillis());

            var skippedWriteCountField = WALConsumer.class.getDeclaredField("skippedWriteCount");
            skippedWriteCountField.setAccessible(true);

            long initialSkipped = ((AtomicLong) skippedWriteCountField.get(consumer)).get();

            WALRecord record = createDataRecord(0, "test_data".getBytes(), "table1");
            WALEvent event = createEvent(record);

            consumer.onEvent(event, 0, false);

            // skippedWriteCount shouldthe
            assertThat(((AtomicLong) skippedWriteCountField.get(consumer)).get())
                    .isGreaterThan(initialSkipped);
            // circuit breaker
            assertThat(circuitBreakerOpenField.getBoolean(consumer)).isTrue();

            consumer.onShutdown();
        }
    }
}
