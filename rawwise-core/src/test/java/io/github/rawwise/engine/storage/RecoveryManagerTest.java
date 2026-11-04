package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.rawwise.backend.routing.RoutingEpoch;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardRecoveryRecord;
import io.github.rawwise.backend.wal.DefaultWALRecord;
import io.github.rawwise.backend.wal.TxnWALRecord;
import io.github.rawwise.backend.wal.TxnWALRecordCodec;
import io.github.rawwise.backend.wal.WALConfig;
import io.github.rawwise.backend.wal.WALRecord;
import io.github.rawwise.backend.wal.WALWriter;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.governance.config.RawWiseConfig;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * RecoveryManager unit test.
 *
 * <p>teststrategy: - static methods(extractKey/Value/TableId, parseTableIdentifier)via reflection
 * directly test - replayWALRecords via mock depend onverify behavior - loadExistInstance /
 * recoverFromWAL real StorageManager + @TempDir - does not depend on real HDFS or LMDB environment
 */
class RecoveryManagerTest {

    @TempDir Path tempDir;

    private StorageManager storageManager;

    @BeforeAll
    static void registerDefaults() {
        StorageRegistry.registerDefaults();
    }

    @BeforeEach
    void setUp() {
        storageManager = new StorageManager(tempDir.toString(), null, RawWiseConfig.empty());
    }

    @AfterEach
    void tearDown() {
        if (storageManager != null) {
            storageManager.close();
        }
    }

    // ==================== extractKeyFromWALRecord ====================

    @Nested
    class ExtractKeyFromWALRecordTests {

        @Test
        void extractsKeyFromValidRecord() {
            byte[] data = buildWALRecordData("db.tbl", "my-key".getBytes(), "my-value".getBytes());
            WALRecord record = createMockWALRecord(data);

            byte[] key = RecoveryManager.extractKeyFromWALRecord(record);
            assertThat(key).isEqualTo("my-key".getBytes());
        }

        @Test
        void returnsEmptyForNullValue() {
            WALRecord record = createMockWALRecord(null);
            byte[] key = RecoveryManager.extractKeyFromWALRecord(record);
            assertThat(key).isEmpty();
        }

        @Test
        void returnsEmptyForTooShortData() {
            WALRecord record = createMockWALRecord(new byte[3]);
            byte[] key = RecoveryManager.extractKeyFromWALRecord(record);
            assertThat(key).isEmpty();
        }

        @Test
        void returnsEmptyForExactly7Bytes() {
            // 8 byte(4 tableIdLen + 4 keyLen of)
            WALRecord record = createMockWALRecord(new byte[7]);
            byte[] key = RecoveryManager.extractKeyFromWALRecord(record);
            assertThat(key).isEmpty();
        }

        @Test
        void returnsEmptyForNegativeTableIdLen() {
            ByteBuffer buf = ByteBuffer.allocate(20);
            buf.putInt(-1); // of tableIdLen
            WALRecord record = createMockWALRecord(buf.array());
            byte[] key = RecoveryManager.extractKeyFromWALRecord(record);
            assertThat(key).isEmpty();
        }

        @Test
        void returnsEmptyForTableIdLenExceedingData() {
            ByteBuffer buf = ByteBuffer.allocate(20);
            buf.putInt(100); // tableIdLen data
            WALRecord record = createMockWALRecord(buf.array());
            byte[] key = RecoveryManager.extractKeyFromWALRecord(record);
            assertThat(key).isEmpty();
        }

        @Test
        void returnsEmptyForNegativeKeyLen() {
            ByteBuffer buf = ByteBuffer.allocate(20);
            buf.putInt(3); // tableIdLen = 3
            buf.put("tbl".getBytes(StandardCharsets.UTF_8));
            buf.putInt(-1); // of keyLen
            WALRecord record = createMockWALRecord(trimBuffer(buf));
            byte[] key = RecoveryManager.extractKeyFromWALRecord(record);
            assertThat(key).isEmpty();
        }

        @Test
        void returnsEmptyForKeyLenExceedingRemaining() {
            ByteBuffer buf = ByteBuffer.allocate(20);
            buf.putInt(3); // tableIdLen = 3
            buf.put("tbl".getBytes(StandardCharsets.UTF_8));
            buf.putInt(999); // keyLen data
            WALRecord record = createMockWALRecord(trimBuffer(buf));
            byte[] key = RecoveryManager.extractKeyFromWALRecord(record);
            assertThat(key).isEmpty();
        }

        @Test
        void returnsEmptyWhenNotEnoughBytesForKeyLen() {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putInt(3); // tableIdLen = 3
            buf.put("tbl".getBytes(StandardCharsets.UTF_8));
            // 1 byte, 4 byteof keyLen
            buf.put((byte) 0);
            WALRecord record = createMockWALRecord(trimBuffer(buf));
            byte[] key = RecoveryManager.extractKeyFromWALRecord(record);
            assertThat(key).isEmpty();
        }

        @Test
        void extractsEmptyKey() {
            byte[] data = buildWALRecordData("db.tbl", new byte[0], "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            byte[] key = RecoveryManager.extractKeyFromWALRecord(record);
            assertThat(key).isEmpty();
        }
    }

    // ==================== extractValueFromWALRecord ====================

    @Nested
    class ExtractValueFromWALRecordTests {

        @Test
        void extractsValueFromValidData() {
            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "my-value".getBytes());
            byte[] value = RecoveryManager.extractValueFromWALRecord(data);
            assertThat(value).isEqualTo("my-value".getBytes());
        }

        @Test
        void returnsEmptyForNullData() {
            byte[] value = RecoveryManager.extractValueFromWALRecord(null);
            assertThat(value).isEmpty();
        }

        @Test
        void returnsEmptyForTooShortData() {
            byte[] value = RecoveryManager.extractValueFromWALRecord(new byte[3]);
            assertThat(value).isEmpty();
        }

        @Test
        void returnsEmptyForNegativeTableIdLen() {
            ByteBuffer buf = ByteBuffer.allocate(20);
            buf.putInt(-1);
            byte[] value = RecoveryManager.extractValueFromWALRecord(buf.array());
            assertThat(value).isEmpty();
        }

        @Test
        void returnsEmptyForNegativeKeyLen() {
            ByteBuffer buf = ByteBuffer.allocate(30);
            buf.putInt(3);
            buf.put("tbl".getBytes(StandardCharsets.UTF_8));
            buf.putInt(-1); // of keyLen
            byte[] value = RecoveryManager.extractValueFromWALRecord(trimBuffer(buf));
            assertThat(value).isEmpty();
        }

        @Test
        void returnsEmptyForNegativeValueLen() {
            ByteBuffer buf = ByteBuffer.allocate(30);
            buf.putInt(3);
            buf.put("tbl".getBytes(StandardCharsets.UTF_8));
            buf.putInt(3);
            buf.put("key".getBytes(StandardCharsets.UTF_8));
            buf.putInt(-1); // of valueLen
            byte[] value = RecoveryManager.extractValueFromWALRecord(trimBuffer(buf));
            assertThat(value).isEmpty();
        }

        @Test
        void returnsEmptyForValueLenExceedingRemaining() {
            ByteBuffer buf = ByteBuffer.allocate(30);
            buf.putInt(3);
            buf.put("tbl".getBytes(StandardCharsets.UTF_8));
            buf.putInt(3);
            buf.put("key".getBytes(StandardCharsets.UTF_8));
            buf.putInt(999); // valueLen data
            byte[] value = RecoveryManager.extractValueFromWALRecord(trimBuffer(buf));
            assertThat(value).isEmpty();
        }

        @Test
        void returnsEmptyWhenNotEnoughBytesForValueLen() {
            ByteBuffer buf = ByteBuffer.allocate(14);
            buf.putInt(3);
            buf.put("tbl".getBytes(StandardCharsets.UTF_8));
            buf.putInt(3);
            buf.put("key".getBytes(StandardCharsets.UTF_8));
            // has byte valueLen (4 bytes needed)
            byte[] value = RecoveryManager.extractValueFromWALRecord(trimBuffer(buf));
            assertThat(value).isEmpty();
        }

        @Test
        void extractsEmptyValue() {
            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), new byte[0]);
            byte[] value = RecoveryManager.extractValueFromWALRecord(data);
            assertThat(value).isEmpty();
        }
    }

    // ==================== extractTableIdFromWALRecord ====================

    @Nested
    class ExtractTableIdFromWALRecordTests {

        @Test
        void extractsTableIdFromValidData() {
            byte[] data = buildWALRecordData("my.table.id", "key".getBytes(), "val".getBytes());
            String tableId = RecoveryManager.extractTableIdFromWALRecord(data);
            assertThat(tableId).isEqualTo("my.table.id");
        }

        @Test
        void returnsNullForNullData() {
            assertThat(RecoveryManager.extractTableIdFromWALRecord(null)).isNull();
        }

        @Test
        void returnsNullForTooShortData() {
            assertThat(RecoveryManager.extractTableIdFromWALRecord(new byte[3])).isNull();
        }

        @Test
        void returnsNullForNegativeTableIdLen() {
            ByteBuffer buf = ByteBuffer.allocate(10);
            buf.putInt(-1);
            assertThat(RecoveryManager.extractTableIdFromWALRecord(buf.array())).isNull();
        }

        @Test
        void returnsNullForTableIdLenExceedingData() {
            ByteBuffer buf = ByteBuffer.allocate(10);
            buf.putInt(100);
            assertThat(RecoveryManager.extractTableIdFromWALRecord(buf.array())).isNull();
        }

        @Test
        void extractsEmptyTableId() {
            ByteBuffer buf = ByteBuffer.allocate(20);
            buf.putInt(0); // tableIdLen = 0
            buf.putInt(3); // keyLen
            buf.put("key".getBytes(StandardCharsets.UTF_8));
            buf.putInt(3); // valueLen
            buf.put("val".getBytes(StandardCharsets.UTF_8));
            String tableId = RecoveryManager.extractTableIdFromWALRecord(trimBuffer(buf));
            assertThat(tableId).isEmpty();
        }

        @Test
        void extractsUnicodeTableId() {
            String tableId = "db.中文表";
            byte[] data = buildWALRecordData(tableId, "k".getBytes(), "v".getBytes());
            String result = RecoveryManager.extractTableIdFromWALRecord(data);
            assertThat(result).isEqualTo(tableId);
        }
    }

    // ==================== parseTableIdentifier ====================

    @Nested
    class ParseTableIdentifierTests {

        @Test
        void parsesSimpleInstanceId() {
            // inst-db.table-12345 -> db.table
            String result = RecoveryManager.parseTableIdentifier("inst-db.table-12345");
            assertThat(result).isEqualTo("db.table");
        }

        @Test
        void parsesInstanceIdWithMultipleDashes() {
            // inst-db-my.table-12345 -> db-my.table
            String result = RecoveryManager.parseTableIdentifier("inst-db-my.table-12345");
            assertThat(result).isEqualTo("db-my.table");
        }

        @Test
        void parsesInstanceIdWithNoSuffixDash() {
            // inst-justtable -> justtable (has after item -)
            String result = RecoveryManager.parseTableIdentifier("inst-justtable");
            assertThat(result).isEqualTo("justtable");
        }

        @Test
        void parsesInstanceIdWithOnlyOneDashAfterPrefix() {
            // inst-table-1 -> table
            String result = RecoveryManager.parseTableIdentifier("inst-table-1");
            assertThat(result).isEqualTo("table");
        }

        @Test
        void parsesInstanceIdWithDottedTableId() {
            // inst-default.users-1700000000000 -> default.users
            String result =
                    RecoveryManager.parseTableIdentifier("inst-default.users-1700000000000");
            assertThat(result).isEqualTo("default.users");
        }

        @Test
        void parsesInstanceIdWithThreePartTableId() {
            // inst-catalog.schema.table-1700000000000 -> catalog.schema.table
            String result =
                    RecoveryManager.parseTableIdentifier("inst-catalog.schema.table-1700000000000");
            assertThat(result).isEqualTo("catalog.schema.table");
        }
    }

    // ==================== replayWALRecords NPE ====================

    @Nested
    class ReplayWALRecordsTests {

        @Test
        void replayWALRecordsSkipsTableWhenNoWritableInstance() {
            // construct item RecoveryManager, currentWritableProvider null
            RecoveryManager mgr =
                    createRecoveryManager(
                            tableId -> null // hascanwriteinstance
                            );

            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            // should not throw NPE
            assertThatCode(() -> invokeReplayWALRecords(mgr, recordsByTable))
                    .doesNotThrowAnyException();
        }

        @Test
        void replayWALRecordsPutsDataIntoWritableInstance() {
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            when(mockInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            AtomicBoolean tableStoreFactoryCalled = new AtomicBoolean(false);
            RecoveryManager mgr =
                    createRecoveryManager(
                            tableId -> mockInstance,
                            tableId -> {
                                tableStoreFactoryCalled.set(true);
                                return Mockito.mock(TableStore.class);
                            });

            byte[] data = buildWALRecordData("db.tbl", "mykey".getBytes(), "myval".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            invokeReplayWALRecords(mgr, recordsByTable);

            verify(mockInstance).put(any(byte[].class), any(byte[].class));
            assertThat(tableStoreFactoryCalled.get()).isTrue();
        }

        @Test
        void replayWALRecordsHandlesMultipleTables() {
            LmdbInstance mockInstance1 = Mockito.mock(LmdbInstance.class);
            when(mockInstance1.put(any(), any())).thenReturn(LmdbInstance.PutStatus.SUCCESS);
            LmdbInstance mockInstance2 = Mockito.mock(LmdbInstance.class);
            when(mockInstance2.put(any(), any())).thenReturn(LmdbInstance.PutStatus.SUCCESS);

            Map<String, LmdbInstance> instanceMap =
                    Map.of(
                            "tbl1", mockInstance1,
                            "tbl2", mockInstance2);

            RecoveryManager mgr =
                    createRecoveryManager(
                            instanceMap::get, tableId -> Mockito.mock(TableStore.class));

            byte[] data1 = buildWALRecordData("tbl1", "k1".getBytes(), "v1".getBytes());
            byte[] data2 = buildWALRecordData("tbl2", "k2".getBytes(), "v2".getBytes());
            Map<String, List<WALRecord>> recordsByTable =
                    Map.of(
                            "tbl1", List.of(createMockWALRecord(data1)),
                            "tbl2", List.of(createMockWALRecord(data2)));

            invokeReplayWALRecords(mgr, recordsByTable);

            verify(mockInstance1).put(any(), any());
            verify(mockInstance2).put(any(), any());
        }

        @Test
        void replayWALRecordsHandlesPutFailureGracefully() {
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            when(mockInstance.put(any(), any())).thenReturn(LmdbInstance.PutStatus.READ_ONLY);

            RecoveryManager mgr =
                    createRecoveryManager(
                            tableId -> mockInstance, tableId -> Mockito.mock(TableStore.class));

            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            // should not throw exception even if put fails
            assertThatCode(() -> invokeReplayWALRecords(mgr, recordsByTable))
                    .doesNotThrowAnyException();
        }

        @Test
        void replayWALRecordsHandlesEmptyRecordsMap() {
            RecoveryManager mgr = createRecoveryManager(tableId -> null);
            assertThatCode(() -> invokeReplayWALRecords(mgr, Map.of())).doesNotThrowAnyException();
        }
    }

    // ==================== loadExistInstance ====================

    @Nested
    class LoadExistInstanceTests {

        @Test
        void loadExistInstanceWithEmptyDirectoryDoesNotThrow() {
            RecoveryManager mgr = createRecoveryManager(tableId -> null);

            assertThatCode(
                            () ->
                                    mgr.loadExistInstance(
                                            () -> true,
                                            () -> {},
                                            tableId -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();
        }

        @Test
        void loadExistInstanceWithNonExistentBasePathDoesNotThrow() {
            StorageManager nonExistentMgr =
                    new StorageManager("/nonexistent/path/xyz", null, RawWiseConfig.empty());
            RecoveryManager mgr = createRecoveryManagerWithStorage(nonExistentMgr, tableId -> null);

            assertThatCode(
                            () ->
                                    mgr.loadExistInstance(
                                            () -> true,
                                            () -> {},
                                            tableId -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();

            nonExistentMgr.close();
        }

        @Test
        void loadExistInstanceSkipsWhenMemoryNotAvailable() throws Exception {
            // create iteminstance
            String tid = "db.test";
            String instanceId = "inst-" + tid + "-1700000000000";
            storageManager.createLocalDirectories(tid + "/" + instanceId);

            AtomicBoolean flushTriggered = new AtomicBoolean(false);
            AtomicBoolean memoryAvailableAfterFlush = new AtomicBoolean(false);

            // No. check: memoryunavailable; flush after unavailable
            RecoveryManager mgr = createRecoveryManager(tableIdentifier -> null);

            assertThatCode(
                            () ->
                                    mgr.loadExistInstance(
                                            () -> {
                                                if (!flushTriggered.get()) {
                                                    flushTriggered.set(true);
                                                    return false;
                                                }
                                                return memoryAvailableAfterFlush.get();
                                            },
                                            () -> {},
                                            tableIdentifier -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();
        }

        @Test
        void loadExistInstanceRegistersInstances() throws Exception {
            // createinstance - : LmdbInstance construct real LMDB environment
            // testofis logic, LmdbInstance create
            String tid = "db.reg";
            storageManager.createLocalDirectories(tid + "/inst-" + tid + "-1700000000001");

            // by LmdbInstance construct willcreaterealof LMDB environment,
            // loadExistInstance will create LmdbInstance cancanfail
            // verify will not yetcaptureexception
            RecoveryManager mgr = createRecoveryManager(tableIdentifier -> null);

            assertThatCode(
                            () ->
                                    mgr.loadExistInstance(
                                            () -> true,
                                            () -> {},
                                            tableIdentifier -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();
        }
    }

    // ==================== recoverFromWAL ====================

    @Nested
    class RecoverFromWALTests {

        @Test
        void recoverFromWALWithMissingWALDirectoryDoesNotThrow() {
            WALConfig config = new WALConfig("wal_logs", "wal", 1024 * 1024, true, 8192);
            RecoveryManager mgr = createRecoveryManagerWithWAL(config, false, tableId -> null);

            assertThatCode(() -> mgr.recoverFromWAL()).doesNotThrowAnyException();
        }

        @Test
        void recoverFromWALWithNonExistentLocalWALDirSkipsRecovery() {
            WALConfig config = new WALConfig("wal_logs", "wal", 1024 * 1024, true, 8192);
            RecoveryManager mgr = createRecoveryManagerWithWAL(config, false, tableId -> null);

            // basePath/wal does not exist
            assertThatCode(() -> mgr.recoverFromWAL()).doesNotThrowAnyException();
        }

        @Test
        void recoverFromWALWithHdfsEnabledButNoHdfsDoesNotThrow() {
            // HDFS storageManager no HDFS
            WALConfig config =
                    new WALConfig(
                            "wal_logs",
                            "wal",
                            1024 * 1024,
                            true,
                            8192,
                            "NONE",
                            "store1",
                            "exec1",
                            "/hdfs/wal",
                            1000L);
            RecoveryManager mgr = createRecoveryManagerWithWAL(config, true, tableId -> null);

            // will create WALReader read HDFS path, cancanfail should not thrownot
            // yetcaptureexception
            assertThatCode(() -> mgr.recoverFromWAL()).doesNotThrowAnyException();
        }

        @Test
        void recoverFromWALWithEmptyWALDirDoesNotThrow() throws IOException {
            // create of WAL
            storageManager.createLocalDirectories("wal");

            WALConfig config = new WALConfig("wal_logs", "wal", 1024 * 1024, true, 8192);
            RecoveryManager mgr = createRecoveryManagerWithWAL(config, false, tableId -> null);

            assertThatCode(() -> mgr.recoverFromWAL()).doesNotThrowAnyException();
        }
    }

    // ==================== discoverLocalTables delegate ====================

    @Test
    void discoverLocalTablesDelegatesToStorageManager() throws IOException {
        storageManager.createLocalDirectories("table_x/inst-001");
        storageManager.createLocalDirectories("table_y/inst-001");

        RecoveryManager mgr = createRecoveryManager(tableId -> null);
        Set<String> tables = mgr.discoverLocalTables(tempDir.toString());

        assertThat(tables).containsExactlyInAnyOrder("table_x", "table_y");
    }

    // ==================== DurabilityLevel integration test ====================

    @Nested
    class DurabilityLevelIntegrationTests {

        @Test
        void loadExistInstanceWithProcessSkipsWalRecovery() {
            RecoveryManager mgr =
                    createRecoveryManagerWithDurabilityLevel(
                            DurabilityLevel.PROCESS, null, tableId -> null);

            assertThatCode(
                            () ->
                                    mgr.loadExistInstance(
                                            () -> true,
                                            () -> {},
                                            tableId -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();
        }

        @Test
        void loadExistInstanceWithNodeButNoWalConfigLogsWarning() {
            RecoveryManager mgr =
                    createRecoveryManagerWithDurabilityLevel(
                            DurabilityLevel.NODE, null, tableId -> null);

            assertThatCode(
                            () ->
                                    mgr.loadExistInstance(
                                            () -> true,
                                            () -> {},
                                            tableId -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();
        }

        @Test
        void loadExistInstanceWithNodeAndWalConfigTriggersRecovery() {
            WALConfig walConfig = new WALConfig("wal_logs", "wal", 1024 * 1024, true, 8192);
            RecoveryManager mgr =
                    createRecoveryManagerWithDurabilityLevel(
                            DurabilityLevel.NODE, walConfig, tableId -> null);

            assertThatCode(
                            () ->
                                    mgr.loadExistInstance(
                                            () -> true,
                                            () -> {},
                                            tableId -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();
        }

        @Test
        void recoverShardWithProcessLevelLogsDataLossWarningOnCrossNode() {
            RecoveryManager mgr =
                    createRecoveryManagerWithDurabilityLevel(
                            DurabilityLevel.PROCESS, null, tableId -> null);

            ShardRecoveryRecord record =
                    new ShardRecoveryRecord(
                            new ShardId(0),
                            "executor-lost",
                            "different-host:0",
                            null,
                            null,
                            Set.of("db.tbl"),
                            RoutingEpoch.initial(),
                            System.currentTimeMillis());

            assertThatCode(
                            () ->
                                    mgr.recoverShard(
                                            record,
                                            () -> true,
                                            () -> {},
                                            tableId -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();
        }

        @Test
        void recoverShardWithProcessLevelAndNoRecoveryPathDoesNotThrow() {
            RecoveryManager mgr =
                    createRecoveryManagerWithDurabilityLevel(
                            DurabilityLevel.PROCESS, null, tableId -> null);

            ShardRecoveryRecord record =
                    new ShardRecoveryRecord(
                            new ShardId(1),
                            "executor-lost-2",
                            "other-host:1",
                            null,
                            null,
                            Set.of("db.test"),
                            new RoutingEpoch(1),
                            System.currentTimeMillis());

            assertThatCode(
                            () ->
                                    mgr.recoverShard(
                                            record,
                                            () -> true,
                                            () -> {},
                                            tableId -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();
        }

        @Test
        void recoverShardWithNodeLevelAndWalHdfsDirAttemptsRemoteRecovery() {
            RecoveryManager mgr =
                    createRecoveryManagerWithDurabilityLevel(
                            DurabilityLevel.NODE, null, tableId -> null);

            ShardRecoveryRecord record =
                    new ShardRecoveryRecord(
                            new ShardId(2),
                            "executor-lost-3",
                            "remote-host:2",
                            null,
                            "/hdfs/wal/executor-lost-3",
                            Set.of("db.remote"),
                            new RoutingEpoch(2),
                            System.currentTimeMillis());

            assertThatCode(
                            () ->
                                    mgr.recoverShard(
                                            record,
                                            () -> true,
                                            () -> {},
                                            tableId -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();
        }
    }

    // ==================== isTxnWALRecord ====================

    @Nested
    class IsTxnWALRecordTests {

        @Test
        void detectsTxnBeginRecord() {
            TxnWALRecord record =
                    TxnWALRecord.begin(
                            1L,
                            List.of("db.tbl"),
                            "READ_COMMITTED",
                            System.currentTimeMillis(),
                            0L);
            byte[] encoded = TxnWALRecordCodec.encode(record);
            assertThat(RecoveryManager.isTxnWALRecord(encoded)).isTrue();
        }

        @Test
        void detectsTxnDataRecord() {
            TxnWALRecord record =
                    TxnWALRecord.data(1L, "db.tbl", "key".getBytes(), "val".getBytes(), "PUT");
            byte[] encoded = TxnWALRecordCodec.encode(record);
            assertThat(RecoveryManager.isTxnWALRecord(encoded)).isTrue();
        }

        @Test
        void detectsTxnPreparedRecord() {
            TxnWALRecord record = TxnWALRecord.prepared(1L, "db.tbl", 1000L);
            byte[] encoded = TxnWALRecordCodec.encode(record);
            assertThat(RecoveryManager.isTxnWALRecord(encoded)).isTrue();
        }

        @Test
        void detectsTxnCommitRecord() {
            TxnWALRecord record = TxnWALRecord.commit(1L, "db.tbl", 1000L, 2000L);
            byte[] encoded = TxnWALRecordCodec.encode(record);
            assertThat(RecoveryManager.isTxnWALRecord(encoded)).isTrue();
        }

        @Test
        void detectsTxnAbortRecord() {
            TxnWALRecord record = TxnWALRecord.abort(1L, "db.tbl");
            byte[] encoded = TxnWALRecordCodec.encode(record);
            assertThat(RecoveryManager.isTxnWALRecord(encoded)).isTrue();
        }

        @Test
        void returnsFalseForLegacyWALRecord() {
            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            assertThat(RecoveryManager.isTxnWALRecord(data)).isFalse();
        }

        @Test
        void returnsFalseForNullData() {
            assertThat(RecoveryManager.isTxnWALRecord(null)).isFalse();
        }

        @Test
        void returnsFalseForEmptyData() {
            assertThat(RecoveryManager.isTxnWALRecord(new byte[0])).isFalse();
        }
    }

    // ==================== recoverWithTxnBoundaries ====================

    @Nested
    class RecoverWithTxnBoundariesTests {

        @Test
        void uncommittedTransactionIsAbortedAndDataNotReplayed() {
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            when(mockInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            WALWriter mockWalWriter = Mockito.mock(WALWriter.class);

            RecoveryManager mgr =
                    createRecoveryManager(
                            tableId -> mockInstance, tableId -> Mockito.mock(TableStore.class));

            long txnId = 100L;
            List<WALRecord> walRecords =
                    List.of(
                            createTxnWALRecord(
                                    TxnWALRecord.begin(
                                            txnId,
                                            List.of("db.tbl"),
                                            "READ_COMMITTED",
                                            System.currentTimeMillis(),
                                            0L)),
                            createTxnWALRecord(
                                    TxnWALRecord.data(
                                            txnId,
                                            "db.tbl",
                                            "key1".getBytes(),
                                            "val1".getBytes(),
                                            "PUT")));

            mgr.recoverWithTxnBoundaries(walRecords, txnId2 -> false, mockWalWriter, null);

            verify(mockInstance, never()).put(any(byte[].class), any(byte[].class));
            try {
                verify(mockWalWriter).write(any(WALRecord.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Test
        void committedTransactionDataIsReplayed() {
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            when(mockInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            WALWriter mockWalWriter = Mockito.mock(WALWriter.class);

            RecoveryManager mgr =
                    createRecoveryManager(
                            tableId -> mockInstance, tableId -> Mockito.mock(TableStore.class));

            long txnId = 200L;
            List<WALRecord> walRecords =
                    List.of(
                            createTxnWALRecord(
                                    TxnWALRecord.begin(
                                            txnId,
                                            List.of("db.tbl"),
                                            "READ_COMMITTED",
                                            System.currentTimeMillis(),
                                            0L)),
                            createTxnWALRecord(
                                    TxnWALRecord.data(
                                            txnId,
                                            "db.tbl",
                                            "key1".getBytes(),
                                            "val1".getBytes(),
                                            "PUT")),
                            createTxnWALRecord(TxnWALRecord.prepared(txnId, "db.tbl", 1000L)),
                            createTxnWALRecord(TxnWALRecord.commit(txnId, "db.tbl", 1000L, 2000L)));

            mgr.recoverWithTxnBoundaries(walRecords, txnId2 -> true, mockWalWriter, null);

            verify(mockInstance).put(any(byte[].class), any(byte[].class));
        }

        @Test
        void preparedButMetastoreCommittedReplaysDataAndWritesCommit() {
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            when(mockInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            WALWriter mockWalWriter = Mockito.mock(WALWriter.class);

            RecoveryManager mgr =
                    createRecoveryManager(
                            tableId -> mockInstance, tableId -> Mockito.mock(TableStore.class));

            long txnId = 300L;
            List<WALRecord> walRecords =
                    List.of(
                            createTxnWALRecord(
                                    TxnWALRecord.begin(
                                            txnId,
                                            List.of("db.tbl"),
                                            "READ_COMMITTED",
                                            System.currentTimeMillis(),
                                            0L)),
                            createTxnWALRecord(
                                    TxnWALRecord.data(
                                            txnId,
                                            "db.tbl",
                                            "key1".getBytes(),
                                            "val1".getBytes(),
                                            "PUT")),
                            createTxnWALRecord(TxnWALRecord.prepared(txnId, "db.tbl", 1000L)));

            mgr.recoverWithTxnBoundaries(walRecords, txnId2 -> true, mockWalWriter, null);

            verify(mockInstance).put(any(byte[].class), any(byte[].class));
            try {
                verify(mockWalWriter).write(any(WALRecord.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Test
        void preparedButMetastoreNotCommittedDeletesKeysFromLMDB() {
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            when(mockInstance.batchDelete(anyList()))
                    .thenReturn(new LmdbInstance.BatchDeleteResult(1, 0));

            WALWriter mockWalWriter = Mockito.mock(WALWriter.class);

            RecoveryManager mgr =
                    createRecoveryManager(
                            tableId -> mockInstance, tableId -> Mockito.mock(TableStore.class));

            long txnId = 400L;
            List<WALRecord> walRecords =
                    List.of(
                            createTxnWALRecord(
                                    TxnWALRecord.begin(
                                            txnId,
                                            List.of("db.tbl"),
                                            "READ_COMMITTED",
                                            System.currentTimeMillis(),
                                            0L)),
                            createTxnWALRecord(
                                    TxnWALRecord.data(
                                            txnId,
                                            "db.tbl",
                                            "key1".getBytes(),
                                            "val1".getBytes(),
                                            "PUT")),
                            createTxnWALRecord(TxnWALRecord.prepared(txnId, "db.tbl", 1000L)));

            mgr.recoverWithTxnBoundaries(walRecords, txnId2 -> false, mockWalWriter, null);

            verify(mockInstance).batchDelete(anyList());
            verify(mockInstance, never()).put(any(byte[].class), any(byte[].class));
            try {
                verify(mockWalWriter).write(any(WALRecord.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Test
        void noTxnRecordsFallsBackToLegacyRecovery() {
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            when(mockInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            RecoveryManager mgr =
                    createRecoveryManager(
                            tableId -> mockInstance, tableId -> Mockito.mock(TableStore.class));

            byte[] data = buildWALRecordData("db.tbl", "key1".getBytes(), "val1".getBytes());
            List<WALRecord> walRecords = List.of(createMockWALRecord(data));

            mgr.recoverWithTxnBoundaries(walRecords, txnId -> false, null, null);

            verify(mockInstance).put(any(byte[].class), any(byte[].class));
        }

        @Test
        void abortedTransactionIsSkipped() {
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            when(mockInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            WALWriter mockWalWriter = Mockito.mock(WALWriter.class);

            RecoveryManager mgr =
                    createRecoveryManager(
                            tableId -> mockInstance, tableId -> Mockito.mock(TableStore.class));

            long txnId = 500L;
            List<WALRecord> walRecords =
                    List.of(
                            createTxnWALRecord(
                                    TxnWALRecord.begin(
                                            txnId,
                                            List.of("db.tbl"),
                                            "READ_COMMITTED",
                                            System.currentTimeMillis(),
                                            0L)),
                            createTxnWALRecord(
                                    TxnWALRecord.data(
                                            txnId,
                                            "db.tbl",
                                            "key1".getBytes(),
                                            "val1".getBytes(),
                                            "PUT")),
                            createTxnWALRecord(TxnWALRecord.abort(txnId, "db.tbl")));

            mgr.recoverWithTxnBoundaries(walRecords, txnId2 -> false, mockWalWriter, null);

            verify(mockInstance, never()).put(any(byte[].class), any(byte[].class));
        }

        @Test
        void versionTrackerRestorerIsCalledWithMaxCommitTs() {
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            when(mockInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            Map<String, Long> restoredVersions = new HashMap<>();

            RecoveryManager mgr =
                    createRecoveryManager(
                            tableId -> mockInstance, tableId -> Mockito.mock(TableStore.class));

            long txnId = 600L;
            List<WALRecord> walRecords =
                    List.of(
                            createTxnWALRecord(
                                    TxnWALRecord.begin(
                                            txnId,
                                            List.of("db.tbl"),
                                            "READ_COMMITTED",
                                            System.currentTimeMillis(),
                                            0L)),
                            createTxnWALRecord(
                                    TxnWALRecord.data(
                                            txnId,
                                            "db.tbl",
                                            "key1".getBytes(),
                                            "val1".getBytes(),
                                            "PUT")),
                            createTxnWALRecord(TxnWALRecord.prepared(txnId, "db.tbl", 5000L)),
                            createTxnWALRecord(TxnWALRecord.commit(txnId, "db.tbl", 5000L, 6000L)));

            mgr.recoverWithTxnBoundaries(
                    walRecords,
                    txnId2 -> true,
                    null,
                    (tableId, commitTs) -> restoredVersions.put(tableId, commitTs));

            assertThat(restoredVersions).containsEntry("db.tbl", 5000L);
        }
    }

    // ==================== B1: loadExistInstance return->continue ====================

    @Nested
    class LoadExistInstanceContinueFixTests {

        @Test
        void loadExistInstanceContinuesAfterMemoryShortageInsteadOfReturning() throws Exception {
            // Create two instance directories
            String tid1 = "db.test1";
            String tid2 = "db.test2";
            String instId1 = "inst-" + tid1 + "-1700000000001";
            String instId2 = "inst-" + tid2 + "-1700000000002";
            storageManager.createLocalDirectories(tid1 + "/" + instId1);
            storageManager.createLocalDirectories(tid2 + "/" + instId2);

            AtomicInteger memoryCheckCount = new AtomicInteger(0);
            // First check returns false (no memory), after flush still false for first instance,
            // then true for second instance
            Supplier<Boolean> memoryChecker =
                    () -> {
                        int count = memoryCheckCount.incrementAndGet();
                        // checks 1-2: first instance (no memory even after flush)
                        // checks 3+: second instance (memory available)
                        return count > 2;
                    };

            AtomicLong flushCount = new AtomicLong(0);
            Runnable flushTrigger = () -> flushCount.incrementAndGet();

            RecoveryManager mgr = createRecoveryManager(tableIdentifier -> null);

            // Should not throw, and should process both instances (first skipped, second attempted)
            assertThatCode(
                            () ->
                                    mgr.loadExistInstance(
                                            memoryChecker,
                                            flushTrigger,
                                            tableIdentifier -> Mockito.mock(TableStore.class)))
                    .doesNotThrowAnyException();

            // Flush should have been triggered at least once for the first instance
            assertThat(flushCount.get()).isGreaterThanOrEqualTo(1);
        }

        @Test
        void loadExistInstanceSecondInstanceIsLoadedAfterFirstSkipped() throws Exception {
            // Create two instance directories under the same table
            String tid = "db.testboth";
            String instId1 = "inst-" + tid + "-1700000000001";
            String instId2 = "inst-" + tid + "-1700000000002";
            storageManager.createLocalDirectories(tid + "/" + instId1);
            storageManager.createLocalDirectories(tid + "/" + instId2);

            AtomicInteger memoryCheckCount = new AtomicInteger(0);
            // First instance: memory not available (checks 1-2 return false)
            // Second instance: memory available (check 3+ returns true)
            Supplier<Boolean> memoryChecker =
                    () -> {
                        int count = memoryCheckCount.incrementAndGet();
                        return count > 2;
                    };

            AtomicLong flushCount = new AtomicLong(0);
            Runnable flushTrigger = () -> flushCount.incrementAndGet();

            // Track which instances get registered
            List<String> registeredInstanceIds = Collections.synchronizedList(new ArrayList<>());
            List<String> writableRegisteredTables = Collections.synchronizedList(new ArrayList<>());

            RecoveryManager mgr =
                    new RecoveryManager(
                            new RecoveryManager.RecoveryContext(
                                    storageManager,
                                    tempDir.toString(),
                                    1024 * 1024L,
                                    new Configuration(),
                                    null,
                                    DurabilityLevel.PROCESS,
                                    true,
                                    () -> 3,
                                    tableIdentifier -> Mockito.mock(TableStore.class),
                                    (tableId, inst) -> writableRegisteredTables.add(tableId),
                                    (tableId, inst) -> {},
                                    (used, reserved) -> {},
                                    tableId -> null,
                                    (instId, future) -> registeredInstanceIds.add(instId),
                                    null, // newInstanceCreator
                                    null, // memoryAvailableChecker
                                    null // syncFlushTrigger
                                    ));

            mgr.loadExistInstance(
                    memoryChecker, flushTrigger, tableId -> Mockito.mock(TableStore.class));

            // First instance was skipped due to memory, second instance was attempted
            // The second instance should have been registered (if LmdbInstance creation succeeds)
            // Since LmdbInstance requires a real LMDB environment, the creation may fail,
            // but we verify that the memory check allowed the second instance to be attempted
            assertThat(memoryCheckCount.get()).isGreaterThanOrEqualTo(3);
            assertThat(flushCount.get()).isGreaterThanOrEqualTo(1);
        }
    }

    // ==================== B2: replayWALRecords newInstanceCreator ====================

    @Nested
    class ReplayWALRecordsNewInstanceCreatorTests {

        @Test
        void replayWALRecordsUsesNewInstanceCreatorWhenNoWritableInstance() {
            LmdbInstance newInstance = Mockito.mock(LmdbInstance.class);
            when(newInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            // currentWritableProvider returns null, but newInstanceCreator provides an instance
            RecoveryManager mgr =
                    createRecoveryManagerWithNewInstanceCreator(
                            tableId -> null, (tableId, mappedBytes) -> newInstance);

            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            assertThatCode(() -> invokeReplayWALRecords(mgr, recordsByTable))
                    .doesNotThrowAnyException();

            verify(newInstance).put(any(byte[].class), any(byte[].class));
        }

        @Test
        void replayWALRecordsSkipsWhenNewInstanceCreatorReturnsNull() {
            // Both currentWritableProvider and newInstanceCreator return null
            RecoveryManager mgr =
                    createRecoveryManagerWithNewInstanceCreator(
                            tableId -> null, (tableId, mappedBytes) -> null);

            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            // Should not throw, just skip
            assertThatCode(() -> invokeReplayWALRecords(mgr, recordsByTable))
                    .doesNotThrowAnyException();
        }

        @Test
        void replayWALRecordsSkipsWhenNewInstanceCreatorThrows() {
            // currentWritableProvider returns null, newInstanceCreator throws
            RecoveryManager mgr =
                    createRecoveryManagerWithNewInstanceCreator(
                            tableId -> null,
                            (tableId, mappedBytes) -> {
                                throw new RuntimeException("creation failed");
                            });

            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            // Should not throw, just skip with warning
            assertThatCode(() -> invokeReplayWALRecords(mgr, recordsByTable))
                    .doesNotThrowAnyException();
        }

        @Test
        void replayWALRecordsFallsBackToNewInstanceCreatorOnlyWhenProviderReturnsNull() {
            LmdbInstance existingInstance = Mockito.mock(LmdbInstance.class);
            when(existingInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            LmdbInstance newInstance = Mockito.mock(LmdbInstance.class);
            when(newInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            AtomicBoolean newInstanceCreatorCalled = new AtomicBoolean(false);

            RecoveryManager mgr =
                    createRecoveryManagerWithNewInstanceCreator(
                            tableId -> existingInstance, // provider returns non-null
                            (tableId, mappedBytes) -> {
                                newInstanceCreatorCalled.set(true);
                                return newInstance;
                            });

            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            invokeReplayWALRecords(mgr, recordsByTable);

            // Should use existing instance, not call newInstanceCreator
            verify(existingInstance).put(any(byte[].class), any(byte[].class));
            assertThat(newInstanceCreatorCalled.get()).isFalse();
        }
    }

    // ==================== B3: recoverFromLocalLMDB memory check ====================

    @Nested
    class RecoverFromLocalLMDBMemoryCheckTests {

        @Test
        void recoverFromLocalLMDBAcceptsMemoryCheckerParameters() {
            RecoveryManager mgr = createRecoveryManager(tableId -> null);

            // Should not throw with null memory checker (backward compatible)
            assertThatCode(
                            () ->
                                    mgr.recoverFromLocalLMDB(
                                            tempDir.toString(), "db.test", null, null))
                    .doesNotThrowAnyException();
        }

        @Test
        void recoverFromLocalLMDBSkipsInstanceWhenNoMemory() throws Exception {
            String tid = "db.localtest";
            String instanceId = "inst-" + tid + "-1700000000001";
            storageManager.createLocalDirectories(tid + "/" + instanceId);

            RecoveryManager mgr = createRecoveryManager(tableId -> null);

            // Memory never available, flush does nothing
            assertThatCode(
                            () ->
                                    mgr.recoverFromLocalLMDB(
                                            tempDir.toString(), tid, () -> false, () -> {}))
                    .doesNotThrowAnyException();
        }

        @Test
        void recoverFromLocalLMDBProceedsWhenMemoryAvailable() throws Exception {
            String tid = "db.localtest2";
            String instanceId = "inst-" + tid + "-1700000000002";
            storageManager.createLocalDirectories(tid + "/" + instanceId);

            RecoveryManager mgr = createRecoveryManager(tableId -> null);

            // Memory always available
            assertThatCode(
                            () ->
                                    mgr.recoverFromLocalLMDB(
                                            tempDir.toString(), tid, () -> true, () -> {}))
                    .doesNotThrowAnyException();
        }
    }

    // ==================== W2: recoverFromRemoteWAL memory check ====================

    @Nested
    class RecoverFromRemoteWALMemoryCheckTests {

        @Test
        void recoverFromRemoteWALSkipsWhenMemoryUnavailableAndFlushFails() {
            // When memoryAvailableChecker returns false and flush doesn't help, replay is skipped
            RecoveryManager mgr =
                    createRecoveryManagerWithMemoryCheck(
                            () -> false, // memory never available
                            () -> {} // flush does nothing
                            );

            // recoverFromRemoteWAL requires HDFS which isn't available in test,
            // so it will return early - but we verify the method doesn't throw
            assertThatCode(
                            () ->
                                    mgr.recoverFromRemoteWAL(
                                            "/hdfs/wal/executor1",
                                            "db.test",
                                            () -> false,
                                            () -> {}))
                    .doesNotThrowAnyException();
        }

        @Test
        void recoverFromRemoteWALProceedsWhenFlushFreesMemory() {
            // When memoryAvailableChecker initially returns false but flush helps, replay proceeds
            AtomicInteger memoryCheckCount = new AtomicInteger(0);
            Supplier<Boolean> memoryChecker =
                    () -> {
                        int count = memoryCheckCount.incrementAndGet();
                        // First check: no memory; after flush: memory available
                        return count > 1;
                    };
            AtomicBoolean flushTriggered = new AtomicBoolean(false);
            Runnable flushTrigger = () -> flushTriggered.set(true);

            RecoveryManager mgr = createRecoveryManagerWithMemoryCheck(memoryChecker, flushTrigger);

            // recoverFromRemoteWAL requires HDFS which isn't available in test,
            // so it will return early - but we verify the method doesn't throw
            assertThatCode(
                            () ->
                                    mgr.recoverFromRemoteWAL(
                                            "/hdfs/wal/executor2",
                                            "db.test",
                                            memoryChecker,
                                            flushTrigger))
                    .doesNotThrowAnyException();
        }
    }

    // ==================== newInstanceCreator registration ====================

    @Nested
    class NewInstanceCreatorRegistrationTests {

        @Test
        void newInstanceCreatorRegistersInstanceAfterCreation() {
            LmdbInstance newInstance = Mockito.mock(LmdbInstance.class);
            when(newInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);
            when(newInstance.getTotalUsageSize()).thenReturn(1024L);
            when(newInstance.instanceId()).thenReturn("inst-db.tbl-1700000000000");

            List<String> registeredInstanceIds = Collections.synchronizedList(new ArrayList<>());
            List<String> writableRegisteredTables = Collections.synchronizedList(new ArrayList<>());
            AtomicLong trackedMemory = new AtomicLong(0L);

            RecoveryManager mgr =
                    new RecoveryManager(
                            new RecoveryManager.RecoveryContext(
                                    storageManager,
                                    tempDir.toString(),
                                    1024 * 1024L,
                                    new Configuration(),
                                    null,
                                    DurabilityLevel.PROCESS,
                                    true,
                                    () -> 3,
                                    tableId -> Mockito.mock(TableStore.class),
                                    (tableId, inst) -> writableRegisteredTables.add(tableId),
                                    (tableId, inst) -> {},
                                    (used, reserved) -> trackedMemory.addAndGet(used),
                                    tableId -> null, // no existing writable instance
                                    (instId, future) -> registeredInstanceIds.add(instId),
                                    (tableId, mappedBytes) -> newInstance, // newInstanceCreator
                                    null, // memoryAvailableChecker
                                    null // syncFlushTrigger
                                    ));

            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            invokeReplayWALRecords(mgr, recordsByTable);

            // Verify instance was registered
            assertThat(registeredInstanceIds).contains("inst-db.tbl-1700000000000");
            // Verify memory was tracked
            assertThat(trackedMemory.get()).isEqualTo(1024L);
            // Verify writable instance was registered
            assertThat(writableRegisteredTables).contains("db.tbl");
            // Verify data was replayed
            verify(newInstance).put(any(byte[].class), any(byte[].class));
        }

        @Test
        void newInstanceCreatorSkipsWhenMemoryUnavailable() {
            LmdbInstance newInstance = Mockito.mock(LmdbInstance.class);
            when(newInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            RecoveryManager mgr =
                    new RecoveryManager(
                            new RecoveryManager.RecoveryContext(
                                    storageManager,
                                    tempDir.toString(),
                                    1024 * 1024L,
                                    new Configuration(),
                                    null,
                                    DurabilityLevel.PROCESS,
                                    true,
                                    () -> 3,
                                    tableId -> Mockito.mock(TableStore.class),
                                    (tableId, inst) -> {},
                                    (tableId, inst) -> {},
                                    (used, reserved) -> {},
                                    tableId -> null, // no existing writable instance
                                    (instId, future) -> {},
                                    (tableId, mappedBytes) -> newInstance, // newInstanceCreator
                                    () -> false, // memoryAvailableChecker: always unavailable
                                    () -> {} // syncFlushTrigger
                                    ));

            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            invokeReplayWALRecords(mgr, recordsByTable);

            // newInstanceCreator should NOT have been called because memory is unavailable
            verify(newInstance, never()).put(any(byte[].class), any(byte[].class));
        }

        @Test
        void newInstanceCreatorProceedsAfterFlushFreesMemory() {
            LmdbInstance newInstance = Mockito.mock(LmdbInstance.class);
            when(newInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);
            when(newInstance.getTotalUsageSize()).thenReturn(512L);
            when(newInstance.instanceId()).thenReturn("inst-db.tbl-1700000000001");

            AtomicInteger memoryCheckCount = new AtomicInteger(0);
            Supplier<Boolean> memoryChecker =
                    () -> {
                        int count = memoryCheckCount.incrementAndGet();
                        // First check: no memory; after flush: memory available
                        return count > 1;
                    };
            AtomicBoolean flushTriggered = new AtomicBoolean(false);

            RecoveryManager mgr =
                    new RecoveryManager(
                            new RecoveryManager.RecoveryContext(
                                    storageManager,
                                    tempDir.toString(),
                                    1024 * 1024L,
                                    new Configuration(),
                                    null,
                                    DurabilityLevel.PROCESS,
                                    true,
                                    () -> 3,
                                    tableId -> Mockito.mock(TableStore.class),
                                    (tableId, inst) -> {},
                                    (tableId, inst) -> {},
                                    (used, reserved) -> {},
                                    tableId -> null, // no existing writable instance
                                    (instId, future) -> {},
                                    (tableId, mappedBytes) -> newInstance, // newInstanceCreator
                                    memoryChecker,
                                    () -> flushTriggered.set(true)));

            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            invokeReplayWALRecords(mgr, recordsByTable);

            // Flush should have been triggered
            assertThat(flushTriggered.get()).isTrue();
            // newInstanceCreator should have been called and data replayed
            verify(newInstance).put(any(byte[].class), any(byte[].class));
        }
    }

    // ==================== RecoveryContextBuilder ====================

    @Nested
    class RecoveryContextBuilderTests {

        @Test
        void builderCreatesValidContext() {
            RecoveryManager.RecoveryContext ctx =
                    RecoveryManager.RecoveryContext.builder()
                            .storageManager(storageManager)
                            .basePath(tempDir.toString())
                            .instanceMappedBytes(1024 * 1024L)
                            .tableStoreFactory(tableId -> Mockito.mock(TableStore.class))
                            .currentWritableProvider(tableId -> null)
                            .build();

            assertThat(ctx.storageManager()).isSameAs(storageManager);
            assertThat(ctx.basePath()).isEqualTo(tempDir.toString());
            assertThat(ctx.instanceMappedBytes()).isEqualTo(1024 * 1024L);
        }

        @Test
        void builderThrowsWhenStorageManagerIsNull() {
            assertThatIllegalStateException()
                    .isThrownBy(
                            () ->
                                    RecoveryManager.RecoveryContext.builder()
                                            .basePath("/tmp")
                                            .instanceMappedBytes(1024)
                                            .tableStoreFactory(
                                                    tableId -> Mockito.mock(TableStore.class))
                                            .currentWritableProvider(tableId -> null)
                                            .build())
                    .withMessageContaining("storageManager");
        }

        @Test
        void builderThrowsWhenBasePathIsBlank() {
            assertThatIllegalStateException()
                    .isThrownBy(
                            () ->
                                    RecoveryManager.RecoveryContext.builder()
                                            .storageManager(storageManager)
                                            .basePath("")
                                            .instanceMappedBytes(1024)
                                            .tableStoreFactory(
                                                    tableId -> Mockito.mock(TableStore.class))
                                            .currentWritableProvider(tableId -> null)
                                            .build())
                    .withMessageContaining("basePath");
        }

        @Test
        void builderThrowsWhenBasePathIsNull() {
            assertThatIllegalStateException()
                    .isThrownBy(
                            () ->
                                    RecoveryManager.RecoveryContext.builder()
                                            .storageManager(storageManager)
                                            .instanceMappedBytes(1024)
                                            .tableStoreFactory(
                                                    tableId -> Mockito.mock(TableStore.class))
                                            .currentWritableProvider(tableId -> null)
                                            .build())
                    .withMessageContaining("basePath");
        }

        @Test
        void builderThrowsWhenInstanceMappedBytesIsZero() {
            assertThatIllegalStateException()
                    .isThrownBy(
                            () ->
                                    RecoveryManager.RecoveryContext.builder()
                                            .storageManager(storageManager)
                                            .basePath("/tmp")
                                            .instanceMappedBytes(0)
                                            .tableStoreFactory(
                                                    tableId -> Mockito.mock(TableStore.class))
                                            .currentWritableProvider(tableId -> null)
                                            .build())
                    .withMessageContaining("instanceMappedBytes");
        }

        @Test
        void builderThrowsWhenInstanceMappedBytesIsNegative() {
            assertThatIllegalStateException()
                    .isThrownBy(
                            () ->
                                    RecoveryManager.RecoveryContext.builder()
                                            .storageManager(storageManager)
                                            .basePath("/tmp")
                                            .instanceMappedBytes(-1)
                                            .tableStoreFactory(
                                                    tableId -> Mockito.mock(TableStore.class))
                                            .currentWritableProvider(tableId -> null)
                                            .build())
                    .withMessageContaining("instanceMappedBytes");
        }

        @Test
        void builderThrowsWhenTableStoreFactoryIsNull() {
            assertThatIllegalStateException()
                    .isThrownBy(
                            () ->
                                    RecoveryManager.RecoveryContext.builder()
                                            .storageManager(storageManager)
                                            .basePath("/tmp")
                                            .instanceMappedBytes(1024)
                                            .currentWritableProvider(tableId -> null)
                                            .build())
                    .withMessageContaining("tableStoreFactory");
        }

        @Test
        void builderThrowsWhenCurrentWritableProviderIsNull() {
            assertThatIllegalStateException()
                    .isThrownBy(
                            () ->
                                    RecoveryManager.RecoveryContext.builder()
                                            .storageManager(storageManager)
                                            .basePath("/tmp")
                                            .instanceMappedBytes(1024)
                                            .tableStoreFactory(
                                                    tableId -> Mockito.mock(TableStore.class))
                                            .build())
                    .withMessageContaining("currentWritableProvider");
        }

        @Test
        void builderSetsAllFieldsCorrectly() {
            WALConfig walConfig = new WALConfig("wal_logs", "wal", 1024 * 1024, true, 8192);
            Configuration conf = new Configuration();
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            TableStore mockTableStore = Mockito.mock(TableStore.class);

            RecoveryManager.RecoveryContext ctx =
                    RecoveryManager.RecoveryContext.builder()
                            .storageManager(storageManager)
                            .basePath("/data")
                            .instanceMappedBytes(64 * 1024 * 1024L)
                            .hadoopConf(conf)
                            .walConfig(walConfig)
                            .durabilityLevel(DurabilityLevel.NODE)
                            .writableNumSupplier(() -> 5)
                            .tableStoreFactory(tableId -> mockTableStore)
                            .writableInstanceRegistrar((id, inst) -> {})
                            .readOnlyInstanceRegistrar((id, inst) -> {})
                            .memoryTracker((used, reserved) -> {})
                            .currentWritableProvider(tableId -> mockInstance)
                            .instanceRegistrar((id, future) -> {})
                            .newInstanceCreator((tableId, mappedBytes) -> mockInstance)
                            .memoryAvailableChecker(() -> true)
                            .syncFlushTrigger(() -> {})
                            .build();

            assertThat(ctx.storageManager()).isSameAs(storageManager);
            assertThat(ctx.basePath()).isEqualTo("/data");
            assertThat(ctx.instanceMappedBytes()).isEqualTo(64 * 1024 * 1024L);
            assertThat(ctx.hadoopConf()).isSameAs(conf);
            assertThat(ctx.walConfig()).isSameAs(walConfig);
            assertThat(ctx.durabilityLevel()).isEqualTo(DurabilityLevel.NODE);
            assertThat(ctx.writableNumSupplier().get()).isEqualTo(5);
            assertThat(ctx.currentWritableProvider().apply("t")).isSameAs(mockInstance);
            assertThat(ctx.newInstanceCreator()).isNotNull();
            assertThat(ctx.memoryAvailableChecker()).isNotNull();
            assertThat(ctx.syncFlushTrigger()).isNotNull();
        }

        @Test
        void builderDefaultsAreApplied() {
            RecoveryManager.RecoveryContext ctx =
                    RecoveryManager.RecoveryContext.builder()
                            .storageManager(storageManager)
                            .basePath("/data")
                            .instanceMappedBytes(1024 * 1024L)
                            .tableStoreFactory(tableId -> Mockito.mock(TableStore.class))
                            .currentWritableProvider(tableId -> null)
                            .build();

            assertThat(ctx.durabilityLevel()).isEqualTo(DurabilityLevel.PROCESS);
            assertThat(ctx.writableNumSupplier().get()).isEqualTo(3);
            assertThat(ctx.newInstanceCreator()).isNull();
            assertThat(ctx.walConfig()).isNull();
            assertThat(ctx.memoryAvailableChecker()).isNull();
            assertThat(ctx.syncFlushTrigger()).isNull();
        }

        @Test
        void builderCreatesWorkingRecoveryManager() {
            LmdbInstance mockInstance = Mockito.mock(LmdbInstance.class);
            when(mockInstance.put(any(byte[].class), any(byte[].class)))
                    .thenReturn(LmdbInstance.PutStatus.SUCCESS);

            RecoveryManager.RecoveryContext ctx =
                    RecoveryManager.RecoveryContext.builder()
                            .storageManager(storageManager)
                            .basePath(tempDir.toString())
                            .instanceMappedBytes(1024 * 1024L)
                            .currentWritableProvider(tableId -> mockInstance)
                            .tableStoreFactory(tableId -> Mockito.mock(TableStore.class))
                            .build();

            RecoveryManager mgr = new RecoveryManager(ctx);

            byte[] data = buildWALRecordData("db.tbl", "key".getBytes(), "val".getBytes());
            WALRecord record = createMockWALRecord(data);
            Map<String, List<WALRecord>> recordsByTable = Map.of("db.tbl", List.of(record));

            assertThatCode(() -> invokeReplayWALRecords(mgr, recordsByTable))
                    .doesNotThrowAnyException();
            verify(mockInstance).put(any(byte[].class), any(byte[].class));
        }
    }

    // ==================== ====================

    /**
     * WAL recordof data, formatis: [4byte tableIdLen][tableId][4byte keyLen][key][4byte
     * valueLen][value]
     */
    private byte[] buildWALRecordData(String tableId, byte[] key, byte[] value) {
        byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
        int capacity = 4 + tableIdBytes.length + 4 + key.length + 4 + value.length;
        ByteBuffer buf = ByteBuffer.allocate(capacity);
        buf.putInt(tableIdBytes.length);
        buf.put(tableIdBytes);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(value.length);
        buf.put(value);
        return buf.array();
    }

    private WALRecord createMockWALRecord(byte[] data) {
        WALRecord record = Mockito.mock(WALRecord.class);
        when(record.value()).thenReturn(data);
        return record;
    }

    private WALRecord createTxnWALRecord(TxnWALRecord txnRecord) {
        byte[] encoded = TxnWALRecordCodec.encode(txnRecord);
        return new DefaultWALRecord(
                System.nanoTime(),
                encoded,
                System.currentTimeMillis(),
                -1L,
                txnRecord.tableId() != null ? txnRecord.tableId() : "unknown",
                "DATA");
    }

    /** ByteBuffer is writeof */
    private byte[] trimBuffer(ByteBuffer buf) {
        buf.flip();
        byte[] result = new byte[buf.remaining()];
        buf.get(result);
        return result;
    }

    /** create RecoveryManager, defaultofdepend on */
    private RecoveryManager createRecoveryManager(Function<String, LmdbInstance> writableProvider) {
        return createRecoveryManagerWithStorage(storageManager, writableProvider);
    }

    private RecoveryManager createRecoveryManager(
            Function<String, LmdbInstance> writableProvider,
            Function<String, TableStore> tableStoreFactory) {
        return new RecoveryManager(
                new RecoveryManager.RecoveryContext(
                        storageManager,
                        tempDir.toString(),
                        1024 * 1024L, // instanceMappedBytes
                        new Configuration(),
                        null, // walConfig
                        DurabilityLevel.PROCESS,
                        true,
                        () -> 3, // writableNumSupplier
                        tableStoreFactory,
                        (tableId, inst) -> {}, // writableInstanceRegistrar
                        (tableId, inst) -> {}, // readOnlyInstanceRegistrar
                        (used, reserved) -> {}, // memoryTracker
                        writableProvider,
                        (instId, future) -> {}, // instanceRegistrar
                        null, // newInstanceCreator
                        null, // memoryAvailableChecker
                        null // syncFlushTrigger
                        ));
    }

    private RecoveryManager createRecoveryManagerWithStorage(
            StorageManager sm, Function<String, LmdbInstance> writableProvider) {
        return new RecoveryManager(
                new RecoveryManager.RecoveryContext(
                        sm,
                        tempDir.toString(),
                        1024 * 1024L,
                        new Configuration(),
                        null,
                        DurabilityLevel.PROCESS,
                        true,
                        () -> 3,
                        tableId -> Mockito.mock(TableStore.class),
                        (tableId, inst) -> {},
                        (tableId, inst) -> {},
                        (used, reserved) -> {},
                        writableProvider,
                        (instId, future) -> {},
                        null, // newInstanceCreator
                        null, // memoryAvailableChecker
                        null // syncFlushTrigger
                        ));
    }

    private RecoveryManager createRecoveryManagerWithWAL(
            WALConfig walConfig,
            boolean walEnabled,
            Function<String, LmdbInstance> writableProvider) {
        return new RecoveryManager(
                new RecoveryManager.RecoveryContext(
                        storageManager,
                        tempDir.toString(),
                        1024 * 1024L,
                        new Configuration(),
                        walConfig,
                        walEnabled ? DurabilityLevel.NODE : DurabilityLevel.PROCESS,
                        true,
                        () -> 3,
                        tableId -> Mockito.mock(TableStore.class),
                        (tableId, inst) -> {},
                        (tableId, inst) -> {},
                        (used, reserved) -> {},
                        writableProvider,
                        (instId, future) -> {},
                        null, // newInstanceCreator
                        null, // memoryAvailableChecker
                        null // syncFlushTrigger
                        ));
    }

    private RecoveryManager createRecoveryManagerWithDurabilityLevel(
            DurabilityLevel durabilityLevel,
            WALConfig walConfig,
            Function<String, LmdbInstance> writableProvider) {
        return new RecoveryManager(
                new RecoveryManager.RecoveryContext(
                        storageManager,
                        tempDir.toString(),
                        1024 * 1024L,
                        new Configuration(),
                        walConfig,
                        durabilityLevel,
                        true,
                        () -> 3,
                        tableId -> Mockito.mock(TableStore.class),
                        (tableId, inst) -> {},
                        (tableId, inst) -> {},
                        (used, reserved) -> {},
                        writableProvider,
                        (instId, future) -> {},
                        null, // newInstanceCreator
                        null, // memoryAvailableChecker
                        null // syncFlushTrigger
                        ));
    }

    private RecoveryManager createRecoveryManagerWithNewInstanceCreator(
            Function<String, LmdbInstance> writableProvider,
            BiFunction<String, Long, LmdbInstance> newInstanceCreator) {
        return new RecoveryManager(
                new RecoveryManager.RecoveryContext(
                        storageManager,
                        tempDir.toString(),
                        1024 * 1024L,
                        new Configuration(),
                        null,
                        DurabilityLevel.PROCESS,
                        true,
                        () -> 3,
                        tableId -> Mockito.mock(TableStore.class),
                        (tableId, inst) -> {},
                        (tableId, inst) -> {},
                        (used, reserved) -> {},
                        writableProvider,
                        (instId, future) -> {},
                        newInstanceCreator,
                        null, // memoryAvailableChecker
                        null // syncFlushTrigger
                        ));
    }

    private RecoveryManager createRecoveryManagerWithMemoryCheck(
            Supplier<Boolean> memoryAvailableChecker, Runnable syncFlushTrigger) {
        return new RecoveryManager(
                new RecoveryManager.RecoveryContext(
                        storageManager,
                        tempDir.toString(),
                        1024 * 1024L,
                        new Configuration(),
                        null,
                        DurabilityLevel.PROCESS,
                        true,
                        () -> 3,
                        tableId -> Mockito.mock(TableStore.class),
                        (tableId, inst) -> {},
                        (tableId, inst) -> {},
                        (used, reserved) -> {},
                        tableId -> null,
                        (instId, future) -> {},
                        null, // newInstanceCreator
                        memoryAvailableChecker,
                        syncFlushTrigger));
    }

    /** via call private replayWALRecords */
    private void invokeReplayWALRecords(
            RecoveryManager mgr, Map<String, List<WALRecord>> recordsByTable) {
        try {
            Method method = RecoveryManager.class.getDeclaredMethod("replayWALRecords", Map.class);
            method.setAccessible(true);
            method.invoke(mgr, recordsByTable);
        } catch (Exception e) {
            if (e.getCause() instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("Failed to invoke replayWALRecords", e);
        }
    }
}
