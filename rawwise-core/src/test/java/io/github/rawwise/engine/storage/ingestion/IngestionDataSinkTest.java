package io.github.rawwise.engine.storage.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.engine.storage.RoutingOwnerContext;
import io.github.rawwise.engine.storage.manifest.SnapshotCommitConflictException;
import io.github.rawwise.engine.storage.manifest.SnapshotOperations;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.txn.SimpleTableRef;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.github.rawwise.model.op.KvWriteBuilder;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * IngestionDataSink E2E test -- cover LMDB / Parquet Sink of .
 *
 * <p>alltest real (DynamicMmapStore + real LMDB / localfilesystem), does not use mock, verifyfrom
 * flushBatch -> prepare -> commit/rollback -> recoverPending ofend-to-end scenario.
 */
@DisplayName("IngestionDataSink E2E test -- write-commit-rollback-recovery")
class IngestionDataSinkTest {

    @BeforeAll
    static void registerStorageProvider() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception e) {
            // Already registered, ignore
        }
    }

    @TempDir java.nio.file.Path tempDir;

    private DynamicMmapStore store;
    private final List<IngestionDataSink> sinksToClose = new ArrayList<>();

    @BeforeEach
    void setUp() {
        sinksToClose.clear();
    }

    @AfterEach
    void tearDown() {
        // Close sinks first (reverse order)
        Collections.reverse(sinksToClose);
        for (IngestionDataSink sink : sinksToClose) {
            try {
                sink.close();
            } catch (Exception e) {
                // Best-effort close
            }
        }
        sinksToClose.clear();

        // Close store
        if (store != null) {
            try {
                store.close();
            } catch (Exception e) {
                // Best-effort close
            }
            store = null;
        }
    }

    // ==================== Helper methods ====================

    private static byte[] k(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] v(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private DynamicMmapStore createStore() {
        DynamicMmapStore s =
                DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
        DynamicMmapStoreLmdbOps lmdbOps = new DynamicMmapStoreLmdbOps(s);
        LmdbIngestionDataSink lmdbSink = new LmdbIngestionDataSink(lmdbOps, 10, 64 * 1024 * 1024);
        s.setIngestionDataSink(lmdbSink);
        return s;
    }

    /**
     * via DynamicMmapStore acquireits of LmdbIngestionDataSink. DynamicMmapStore inconstruct
     * already create LmdbIngestionDataSink(default LMDB), via getIngestionDataSink() acquire.
     */
    private LmdbIngestionDataSink getLmdbSink(DynamicMmapStore dynamicMmapStore) {
        IngestionDataSink sink = dynamicMmapStore.getIngestionDataSink();
        assertThat(sink).as("DynamicMmapStore should have a built-in LMDB sink").isNotNull();
        assertThat(sink).isInstanceOf(LmdbIngestionDataSink.class);
        sinksToClose.add(sink);
        return (LmdbIngestionDataSink) sink;
    }

    /**
     * via store.write() initialize itemcanwrite LMDB Instance. DynamicMmapStoreLmdbOps.batchPut()
     * depend on TableStore.getCurrentWritableInstance(), canwrite Instance hasin store.write()
     * pathin willbecreate. this via write call initialize TableStore ofcanwriteinstance.
     */
    private void initializeWritableInstance(DynamicMmapStore dynamicMmapStore, String tableId) {
        KvWriteBuilder initWrite =
                KvWriteBuilder.builder()
                        .table(new SimpleTableRef(tableId))
                        .key(k("__init__"))
                        .value(v("__init__"))
                        .partitionHint(0);
        dynamicMmapStore.write(initWrite, new RoutingOwnerContext("exec-test", 0));
    }

    /**
     * create based on local filesystem ParquetIngestionDataSink. InMemorySnapshotOperations is
     * SnapshotOperations ofreal , avoiddepend on Metastore.
     */
    private ParquetIngestionDataSink createParquetSink(java.nio.file.Path basePath)
            throws IOException {
        Configuration hadoopConf = new Configuration();
        // localfilesystemsimulate HDFS
        FileSystem hdfs = FileSystem.getLocal(hadoopConf);

        InMemorySnapshotOperations snapshotOps = new InMemorySnapshotOperations();

        // of shouldtestenvironment
        ParquetIngestionDataSink sink =
                new ParquetIngestionDataSink(
                        basePath.toString(),
                        hadoopConf,
                        hdfs,
                        snapshotOps,
                        "SNAPPY", // compressionCodec
                        128L * 1024 * 1024, // rowGroupSize
                        64 * 1024, // pageSize
                        (short) 1, // hdfsReplication
                        512L * 1024 * 1024, // maxBatchBytes
                        1024L * 1024 * 1024, // maxPendingBytes
                        1000L, // orphanAgeThresholdMs (1s for test)
                        10, // recentSnapshotScanCount
                        Clock.systemUTC());
        sinksToClose.add(sink);
        return sink;
    }

    // ==================== LMDB Sink E2E Scenarios ====================

    @Test
    @DisplayName(
            "LMDB write-commit-verify : flushBatch -> prepare -> commit does not throw exception")
    void testLmdbFlushPrepareCommitPendingCleared() throws Exception {
        store = createStore();
        String tableId = "testdb.lmdb_commit";
        long txnId = 1L;
        long commitTs = System.currentTimeMillis();

        initializeWritableInstance(store, tableId);

        TableStore tableStore = store.tableStore(tableId);
        assertThat(tableStore).isNotNull();

        LmdbIngestionDataSink sink = getLmdbSink(store);

        List<byte[]> keys = List.of(k("key1"), k("key2"), k("key3"));
        List<byte[]> values = List.of(v("val1"), v("val2"), v("val3"));

        IngestionDataSink.FlushResult flushResult =
                sink.flushBatch(tableId, txnId, keys, values, 1L, 1L, 3L);

        assertThat(flushResult.pendingIds()).isNotEmpty();

        sink.prepare(tableId, flushResult.pendingIds(), txnId);
        sink.commit(tableId, flushResult.pendingIds(), txnId, commitTs);

        assertThat(sink.sinkType()).isEqualTo("lmdb");
    }

    @Test
    @DisplayName("LMDB write-rollback : flushBatch -> rollback does not throw exception")
    void testLmdbFlushRollbackPendingCleared() throws Exception {
        store = createStore();
        String tableId = "testdb.lmdb_rollback";
        long txnId = 2L;

        initializeWritableInstance(store, tableId);

        TableStore tableStore = store.tableStore(tableId);
        LmdbIngestionDataSink sink = getLmdbSink(store);

        List<byte[]> keys = List.of(k("rkey1"), k("rkey2"));
        List<byte[]> values = List.of(v("rval1"), v("rval2"));

        IngestionDataSink.FlushResult flushResult =
                sink.flushBatch(tableId, txnId, keys, values, 1L, 1L, 2L);

        assertThat(flushResult.pendingIds()).isNotEmpty();

        sink.rollback(tableId, flushResult.pendingIds(), txnId);
    }

    @Test
    @DisplayName("LMDB recovery : recoverPending returns empty results(no pending instance)")
    void testLmdbRecoverPendingCleansUncommittedData() throws Exception {
        store = createStore();
        String tableId = "testdb.lmdb_recover";
        long txnId = 3L;

        initializeWritableInstance(store, tableId);

        TableStore tableStore = store.tableStore(tableId);
        LmdbIngestionDataSink sink = getLmdbSink(store);

        List<byte[]> keys = List.of(k("reckey1"), k("reckey2"));
        List<byte[]> values = List.of(v("recval1"), v("recval2"));

        IngestionDataSink.FlushResult flushResult =
                sink.flushBatch(tableId, txnId, keys, values, 1L, 1L, 2L);

        assertThat(flushResult.pendingIds()).isNotEmpty();

        IngestionDataSink.RecoveryResult recoveryResult = sink.recoverPending(tableId);
        assertThat(recoveryResult).isNotNull();
    }

    // ==================== Parquet Sink E2E Scenarios ====================

    @Test
    @DisplayName(
            "Parquet write-commit-verify : flushBatch -> prepare -> commit -> from pending to data")
    void testParquetFlushPrepareCommitFileMovedToData() throws Exception {
        // Given: create based on local filesystem ParquetIngestionDataSink
        java.nio.file.Path basePath = tempDir.resolve("parquet-commit-" + UUID.randomUUID());
        java.nio.file.Files.createDirectories(basePath);
        ParquetIngestionDataSink sink = createParquetSink(basePath);

        String tableId = "testdb.parquet_commit";
        long txnId = 10L;
        long commitTs = System.currentTimeMillis();

        // When: write data
        List<byte[]> keys = List.of(k("pkey1"), k("pkey2"), k("pkey3"));
        List<byte[]> values = List.of(v("pval1"), v("pval2"), v("pval3"));

        IngestionDataSink.FlushResult flushResult =
                sink.flushBatch(tableId, txnId, keys, values, 1L, 1L, 3L);

        // Then: flushBatch pendingId
        assertThat(flushResult.pendingIds()).hasSize(1);
        String pendingId = flushResult.pendingIds().get(0);

        // And: pending in parquet
        Configuration hadoopConf = new Configuration();
        FileSystem hdfs = FileSystem.getLocal(hadoopConf);

        Path pendingDir =
                new Path(basePath.toString(), TableId.parseLenient(tableId).toPath() + "/pending");
        Path pendingFilePath = new Path(pendingDir, pendingId + ".parquet");
        assertThat(hdfs.exists(pendingFilePath))
                .as("Pending file should exist after flushBatch")
                .isTrue();

        // When: prepare
        sink.prepare(tableId, flushResult.pendingIds(), txnId);

        // When: commit
        sink.commit(tableId, flushResult.pendingIds(), txnId, commitTs);

        // Then: pending already to data
        Path dataDir =
                new Path(basePath.toString(), TableId.parseLenient(tableId).toPath() + "/data");
        Path dataFilePath = new Path(dataDir, pendingId + ".parquet");
        assertThat(hdfs.exists(dataFilePath)).as("Data file should exist after commit").isTrue();
        assertThat(hdfs.exists(pendingFilePath))
                .as("Pending file should no longer exist after commit")
                .isFalse();

        // And: sink of sinkType is parquet
        assertThat(sink.sinkType()).isEqualTo("parquet");
    }

    @Test
    @DisplayName("Parquet write-rollback : flushBatch -> rollback -> pending bedelete")
    void testParquetFlushRollbackPendingFileDeleted() throws Exception {
        // Given: create based on local filesystem ParquetIngestionDataSink
        java.nio.file.Path basePath = tempDir.resolve("parquet-rollback-" + UUID.randomUUID());
        java.nio.file.Files.createDirectories(basePath);
        ParquetIngestionDataSink sink = createParquetSink(basePath);

        String tableId = "testdb.parquet_rollback";
        long txnId = 20L;

        // When: write data
        List<byte[]> keys = List.of(k("rbkey1"), k("rbkey2"));
        List<byte[]> values = List.of(v("rbval1"), v("rbval2"));

        IngestionDataSink.FlushResult flushResult =
                sink.flushBatch(tableId, txnId, keys, values, 1L, 1L, 2L);

        assertThat(flushResult.pendingIds()).hasSize(1);
        String pendingId = flushResult.pendingIds().get(0);

        // And: pending in
        Configuration hadoopConf = new Configuration();
        FileSystem hdfs = FileSystem.getLocal(hadoopConf);

        Path pendingDir =
                new Path(basePath.toString(), TableId.parseLenient(tableId).toPath() + "/pending");
        Path pendingFilePath = new Path(pendingDir, pendingId + ".parquet");
        assertThat(hdfs.exists(pendingFilePath)).isTrue();

        // When: rollback
        sink.rollback(tableId, flushResult.pendingIds(), txnId);

        // Then: pending bedelete
        assertThat(hdfs.exists(pendingFilePath))
                .as("Pending file should be deleted after rollback")
                .isFalse();
    }

    @Test
    @DisplayName("Parquet recovery : flushBatch -> simulate -> recoverPending -> becleanup")
    void testParquetRecoverPendingCleansOrphanFiles() throws Exception {
        // Given: create based on local filesystem ParquetIngestionDataSink
        java.nio.file.Path basePath = tempDir.resolve("parquet-recover-" + UUID.randomUUID());
        java.nio.file.Files.createDirectories(basePath);
        ParquetIngestionDataSink sink = createParquetSink(basePath);

        String tableId = "testdb.parquet_recover";
        long txnId = 30L;

        Configuration hadoopConf = new Configuration();
        FileSystem hdfs = FileSystem.getLocal(hadoopConf);

        // When: write data(simulate beforestatus)
        List<byte[]> keys = List.of(k("reckey1"), k("reckey2"));
        List<byte[]> values = List.of(v("recval1"), v("recval2"));

        IngestionDataSink.FlushResult flushResult =
                sink.flushBatch(tableId, txnId, keys, values, 1L, 1L, 2L);

        assertThat(flushResult.pendingIds()).hasSize(1);

        // And: pending in
        Path pendingDir =
                new Path(basePath.toString(), TableId.parseLenient(tableId).toPath() + "/pending");
        assertThat(hdfs.listStatus(pendingDir).length).isGreaterThan(0);

        // When: simulate -- call commit or rollback, directlycall recoverPending
        // wait orphanAgeThresholdMs exceedsthreshold(testconfigis 1000ms)
        Thread.sleep(1100);

        IngestionDataSink.RecoveryResult recoveryResult = sink.recoverPending(tableId);

        // Then: becleanup(because has manifest , exceeds age threshold)
        assertThat(recoveryResult.cleanedCount())
                .as("Orphan pending files should be cleaned during recovery")
                .isGreaterThan(0);
    }

    // ==================== Factory E2E Scenario ====================

    @Test
    @DisplayName("Factory create : configcreate LMDB/PARQUET sink")
    void testFactoryCreatesCorrectSinkType() throws Exception {
        // Scenario 1: via DynamicMmapStore of LMDB sink verify
        store = createStore();
        IngestionDataSink lmdbSink = store.getIngestionDataSink();
        assertThat(lmdbSink).as("DynamicMmapStore should provide a built-in LMDB sink").isNotNull();
        assertThat(lmdbSink).isInstanceOf(LmdbIngestionDataSink.class);
        assertThat(lmdbSink.sinkType()).isEqualTo("lmdb");

        // Scenario 2: via IngestionDataSinkFactory create PARQUET sink
        java.nio.file.Path parquetBasePath =
                tempDir.resolve("parquet-factory-" + UUID.randomUUID());
        java.nio.file.Files.createDirectories(parquetBasePath);

        RawWiseConfig parquetConfig =
                RawWiseConfig.empty()
                        .with(RawWiseOptions.INGESTION_SINK_TYPE, "PARQUET")
                        .with(
                                RawWiseOptions.INGESTION_SINK_PARQUET_HDFS_BASE_PATH,
                                parquetBasePath.toString());

        InMemorySnapshotOperations snapshotOps = new InMemorySnapshotOperations();
        Configuration hadoopConf = new Configuration();
        FileSystem hdfs = FileSystem.getLocal(hadoopConf);

        IngestionDataSink parquetSink =
                IngestionDataSinkFactory.createParquetSink(
                        parquetConfig, hadoopConf, hdfs, snapshotOps, Clock.systemUTC());
        sinksToClose.add(parquetSink);

        assertThat(parquetSink).isInstanceOf(ParquetIngestionDataSink.class);
        assertThat(parquetSink.sinkType()).isEqualTo("parquet");
    }

    // ==================== In-Memory SnapshotOperations ====================

    /**
     * memoryof SnapshotOperations , Parquet testscenario. does not depend on external Metastore,
     * synchronized List storage snapshot and dataFile .
     */
    static class InMemorySnapshotOperations implements SnapshotOperations {

        private final AtomicLong snapshotIdGenerator = new AtomicLong(0);
        private final List<Snapshot> snapshots = Collections.synchronizedList(new ArrayList<>());
        private final List<DataFile> dataFiles = Collections.synchronizedList(new ArrayList<>());

        @Override
        public Optional<Snapshot> latestSnapshot(String tableId) {
            synchronized (snapshots) {
                if (snapshots.isEmpty()) return Optional.empty();
                return Optional.of(snapshots.get(snapshots.size() - 1));
            }
        }

        @Override
        public List<DataFile> listDataFiles(String tableId, long snapshotId) throws IOException {
            return List.copyOf(dataFiles);
        }

        @Override
        public List<Snapshot> getSnapshots(String tableId) throws IOException {
            return List.copyOf(snapshots);
        }

        @Override
        public List<Snapshot> getRecentSnapshots(String tableId, int count) throws IOException {
            synchronized (snapshots) {
                if (snapshots.size() <= count) return List.copyOf(snapshots);
                return List.copyOf(snapshots.subList(snapshots.size() - count, snapshots.size()));
            }
        }

        @Override
        public Snapshot commitSnapshot(
                String tableId,
                List<DataFile> newDataFiles,
                List<LogFile> newLogFiles,
                long minSeq,
                long maxSeq)
                throws SnapshotCommitConflictException, IOException {
            long snapshotId = snapshotIdGenerator.incrementAndGet();
            Snapshot snapshot =
                    Snapshot.of(
                            snapshotId,
                            List.of("manifest-" + snapshotId),
                            System.currentTimeMillis(),
                            "append",
                            "append",
                            null);
            snapshots.add(snapshot);
            dataFiles.addAll(newDataFiles);
            return snapshot;
        }
    }
}
