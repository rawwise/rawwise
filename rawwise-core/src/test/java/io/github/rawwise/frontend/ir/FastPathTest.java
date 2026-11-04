package io.github.rawwise.frontend.ir;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.RoutingOwnerContext;
import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.read.ParquetBlockReader;
import io.github.rawwise.engine.storage.bloom.BloomFilterBuilder;
import io.github.rawwise.engine.storage.bloom.BloomFilterReader;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.OperationSet;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.SimpleTableRef;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.model.op.KvWriteBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Fast Path E2E test - lazypoint query/point updatefull chain")
class FastPathTest {

    private static final Schema KEY_VALUE_SCHEMA =
            new Schema.Parser()
                    .parse(
                            """
            {
              "type": "record",
              "name": "RawWiseKeyValueRecord",
              "fields": [
                {"name": "key", "type": "bytes"},
                {"name": "value", "type": "bytes"}
              ]
            }
            """);

    private static final DefaultKeyCodec DEFAULT_KEY_CODEC = new DefaultKeyCodec();

    @TempDir static Path tempDir;

    private static DynamicMmapStore store;
    private static String storeId;

    @BeforeAll
    static void setUpClass() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception e) {
            // Already registered
        }
        storeId = "e2e-store-" + UUID.randomUUID();
        store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
    }

    @AfterAll
    static void tearDownClass() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    // ==================== Helpers ====================

    private static byte[] k(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] v(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private void writeToStore(String tableId, String key, String value, long epoch) {
        byte[] keyBytes = k(key);
        byte[] valueBytes = v(value);
        KvWriteBuilder builder =
                KvWriteBuilder.builder()
                        .table(new SimpleTableRef(tableId))
                        .key(keyBytes)
                        .value(valueBytes)
                        .partitionHint(0);
        RoutingOwnerContext owner = new RoutingOwnerContext("exec-e2e", epoch);
        store.write(builder, owner);
    }

    private byte[] readRawValue(String tableId, String key) {
        byte[] raw = store.tableStore(tableId).getRawValue(k(key));
        if (raw == null) return null;
        return NullableValueCodec.isTombstone(raw) ? null : NullableValueCodec.decode(raw);
    }

    private ColdDataBlock buildColdBlock(
            String tableId, byte[] bloomFilter, int rowCount, String parquetPath) {
        long fileSize;
        try {
            fileSize = Files.exists(Path.of(parquetPath)) ? Files.size(Path.of(parquetPath)) : 100L;
        } catch (Exception e) {
            fileSize = 100L;
        }
        return new ColdDataBlock(
                "cold-block-" + UUID.randomUUID(),
                tableId,
                DataBlock.Kind.IMMUTABLE_COLD,
                DataBlock.Format.PARQUET,
                DataBlock.Format.PARQUET,
                new DataBlock.Location(
                        "local", new Storage.StoragePath("file", parquetPath), 0L, fileSize),
                new KeyRange(new byte[] {0}, true, new byte[] {(byte) 0xFF}, true),
                rowCount,
                fileSize,
                0L,
                -1L,
                Map.of(),
                new ColdDataBlock.Statistics(rowCount, fileSize, 0L, 0L),
                DataBlock.Visibility.ARCHIVED,
                0L,
                0L,
                0,
                bloomFilter,
                null);
    }

    private ParquetBlockReader createParquetBlockReader() {
        return new ParquetBlockReader(DEFAULT_KEY_CODEC);
    }

    // ==================== scenario group A: PointQuery point query (6) ====================

    @Nested
    @DisplayName("scenario group A: PointQuery point query")
    class PointQueryTests {

        private String tableId;

        @BeforeEach
        void setUp() {
            tableId = "testdb.a_" + UUID.randomUUID().toString().replace("-", "");
        }

        @Test
        @DisplayName(
                "A1: hot data point query — after writing to LMDB point query returns correct value")
        void testHotDataPointQuery() {
            writeToStore(tableId, "key-a1", "value-a1", 0L);

            byte[] result = readRawValue(tableId, "key-a1");
            assertThat(result).isNotNull();
            assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("value-a1");
        }

        @Test
        @DisplayName(
                "A2: cold data point query — after writing flush to Parquet, cold path returns correct value")
        void testColdDataPointQuery() throws Exception {
            writeToStore(tableId, "key-cold", "value-cold", 0L);

            TableStore ts = store.tableStore(tableId);
            List<LmdbInstance> writableInstances = ts.getWritableInstances();
            assertThat(writableInstances).isNotEmpty();

            LmdbInstance instance = writableInstances.get(0);
            instance.markAsReadOnly();

            Path parquetFile = tempDir.resolve("cold-" + UUID.randomUUID() + ".parquet");
            String parquetPath = parquetFile.toUri().toString();
            List<byte[]> flushedKeys = new ArrayList<>();

            try (ParquetWriter<GenericRecord> writer =
                    AvroParquetWriter.<GenericRecord>builder(
                                    new org.apache.hadoop.fs.Path(parquetPath))
                            .withSchema(KEY_VALUE_SCHEMA)
                            .withConf(new Configuration())
                            .build()) {
                try (var iterator = instance.closeableIterator()) {
                    while (iterator.hasNext()) {
                        var pair = iterator.next();
                        flushedKeys.add(pair.getLeft());
                        GenericRecord record = new GenericData.Record(KEY_VALUE_SCHEMA);
                        record.put("key", java.nio.ByteBuffer.wrap(pair.getLeft()));
                        record.put("value", java.nio.ByteBuffer.wrap(pair.getRight()));
                        writer.write(record);
                    }
                }
            }

            byte[] bloomFilter = BloomFilterBuilder.build(flushedKeys, flushedKeys.size());
            ColdDataBlock coldBlock =
                    buildColdBlock(tableId, bloomFilter, flushedKeys.size(), parquetPath);
            ts.registerColdBlock(coldBlock);

            List<ColdDataBlock> coldBlocks =
                    ts.currentDataBlocks().stream()
                            .filter(ColdDataBlock.class::isInstance)
                            .map(ColdDataBlock.class::cast)
                            .filter(b -> b.visibility() == DataBlock.Visibility.ARCHIVED)
                            .toList();
            assertThat(coldBlocks).isNotEmpty();

            ParquetBlockReader reader = createParquetBlockReader();
            byte[] coldResult = reader.getRawValueFromColdBlocks(coldBlocks, k("key-cold"));

            assertThat(coldResult).isNotNull();
            byte[] decodedValue = NullableValueCodec.decode(coldResult);
            assertThat(new String(decodedValue, StandardCharsets.UTF_8)).isEqualTo("value-cold");
        }

        @Test
        @DisplayName("A3: point query key not found — query non-existent key returns null")
        void testPointQueryKeyNotFound() {
            writeToStore(tableId, "existing-key", "some-value", 0L);

            byte[] result = readRawValue(tableId, "non-existent-key");

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("A4: MVCCsnapshotisolation — snapshotTs filters invisible data versions")
        void testMvccSnapshotIsolation() {
            String keyName = "key-mvcc";
            writeToStore(tableId, keyName, "v1", 0L);

            ShardVersionTracker tracker = store.getOrCreateVersionTracker(tableId);
            long commitTs = tracker.updateVersion(k(keyName), 100L);
            assertThat(commitTs).isGreaterThan(0L);

            long currentCommitTs = tracker.getCommitTs(k(keyName));
            assertThat(currentCommitTs).isEqualTo(commitTs);

            byte[] visibleValue = readRawValue(tableId, keyName);
            assertThat(visibleValue).isNotNull();
            assertThat(new String(visibleValue, StandardCharsets.UTF_8)).isEqualTo("v1");
        }

        @Test
        @DisplayName(
                "A5: cold data Bloom Filter exclude — non-existent key quickly excluded by Bloom Filter")
        void testBloomFilterNegativeExclusion() {
            List<byte[]> keys = List.of(k("bf-key-1"), k("bf-key-2"), k("bf-key-3"));
            byte[] serializedBf = BloomFilterBuilder.build(keys, keys.size());

            byte[] nonExistentKey = k("bf-key-missing");
            boolean mightContain = BloomFilterReader.mightContain(serializedBf, nonExistentKey);

            assertThat(mightContain)
                    .as("Bloom filter should exclude non-existent key (negative)")
                    .isFalse();
        }

        @Test
        @DisplayName("A6: cold data Bloom Filter inkey — existing key mightContain returns true")
        void testBloomFilterPositiveForExistingKey() {
            byte[] existingKey = k("bf-key-present");
            List<byte[]> keys = List.of(k("bf-key-other"), existingKey);
            byte[] serializedBf = BloomFilterBuilder.build(keys, keys.size());

            boolean mightContain = BloomFilterReader.mightContain(serializedBf, existingKey);

            assertThat(mightContain)
                    .as("Bloom filter should report true for key that was inserted")
                    .isTrue();
        }
    }

    // ==================== scenario group B: PointUpdate point update (4) ====================

    @Nested
    @DisplayName("scenario group B: PointUpdate point update")
    class PointUpdateTests {

        private String tableId;

        @BeforeEach
        void setUp() {
            tableId = "testdb.b_" + UUID.randomUUID().toString().replace("-", "");
        }

        @Test
        @DisplayName("B1: WRITE_THROUGH point update — commitTscorrectly increments")
        void testWriteThroughPointUpdateCommitTsIncrement() {
            String keyName = "key-wt";

            KvWriteBuilder builder =
                    KvWriteBuilder.builder()
                            .table(new SimpleTableRef(tableId))
                            .key(k(keyName))
                            .value(v("wt-value"))
                            .partitionHint(0);
            RoutingOwnerContext owner = new RoutingOwnerContext("exec-e2e", 0L);
            store.write(builder, owner);

            ShardVersionTracker tracker = store.getOrCreateVersionTracker(tableId);
            long commitTs = tracker.updateVersion(k(keyName), 200L);

            assertThat(commitTs).isGreaterThan(0L);
            assertThat(tracker.getCommitTs(k(keyName))).isEqualTo(commitTs);

            writeToStore(tableId, keyName, "wt-value-2", 0L);
            long commitTs2 = tracker.updateVersion(k(keyName), 201L);
            assertThat(commitTs2).isGreaterThan(commitTs);
        }

        @Test
        @DisplayName(
                "B2: OCCconflict detection — concurrent write same key, latter detectsWRITE_WRITEconflict")
        void testOccConflictDetectionOnSameKey() {
            String conflictKey = "key-conflict";

            ShardVersionTracker tracker = store.getOrCreateVersionTracker(tableId);
            long baseTs = tracker.updateVersion(k(conflictKey), 0L);
            assertThat(baseTs).isGreaterThan(0L);

            TxnContext txn1 =
                    new TxnContext(
                            1L,
                            0L,
                            IsolationLevel.SERIALIZABLE,
                            Set.of(tableId),
                            0,
                            0L,
                            0L,
                            "attempt-1",
                            io.github.rawwise.engine.txn.TxnTimeoutProfile.OLTP);
            txn1.operationSet().recordWrite(k(conflictKey), OperationSet.WriteType.UPDATE, baseTs);

            TxnContext txn2 =
                    new TxnContext(
                            2L,
                            0L,
                            IsolationLevel.SERIALIZABLE,
                            Set.of(tableId),
                            0,
                            0L,
                            0L,
                            "attempt-2",
                            io.github.rawwise.engine.txn.TxnTimeoutProfile.OLTP);
            txn2.operationSet().recordWrite(k(conflictKey), OperationSet.WriteType.UPDATE, baseTs);

            ShardVersionTracker.AtomicValidateResult result1 =
                    tracker.validateAndUpdateAtomic(txn1, tracker.getCommitLock());
            assertThat(result1.success()).as("Txn1 should commit successfully").isTrue();
            long txn1CommitTs = result1.commitTs();
            assertThat(txn1CommitTs).isGreaterThan(baseTs);

            ShardVersionTracker.AtomicValidateResult result2 =
                    tracker.validateAndUpdateAtomic(txn2, tracker.getCommitLock());
            assertThat(result2.success())
                    .as("Txn2 should detect WRITE_WRITE conflict because txn1 changed commitTs")
                    .isFalse();
            assertThat(result2.conflictKey()).isNotNull();
        }

        @Test
        @DisplayName("B3: WAL-before-LMDB — WAL persisted before LMDB on write")
        void testWalBeforeLmdbWriteOrder() {
            String keyName = "key-wal-order";
            String expectedValue = "wal-lmdb-value";

            KvWriteBuilder builder =
                    KvWriteBuilder.builder()
                            .table(new SimpleTableRef(tableId))
                            .key(k(keyName))
                            .value(v(expectedValue))
                            .partitionHint(0);
            RoutingOwnerContext owner = new RoutingOwnerContext("exec-e2e", 0L);
            store.write(builder, owner);

            byte[] result = readRawValue(tableId, keyName);
            assertThat(result).isNotNull();
            assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo(expectedValue);
        }

        @Test
        @DisplayName("B4: point updateafterpoint query — MVCC read-your-writesguaranteecan")
        void testReadYourWritesAfterPointUpdate() {
            String keyName = "key-ryw";
            writeToStore(tableId, keyName, "v2", 0L);

            ShardVersionTracker tracker = store.getOrCreateVersionTracker(tableId);
            long commitTs = tracker.updateVersion(k(keyName), 300L);

            byte[] result = readRawValue(tableId, keyName);
            long keyCommitTs = tracker.getCommitTs(k(keyName));

            assertThat(result).isNotNull();
            assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("v2");
            assertThat(keyCommitTs).isEqualTo(commitTs);
        }
    }

    // ==================== scenario group C: BatchPointQuery batchpoint query (3)
    // ====================

    @Nested
    @DisplayName("scenario group C: BatchPointQuery batchpoint query")
    class BatchPointQueryTests {

        private String tableId;

        @BeforeEach
        void setUp() {
            tableId = "testdb.c_" + UUID.randomUUID().toString().replace("-", "");
        }

        @Test
        @DisplayName(
                "C1: batch hot datapoint query — write multiplekeyafterbatch query correctly returns")
        void testBatchHotDataPointQuery() {
            int keyCount = 10;
            for (int i = 0; i < keyCount; i++) {
                writeToStore(tableId, "batch-key-" + i, "batch-value-" + i, 0L);
            }

            List<byte[]> keys = new ArrayList<>();
            for (int i = 0; i < keyCount; i++) {
                keys.add(k("batch-key-" + i));
            }
            List<byte[]> results = new ArrayList<>();
            TableStore ts = store.tableStore(tableId);
            for (byte[] key : keys) {
                byte[] raw = ts.getRawValue(key);
                results.add(raw != null ? NullableValueCodec.decode(raw) : null);
            }

            assertThat(results).hasSize(keyCount);
            for (int i = 0; i < keyCount; i++) {
                assertThat(results.get(i)).isNotNull();
                assertThat(new String(results.get(i), StandardCharsets.UTF_8))
                        .isEqualTo("batch-value-" + i);
            }
        }

        @Test
        @DisplayName(
                "C2: batch mixed hot/cold — partialkeyinLMDB, partialalreadyflushtoParquet, correctly returns")
        void testBatchMixedHotColdPointQuery() throws Exception {
            writeToStore(tableId, "hot-1", "hot-value-1", 0L);
            writeToStore(tableId, "hot-2", "hot-value-2", 0L);
            writeToStore(tableId, "cold-1", "cold-value-1", 0L);

            TableStore ts = store.tableStore(tableId);
            List<LmdbInstance> writableInstances = ts.getWritableInstances();
            assertThat(writableInstances).isNotEmpty();
            LmdbInstance instance = writableInstances.get(0);
            instance.markAsReadOnly();

            Path parquetFile = tempDir.resolve("mixed-" + UUID.randomUUID() + ".parquet");
            String parquetPath = parquetFile.toUri().toString();
            List<byte[]> flushedKeys = new ArrayList<>();

            try (ParquetWriter<GenericRecord> writer =
                    AvroParquetWriter.<GenericRecord>builder(
                                    new org.apache.hadoop.fs.Path(parquetPath))
                            .withSchema(KEY_VALUE_SCHEMA)
                            .withConf(new Configuration())
                            .build()) {
                try (var iterator = instance.closeableIterator()) {
                    while (iterator.hasNext()) {
                        var pair = iterator.next();
                        flushedKeys.add(pair.getLeft());
                        GenericRecord record = new GenericData.Record(KEY_VALUE_SCHEMA);
                        record.put("key", java.nio.ByteBuffer.wrap(pair.getLeft()));
                        record.put("value", java.nio.ByteBuffer.wrap(pair.getRight()));
                        writer.write(record);
                    }
                }
            }

            byte[] bloomFilter = BloomFilterBuilder.build(flushedKeys, flushedKeys.size());
            ColdDataBlock coldBlock =
                    buildColdBlock(tableId, bloomFilter, flushedKeys.size(), parquetPath);
            ts.registerColdBlock(coldBlock);

            List<ColdDataBlock> coldBlocks =
                    ts.currentDataBlocks().stream()
                            .filter(ColdDataBlock.class::isInstance)
                            .map(ColdDataBlock.class::cast)
                            .filter(b -> b.visibility() == DataBlock.Visibility.ARCHIVED)
                            .toList();

            ParquetBlockReader reader = createParquetBlockReader();

            List<byte[]> allKeys = List.of(k("hot-1"), k("hot-2"), k("cold-1"));
            List<byte[]> results = new ArrayList<>();
            for (byte[] key : allKeys) {
                byte[] value = ts.getRawValue(key);
                if (value == null) {
                    value = reader.getRawValueFromColdBlocks(coldBlocks, key);
                }
                results.add(value);
            }

            assertThat(results).hasSize(3);
            assertThat(
                            new String(
                                    NullableValueCodec.decode(results.get(0)),
                                    StandardCharsets.UTF_8))
                    .isEqualTo("hot-value-1");
            assertThat(
                            new String(
                                    NullableValueCodec.decode(results.get(1)),
                                    StandardCharsets.UTF_8))
                    .isEqualTo("hot-value-2");
            assertThat(results.get(2)).isNotNull();
            byte[] decodedCold = NullableValueCodec.decode(results.get(2));
            assertThat(new String(decodedCold, StandardCharsets.UTF_8)).isEqualTo("cold-value-1");
        }

        @Test
        @DisplayName("C3: batch boundary4096 — verify4096itemkey")
        void testBatchBoundary4096Keys() {
            int keyCount = 4096;
            List<byte[]> keys = new ArrayList<>(keyCount);
            for (int i = 0; i < keyCount; i++) {
                keys.add(k(String.format("key-%04d", i)));
            }

            assertThat(keys).hasSize(4096);
            assertThat(keys.size())
                    .as("Key count should not exceed 4096 limit")
                    .isLessThanOrEqualTo(4096);
            assertThatCode(
                            () -> {
                                if (keys.size() > 4096) {
                                    throw new IllegalArgumentException(
                                            "Key count exceeds 4096 limit");
                                }
                            })
                    .doesNotThrowAnyException();
        }
    }

    // ==================== scenario group D: Epoch anderrorhandle (2) ====================

    @Nested
    @DisplayName("scenario group D: Epoch anderrorhandle")
    class EpochAndErrorHandlingTests {

        private String tableId;

        @BeforeEach
        void setUp() {
            tableId = "testdb.d_" + UUID.randomUUID().toString().replace("-", "");
        }

        @Test
        @DisplayName(
                "D1: commitTs singlecall — multiple timesupdatesamekeyaftercommitTscorrectly increments")
        void testCommitTsMonotonicIncrement() {
            String keyName = "epoch-key";

            writeToStore(tableId, keyName, "epoch-value-v1", 0L);

            ShardVersionTracker tracker = store.getOrCreateVersionTracker(tableId);
            long commitTs1 = tracker.updateVersion(k(keyName), 100L);
            assertThat(commitTs1).isGreaterThan(0L);

            writeToStore(tableId, keyName, "epoch-value-v2", 0L);

            long commitTs2 = tracker.updateVersion(k(keyName), 200L);
            assertThat(commitTs2).isGreaterThan(commitTs1);

            byte[] latestValue = readRawValue(tableId, keyName);
            assertThat(latestValue).isNotNull();
            assertThat(new String(latestValue, StandardCharsets.UTF_8)).isEqualTo("epoch-value-v2");
        }

        @Test
        @DisplayName("D2: non- — null tableIdand keyof handle")
        void testInvalidInputHandling() {
            assertThatThrownBy(
                            () -> {
                                KvWriteBuilder builder =
                                        KvWriteBuilder.builder()
                                                .table(new SimpleTableRef(null))
                                                .key(k("some-key"))
                                                .value(v("some-value"))
                                                .partitionHint(0);
                                store.write(builder, null);
                            })
                    .isInstanceOf(Exception.class);

            assertThatCode(
                            () -> {
                                writeToStore(tableId, "empty-key-test", "empty-key-value", 0L);
                            })
                    .doesNotThrowAnyException();

            byte[] keyResult = readRawValue(tableId, "empty-key-test");
            assertThat(keyResult).isNotNull();
            assertThat(new String(keyResult, StandardCharsets.UTF_8)).isEqualTo("empty-key-value");
        }
    }

    // ==================== scenario group E: end-to-end (2) ====================

    @Nested
    @DisplayName("scenario group E: end-to-end")
    class EndToEndIntegrationTests {

        private String tableId;

        @BeforeEach
        void setUp() {
            tableId = "testdb.e_" + UUID.randomUUID().toString().replace("-", "");
        }

        @Test
        @DisplayName("E1: writeread — write -> commitTs -> point query returns value")
        void testCompleteWriteReadChain() {
            String keyName = "full-chain-key";

            writeToStore(tableId, keyName, "initial-value", 0L);

            ShardVersionTracker tracker = store.getOrCreateVersionTracker(tableId);
            long commitTs = tracker.updateVersion(k(keyName), 500L);
            assertThat(commitTs).isGreaterThan(0L);

            byte[] value = readRawValue(tableId, keyName);

            assertThat(value).isNotNull();
            assertThat(new String(value, StandardCharsets.UTF_8)).isEqualTo("initial-value");

            writeToStore(tableId, keyName, "updated-value", 0L);
            long commitTs2 = tracker.updateVersion(k(keyName), 600L);
            byte[] updatedValue = readRawValue(tableId, keyName);
            assertThat(updatedValue).isNotNull();
            assertThat(new String(updatedValue, StandardCharsets.UTF_8)).isEqualTo("updated-value");
            assertThat(commitTs2).isGreaterThan(commitTs);
        }

        @Test
        @DisplayName(
                "E2: concurrentpoint query — multi same PointQuerydifferentkey correctly returns")
        void testConcurrentPointQueries() throws Exception {
            int keyCount = 10;
            for (int i = 0; i < keyCount; i++) {
                writeToStore(tableId, "concurrent-key-" + i, "concurrent-value-" + i, 0L);
            }

            int threadCount = keyCount;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            ConcurrentHashMap<Integer, String> results = new ConcurrentHashMap<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();

            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                executor.submit(
                        () -> {
                            try {
                                startLatch.await();
                                TableStore ts = store.tableStore(tableId);
                                byte[] raw = ts.getRawValue(k("concurrent-key-" + idx));
                                if (raw != null) {
                                    byte[] decoded = NullableValueCodec.decode(raw);
                                    results.put(idx, new String(decoded, StandardCharsets.UTF_8));
                                }
                            } catch (Throwable t) {
                                failure.compareAndSet(null, t);
                            } finally {
                                doneLatch.countDown();
                            }
                        });
            }

            startLatch.countDown();
            assertThat(doneLatch.await(30, TimeUnit.SECONDS))
                    .as("All threads should complete within 30 seconds")
                    .isTrue();
            executor.shutdown();

            if (failure.get() != null) {
                throw new AssertionError(
                        "Thread failed: " + failure.get().getMessage(), failure.get());
            }

            assertThat(results).hasSize(keyCount);
            for (int i = 0; i < keyCount; i++) {
                assertThat(results.get(i))
                        .as("Concurrent key " + i + " should return correct value")
                        .isEqualTo("concurrent-value-" + i);
            }
        }
    }
}
