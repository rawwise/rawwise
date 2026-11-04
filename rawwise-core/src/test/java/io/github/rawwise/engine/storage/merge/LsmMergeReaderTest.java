package io.github.rawwise.engine.storage.merge;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.StorageTestHelper;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.HotDataBlock;
import io.github.rawwise.engine.storage.block.read.DataBlockReader;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.OpType;
import io.github.rawwise.model.op.Scan;
import io.github.rawwise.model.record.DefaultRecord;
import io.github.rawwise.model.record.RawWiseType;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Types;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("LsmMergeReader E2Etest")
class LsmMergeReaderTest {

    @BeforeAll
    static void registerStorageProvider() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception e) {
        }
    }

    @TempDir Path tempDir;

    private DynamicMmapStore store;
    private String tableId;
    private KeyCodec keyCodec;

    @BeforeEach
    void setUp() throws Exception {
        tableId = "catalog.db.merge_test_" + UUID.randomUUID().toString().substring(0, 8);
        store =
                DynamicMmapStore.getInstance(
                        tempDir.toString(), "merge-store-" + UUID.randomUUID());
        keyCodec = store.tableStore(tableId).getKeyCodec();
        try {
            io.github.rawwise.engine.txn.state.TxnContextHolder.clear();
        } catch (Exception e) {
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    private void writeKv(Map<String, Comparable> pkValues, String value) throws IOException {
        StorageTestHelper.writeKv(store, tableId, pkValues, value);
    }

    private void deleteKey(Map<String, Comparable> pkValues) throws IOException {
        StorageTestHelper.deleteKey(store, tableId, pkValues);
    }

    private byte[] encodeKey(Map<String, Comparable> pkValues) throws IOException {
        return keyCodec.encodePrimaryKey(pkValues);
    }

    private List<DataBlock> allBlocks() {
        TableStore ts = store.tableStore(tableId);
        return ts.getAllInstancesDescending().stream()
                .map(
                        inst ->
                                (DataBlock)
                                        new HotDataBlock(
                                                inst.instanceId(),
                                                tableId,
                                                inst.readOnly()
                                                        ? DataBlock.Kind.IMMUTABLE_HOT
                                                        : DataBlock.Kind.MUTABLE_HOT,
                                                DataBlock.Format.LMDB,
                                                null,
                                                null,
                                                0,
                                                0,
                                                0,
                                                DataBlock.Visibility.ACTIVE,
                                                0,
                                                0,
                                                0,
                                                0,
                                                null))
                .toList();
    }

    private void ensureBlocksExist() throws IOException {
        if (allBlocks().isEmpty()) {
            writeKv(Map.of("id", "placeholder"), "placeholder");
        }
    }

    private Find testFind(Map<String, Comparable> pkValues) {
        List<DataBlock> blocks = allBlocks();
        TableStore ts = store.tableStore(tableId);
        return Find.builder()
                .withTable(new io.github.rawwise.engine.txn.SimpleTableRef(tableId))
                .withPrimaryKey(pkValues)
                .withDataBlocks(blocks)
                .withReader(ts.reader())
                .build();
    }

    private Scan testScan() {
        List<DataBlock> blocks = allBlocks();
        return new Scan() {
            @Override
            public List<DataBlock> plan() {
                return blocks;
            }

            @Override
            public List<String> projections() {
                return List.of();
            }

            @Override
            public KeyRange keyRange() {
                return null;
            }

            @Override
            public List<DataBlock> getDataBlocks() {
                return blocks;
            }

            @Override
            public Expression getPushedPredicate() {
                return null;
            }

            @Override
            public boolean isPrimaryKeyLookup() {
                return false;
            }

            @Override
            public long estimatedSize() {
                return 0;
            }

            @Override
            public Scan.Result exec() {
                return Scan.Result.ok(List.of(), false);
            }

            @Override
            public OpType type() {
                return OpType.KV_SCAN;
            }
        };
    }

    private Record makeRecord(long seqNum, Record.Kind kind) {
        Record.Key recordKey = Records.keyOf("test-key".getBytes(StandardCharsets.UTF_8));
        Record.Header header = Record.Header.of(recordKey, 0, 0, seqNum, 0, kind, true);
        Record.Body body =
                new Record.Body() {
                    final Schema bodySchema =
                            Schema.builder().field("value", Types.binary()).build();

                    @Override
                    public Schema schema() {
                        return bodySchema;
                    }

                    @Override
                    public Record.Value value(String fieldName) {
                        return new Record.Value() {
                            @Override
                            public RawWiseType type() {
                                return Types.binary();
                            }

                            @Override
                            public boolean isNull() {
                                return false;
                            }

                            @Override
                            public Object raw() {
                                return ("data-seq-" + seqNum).getBytes(StandardCharsets.UTF_8);
                            }

                            @Override
                            public byte[] rawBytes() {
                                return ("data-seq-" + seqNum).getBytes(StandardCharsets.UTF_8);
                            }
                        };
                    }

                    @Override
                    public Record.Value value(Record.FieldId fieldId) {
                        return value(fieldId.name());
                    }

                    @Override
                    public boolean has(String fieldName) {
                        return "value".equals(fieldName);
                    }

                    @Override
                    public boolean has(Record.FieldId fieldId) {
                        return has(fieldId.name());
                    }
                };
        return new DefaultRecord(recordKey, body, header);
    }

    private static final class StubReader implements DataBlockReader {
        private final List<Record> records;
        private boolean closed = false;

        StubReader(List<Record> records) {
            this.records = records;
        }

        @Override
        public boolean supports(DataBlock block) {
            return block != null;
        }

        @Override
        public Find.Result find(Find find, List<DataBlock> blocks) throws IOException {
            Record best = null;
            long bestSeq = Long.MIN_VALUE;
            for (Record r : records) {
                if (r.header().sequenceNumber() > bestSeq) {
                    best = r;
                    bestSeq = r.header().sequenceNumber();
                }
            }
            if (best != null) return Find.Result.found(best);
            return Find.Result.notFound();
        }

        @Override
        public Scan.Result scan(Scan scan, List<DataBlock> blocks) throws IOException {
            return Scan.Result.ok(new ArrayList<>(records), false);
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }

    @Nested
    @DisplayName("S1 Happy Path")
    class HappyPathTests {

        @Test
        @DisplayName(
                "multireader findmerge: tworeader differentseqNumofINSERT, LsmMergeReader column")
        void testMultiReaderFindMerge() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader reader1 = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));
            StubReader reader2 = new StubReader(List.of(makeRecord(200L, Record.Kind.INSERT)));

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(reader1, reader2))) {
                Find.Result result = mergeReader.find(testFind(Map.of("id", "test")), blocks);
                assertThat(result.isFound()).isTrue();
                assertThat(result.foundRecord()).isPresent();
                assertThat(result.foundRecord().get().header().sequenceNumber()).isEqualTo(200L);
            }
        }

        @Test
        @DisplayName("Tombstone find: DELETE column INSERT find notFound")
        void testTombstoneFoldingInFind() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader reader1 = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));
            StubReader reader2 = new StubReader(List.of(makeRecord(200L, Record.Kind.DELETE)));

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(reader1, reader2))) {
                Find.Result result = mergeReader.find(testFind(Map.of("id", "test")), blocks);
                assertThat(result.isFound()).isFalse();
            }
        }

        @Test
        @DisplayName("versionfind: samekeymultireaderwrite, column version")
        void testLatestVersionInFind() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader reader1 = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));
            StubReader reader2 = new StubReader(List.of(makeRecord(200L, Record.Kind.INSERT)));
            StubReader reader3 = new StubReader(List.of(makeRecord(150L, Record.Kind.INSERT)));

            try (LsmMergeReader mergeReader =
                    new LsmMergeReader(List.of(reader1, reader2, reader3))) {
                Find.Result result = mergeReader.find(testFind(Map.of("id", "test")), blocks);
                assertThat(result.isFound()).isTrue();
                assertThat(result.foundRecord()).isPresent();
                assertThat(result.foundRecord().get().header().sequenceNumber()).isEqualTo(200L);
            }
        }

        @Test
        @DisplayName("multireader scanmerge: tworeaderofrecord occurrenceinscan in")
        void testMultiReaderScanMerge() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader reader1 = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));
            StubReader reader2 = new StubReader(List.of(makeRecord(200L, Record.Kind.INSERT)));

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(reader1, reader2))) {
                Scan.Result scanResult = mergeReader.scan(testScan(), blocks);
                assertThat(scanResult.success()).isTrue();
                assertThat(scanResult.data()).hasSize(2);
            }
        }

        @Test
        @DisplayName("Tombstonefilterscan: DELETErecord occurrenceinscan in")
        void testTombstoneFilteringInScan() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader reader1 = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));
            StubReader reader2 =
                    new StubReader(
                            List.of(
                                    makeRecord(200L, Record.Kind.DELETE),
                                    makeRecord(300L, Record.Kind.INSERT)));

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(reader1, reader2))) {
                Scan.Result scanResult = mergeReader.scan(testScan(), blocks);
                assertThat(scanResult.success()).isTrue();

                List<Record> nonDeleted =
                        scanResult.data().stream()
                                .filter(r -> r.header().kind() != Record.Kind.DELETE)
                                .toList();
                assertThat(nonDeleted).hasSize(2);

                List<Record> deleted =
                        scanResult.data().stream()
                                .filter(r -> r.header().kind() == Record.Kind.DELETE)
                                .toList();
                assertThat(deleted).isEmpty();
            }
        }

        @Test
        @DisplayName(
                "writedeleteread : viaDynamicMmapStorewrite->delete->verifytombstone-> write->verifyrecovery")
        void testWriteDeleteRewriteFlow() throws IOException {
            Map<String, Comparable> pk = Map.of("id", "flow-key");

            writeKv(pk, "original");
            TableStore ts = store.tableStore(tableId);
            byte[] encodedKey = encodeKey(pk);

            byte[] raw1 = ts.getRawValue(encodedKey);
            assertThat(raw1).isNotNull();
            assertThat(NullableValueCodec.isTombstone(raw1)).isFalse();
            assertThat(new String(NullableValueCodec.decode(raw1), StandardCharsets.UTF_8))
                    .isEqualTo("original");

            deleteKey(pk);
            byte[] raw2 = ts.getRawValue(encodedKey);
            assertThat(raw2).isNotNull();
            assertThat(NullableValueCodec.isTombstone(raw2)).isTrue();

            writeKv(pk, "restored");
            byte[] raw3 = ts.getRawValue(encodedKey);
            assertThat(raw3).isNotNull();
            assertThat(NullableValueCodec.isTombstone(raw3)).isFalse();
            assertThat(new String(NullableValueCodec.decode(raw3), StandardCharsets.UTF_8))
                    .isEqualTo("restored");
        }
    }

    @Nested
    @DisplayName("S2 Concurrency")
    class ConcurrencyTests {

        @Test
        @DisplayName("multi-threaded concurrentfindviaLsmMergeReaderread correct")
        void testConcurrentFindAcrossReaders() throws Exception {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            int recordCount = 20;
            List<Record> records = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                records.add(makeRecord(100L + i, Record.Kind.INSERT));
            }
            StubReader stubReader = new StubReader(records);

            int threadCount = 8;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(stubReader))) {
                for (int t = 0; t < threadCount; t++) {
                    new Thread(
                                    () -> {
                                        try {
                                            startLatch.await();
                                            for (int i = 0; i < 3; i++) {
                                                Find.Result result =
                                                        mergeReader.find(
                                                                testFind(Map.of("id", "test")),
                                                                blocks);
                                                if (result.isFound()) {
                                                    successCount.incrementAndGet();
                                                }
                                            }
                                        } catch (Exception e) {
                                            errorCount.incrementAndGet();
                                        } finally {
                                            doneLatch.countDown();
                                        }
                                    })
                            .start();
                }

                startLatch.countDown();
                doneLatch.await();

                assertThat(errorCount.get()).isEqualTo(0);
                assertThat(successCount.get()).isEqualTo(threadCount * 3);
            }
        }
    }

    @Nested
    @DisplayName("S3 Fault Recovery")
    class FaultRecoveryTests {

        @Test
        @DisplayName("closeexception : LsmMergeReaderon close allreaderexception")
        void testCloseExceptionCollection() throws IOException {
            DataBlockReader throwingReader1 =
                    new DataBlockReader() {
                        @Override
                        public boolean supports(DataBlock block) {
                            return true;
                        }

                        @Override
                        public Find.Result find(Find find, List<DataBlock> blocks) {
                            return Find.Result.notFound();
                        }

                        @Override
                        public Scan.Result scan(Scan scan, List<DataBlock> blocks) {
                            return Scan.Result.ok(List.of(), false);
                        }

                        @Override
                        public boolean isClosed() {
                            return false;
                        }

                        @Override
                        public void close() throws IOException {
                            throw new IOException("reader-1-error");
                        }
                    };
            DataBlockReader throwingReader2 =
                    new DataBlockReader() {
                        @Override
                        public boolean supports(DataBlock block) {
                            return true;
                        }

                        @Override
                        public Find.Result find(Find find, List<DataBlock> blocks) {
                            return Find.Result.notFound();
                        }

                        @Override
                        public Scan.Result scan(Scan scan, List<DataBlock> blocks) {
                            return Scan.Result.ok(List.of(), false);
                        }

                        @Override
                        public boolean isClosed() {
                            return false;
                        }

                        @Override
                        public void close() throws IOException {
                            throw new IOException("reader-2-error");
                        }
                    };

            LsmMergeReader mergeReader =
                    new LsmMergeReader(List.of(throwingReader1, throwingReader2));

            try {
                mergeReader.close();
            } catch (IOException e) {
                assertThat(e.getMessage()).isEqualTo("reader-1-error");
                assertThat(e.getSuppressed()).hasSize(1);
                assertThat(e.getSuppressed()[0].getMessage()).isEqualTo("reader-2-error");
            }
        }

        @Test
        @DisplayName("partialreader faildoes not affectits reader: reader cancorrect")
        void testPartialCloseFailureDoesNotAffectOtherReaders() throws IOException {
            StubReader normalReader = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));
            DataBlockReader throwingReader =
                    new DataBlockReader() {
                        boolean closed = false;

                        @Override
                        public boolean supports(DataBlock block) {
                            return true;
                        }

                        @Override
                        public Find.Result find(Find find, List<DataBlock> blocks) {
                            return Find.Result.notFound();
                        }

                        @Override
                        public Scan.Result scan(Scan scan, List<DataBlock> blocks) {
                            return Scan.Result.ok(List.of(), false);
                        }

                        @Override
                        public boolean isClosed() {
                            return closed;
                        }

                        @Override
                        public void close() throws IOException {
                            closed = true;
                            throw new IOException("throw-on-close");
                        }
                    };

            LsmMergeReader mergeReader = new LsmMergeReader(List.of(normalReader, throwingReader));

            try {
                mergeReader.close();
            } catch (IOException e) {
                assertThat(e.getMessage()).isEqualTo("throw-on-close");
            }

            assertThat(normalReader.closed).isTrue();
        }

        @Test
        @DisplayName("scanin itemreader error itemscanin error")
        void testScanErrorPropagation() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader goodReader = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));
            DataBlockReader errorReader =
                    new DataBlockReader() {
                        @Override
                        public boolean supports(DataBlock block) {
                            return true;
                        }

                        @Override
                        public Find.Result find(Find find, List<DataBlock> blocks) {
                            return Find.Result.notFound();
                        }

                        @Override
                        public Scan.Result scan(Scan scan, List<DataBlock> blocks) {
                            return Scan.Result.error("simulated failure", null);
                        }

                        @Override
                        public boolean isClosed() {
                            return false;
                        }

                        @Override
                        public void close() {}
                    };

            try (LsmMergeReader mergeReader =
                    new LsmMergeReader(List.of(goodReader, errorReader))) {
                Scan.Result result = mergeReader.scan(testScan(), blocks);
                assertThat(result.success()).isFalse();
            }
        }
    }

    @Nested
    @DisplayName("S4 Boundary Conditions")
    class BoundaryConditionTests {

        @Test
        @DisplayName("readerscolumntable: find notFound, scanreturns empty results")
        void testEmptyReadersList() throws IOException {
            ensureBlocksExist();
            LsmMergeReader mergeReader = new LsmMergeReader(List.of());

            Find.Result findResult = mergeReader.find(testFind(Map.of("id", "test")), allBlocks());
            assertThat(findResult.isFound()).isFalse();

            Scan.Result scanResult = mergeReader.scan(testScan(), allBlocks());
            assertThat(scanResult.success()).isTrue();
            assertThat(scanResult.data()).isEmpty();

            mergeReader.close();
        }

        @Test
        @DisplayName("singleitemreader: LsmMergeReaderbehaviorand readerconsistent")
        void testSingleReader() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader singleReader = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(singleReader))) {
                Find.Result result = mergeReader.find(testFind(Map.of("id", "test")), blocks);
                assertThat(result.isFound()).isTrue();
            }
        }

        @Test
        @DisplayName("querynon-existent key: find notFoundnoexception")
        void testFindNonExistentKey() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader emptyReader = new StubReader(List.of());

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(emptyReader))) {
                Find.Result result =
                        mergeReader.find(testFind(Map.of("id", "nonexistent")), blocks);
                assertThat(result.isFound()).isFalse();
                assertThat(result.success()).isTrue();
            }
        }

        @Test
        @DisplayName("DELETE column INSERT find notFound")
        void testDeleteOverridesInsertInFind() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader reader1 = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));
            StubReader reader2 = new StubReader(List.of(makeRecord(200L, Record.Kind.DELETE)));

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(reader1, reader2))) {
                Find.Result result = mergeReader.find(testFind(Map.of("id", "test")), blocks);
                assertThat(result.isFound()).isFalse();
            }
        }

        @Test
        @DisplayName("INSERTrecord column DELETE find INSERT")
        void testInsertOverridesDeleteInFind() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader reader1 =
                    new StubReader(
                            List.of(
                                    makeRecord(100L, Record.Kind.INSERT),
                                    makeRecord(200L, Record.Kind.DELETE)));
            StubReader reader2 = new StubReader(List.of(makeRecord(300L, Record.Kind.INSERT)));

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(reader1, reader2))) {
                Find.Result result = mergeReader.find(testFind(Map.of("id", "test")), blocks);
                assertThat(result.isFound()).isTrue();
                assertThat(result.foundRecord()).isPresent();
                assertThat(result.foundRecord().get().header().sequenceNumber()).isEqualTo(300L);
                assertThat(result.foundRecord().get().header().kind())
                        .isEqualTo(Record.Kind.INSERT);
            }
        }

        @Test
        @DisplayName("allreaderall DELETE find notFound")
        void testAllReadersReturnDeleteFindReturnsNotFound() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            StubReader reader1 = new StubReader(List.of(makeRecord(100L, Record.Kind.DELETE)));
            StubReader reader2 = new StubReader(List.of(makeRecord(200L, Record.Kind.DELETE)));

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(reader1, reader2))) {
                Find.Result result = mergeReader.find(testFind(Map.of("id", "test")), blocks);
                assertThat(result.isFound()).isFalse();
            }
        }

        @Test
        @DisplayName("findpathreader error error")
        void testFindErrorPropagation() throws IOException {
            ensureBlocksExist();
            List<DataBlock> blocks = allBlocks();

            DataBlockReader errorReader =
                    new DataBlockReader() {
                        @Override
                        public boolean supports(DataBlock block) {
                            return true;
                        }

                        @Override
                        public Find.Result find(Find find, List<DataBlock> blocks) {
                            return Find.Result.error("find-error", null);
                        }

                        @Override
                        public Scan.Result scan(Scan scan, List<DataBlock> blocks) {
                            return Scan.Result.ok(List.of(), false);
                        }

                        @Override
                        public boolean isClosed() {
                            return false;
                        }

                        @Override
                        public void close() {}
                    };

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(errorReader))) {
                Find.Result result = mergeReader.find(testFind(Map.of("id", "test")), blocks);
                assertThat(result.success()).isFalse();
            }
        }

        @Test
        @DisplayName("supports : for non-null blockreturns true")
        void testSupportsReturnsTrueForBlocks() throws IOException {
            ensureBlocksExist();
            StubReader reader = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));

            try (LsmMergeReader mergeReader = new LsmMergeReader(List.of(reader))) {
                for (DataBlock block : allBlocks()) {
                    assertThat(mergeReader.supports(block)).isTrue();
                }
            }
        }

        @Test
        @DisplayName("isClosed : afterreturns true")
        void testIsClosedAfterClose() throws IOException {
            StubReader reader = new StubReader(List.of(makeRecord(100L, Record.Kind.INSERT)));
            LsmMergeReader mergeReader = new LsmMergeReader(List.of(reader));

            assertThat(mergeReader.isClosed()).isFalse();
            mergeReader.close();
            assertThat(mergeReader.isClosed()).isTrue();
            assertThat(reader.closed).isTrue();
        }
    }
}
