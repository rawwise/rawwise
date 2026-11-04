package io.github.rawwise.model.op;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.HotDataBlock;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.table.read.TableReader;
import io.github.rawwise.engine.txn.OperationSet;
import io.github.rawwise.model.record.DynamicTableInfo;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableMeta;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ReadPathConvergenceE2ETest {

    private static HotDataBlock createTestBlock(String tableId) {
        return new HotDataBlock(
                "block-1",
                tableId,
                DataBlock.Kind.MUTABLE_HOT,
                DataBlock.Format.LMDB,
                new DataBlock.Location(
                        "local", new Storage.StoragePath("file", "/tmp/test"), 0L, 100L),
                KeyRange.FULL_RANGE,
                0L,
                100L,
                0L,
                DataBlock.Visibility.ACTIVE,
                0L,
                100L,
                0L,
                100L,
                "region-1");
    }

    private static HotDataBlock createTestBlock(String tableId, KeyRange keyRange) {
        return new HotDataBlock(
                "block-1",
                tableId,
                DataBlock.Kind.MUTABLE_HOT,
                DataBlock.Format.LMDB,
                new DataBlock.Location(
                        "local", new Storage.StoragePath("file", "/tmp/test"), 0L, 100L),
                keyRange,
                0L,
                100L,
                0L,
                DataBlock.Visibility.ACTIVE,
                0L,
                100L,
                0L,
                100L,
                "region-1");
    }

    private record TestTable(String uniqId) implements RawWiseTable {
        @Override
        public void close() {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void refresh() {}

        @Override
        public TableMeta meta() {
            return null;
        }

        @Override
        public DynamicTableInfo dynamicInfo() {
            return null;
        }

        @Override
        public Snapshot currentSnapshot() {
            return null;
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            return null;
        }

        @Override
        public List<Snapshot> snapshots() {
            return List.of();
        }

        @Override
        public String getTableIdentifier() {
            return uniqId;
        }
    }

    private static class CountingReader implements TableReader {
        final AtomicInteger findCount = new AtomicInteger(0);
        final AtomicInteger scanCount = new AtomicInteger(0);
        volatile Find.Result findResult = Find.Result.notFound();
        volatile Scan.Result scanResult = Scan.Result.ok(List.of(), false);

        @Override
        public Read.Result read(Read read) throws IOException {
            if (read instanceof Find f) return find(f);
            if (read instanceof Scan s) return scan(s);
            return Scan.Result.error("Unsupported", null);
        }

        @Override
        public Find.Result find(Find find) throws IOException {
            findCount.incrementAndGet();
            return findResult;
        }

        @Override
        public Scan.Result scan(Scan scan) throws IOException {
            scanCount.incrementAndGet();
            return scanResult;
        }

        @Override
        public void close() throws IOException {}

        @Override
        public boolean isClosed() {
            return false;
        }
    }

    private static class RecordingReadExecutor implements ReadExecutor {
        final TableReader delegate;
        final OperationSet operationSet;
        final AtomicInteger executeCount = new AtomicInteger(0);

        RecordingReadExecutor(TableReader delegate, OperationSet operationSet) {
            this.delegate = delegate;
            this.operationSet = operationSet;
        }

        @Override
        public Read.Result execute(Read read) throws IOException {
            executeCount.incrementAndGet();
            Read.Result result;
            if (read instanceof Find f) {
                result = delegate.find(f);
            } else {
                result = delegate.read(read);
            }

            if (result.success()) {
                if (read instanceof Find f) {
                    Map<String, Comparable> pkValues = f.primaryKey().values();
                    if (pkValues != null && !pkValues.isEmpty()) {
                        byte[] key = pkValues.toString().getBytes();
                        operationSet.recordRead(key, 100L);
                    }
                } else if (read instanceof Scan s) {
                    KeyRange keyRange = s.keyRange();
                    if (keyRange != null && !keyRange.isFullRange()) {
                        operationSet.recordRangeRead(keyRange.startKey(), keyRange.endKey(), 100L);
                    }
                }
            }
            return result;
        }
    }

    @Nested
    @DisplayName("T1: ReadExecutorroutingverify")
    class ReadExecutorRoutingTests {

        @Test
        @DisplayName("E2E-1: Find should route through ReadExecutor when provided")
        void shouldRouteFindThroughReadExecutor() throws IOException {
            TestTable table = new TestTable("catalog.db.t1");
            List<DataBlock> blocks = List.of(createTestBlock(table.uniqId()));
            CountingReader reader = new CountingReader();
            reader.findResult = Find.Result.notFound();

            AtomicBoolean executorCalled = new AtomicBoolean(false);
            ReadExecutor executor =
                    read -> {
                        executorCalled.set(true);
                        return reader.find((Find) read);
                    };

            Find find =
                    Find.builder()
                            .withTable(table)
                            .withPrimaryKey(Map.of("id", 42L))
                            .withReadExecutor(executor)
                            .build();

            Find.Result result = find.exec();

            assertThat(result.success()).isTrue();
            assertThat(executorCalled.get()).isTrue();
            assertThat(reader.findCount.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("E2E-2: KvScan should route through ReadExecutor when provided")
        void shouldRouteScanThroughReadExecutor() {
            TestTable table = new TestTable("catalog.db.t2");
            List<DataBlock> blocks = List.of(createTestBlock(table.uniqId()));
            CountingReader reader = new CountingReader();
            reader.scanResult = Scan.Result.ok(List.of(), false);

            AtomicBoolean executorCalled = new AtomicBoolean(false);
            ReadExecutor executor =
                    read -> {
                        executorCalled.set(true);
                        return reader.scan((Scan) read);
                    };

            KvScan scan =
                    KvScan.builder()
                            .withTable(table)
                            .withDataBlocks(blocks)
                            .withReadExecutor(executor)
                            .withKeyRange(KeyRange.FULL_RANGE)
                            .build();

            Scan.Result result = scan.exec();

            assertThat(result.success()).isTrue();
            assertThat(executorCalled.get()).isTrue();
        }

        @Test
        @DisplayName("E2E-3: BatchScan should route through ReadExecutor when provided")
        void shouldRouteBatchScanThroughReadExecutor() {
            TestTable table = new TestTable("catalog.db.t3");
            List<DataBlock> blocks = List.of(createTestBlock(table.uniqId()));
            CountingReader reader = new CountingReader();
            reader.scanResult = Scan.Result.ok(List.of(), false);

            AtomicBoolean executorCalled = new AtomicBoolean(false);
            ReadExecutor executor =
                    read -> {
                        executorCalled.set(true);
                        return reader.scan((Scan) read);
                    };

            BatchScan scan =
                    BatchScan.builder()
                            .withTable(table)
                            .withDataBlocks(blocks)
                            .withReadExecutor(executor)
                            .build();

            Scan.Result result = scan.exec();

            assertThat(result.success()).isTrue();
            assertThat(executorCalled.get()).isTrue();
        }
    }

    @Nested
    @DisplayName("T2: noReadExecutor fallbacktodirectlyReader")
    class FallbackToDirectReaderTests {

        @Test
        @DisplayName("E2E-4: Find should fallback to direct reader when no ReadExecutor")
        void shouldFallbackToDirectReaderForFind() {
            TestTable table = new TestTable("catalog.db.t4");
            CountingReader reader = new CountingReader();
            reader.findResult = Find.Result.notFound();

            Find find =
                    Find.builder()
                            .withTable(table)
                            .withPrimaryKey(Map.of("id", 42L))
                            .withReader(reader)
                            .build();

            Find.Result result = find.exec();

            assertThat(result.success()).isTrue();
            assertThat(reader.findCount.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("E2E-5: KvScan should fallback to direct reader when no ReadExecutor")
        void shouldFallbackToDirectReaderForScan() {
            TestTable table = new TestTable("catalog.db.t5");
            List<DataBlock> blocks = List.of(createTestBlock(table.uniqId()));
            CountingReader reader = new CountingReader();
            reader.scanResult = Scan.Result.ok(List.of(), false);

            KvScan scan =
                    KvScan.builder()
                            .withTable(table)
                            .withDataBlocks(blocks)
                            .withReader(reader)
                            .withKeyRange(KeyRange.FULL_RANGE)
                            .build();

            Scan.Result result = scan.exec();

            assertThat(result.success()).isTrue();
            assertThat(reader.scanCount.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("E2E-6: BatchScan should fallback to direct reader when no ReadExecutor")
        void shouldFallbackToDirectReaderForBatchScan() {
            TestTable table = new TestTable("catalog.db.t6");
            List<DataBlock> blocks = List.of(createTestBlock(table.uniqId()));
            CountingReader reader = new CountingReader();
            reader.scanResult = Scan.Result.ok(List.of(), false);

            BatchScan scan =
                    BatchScan.builder()
                            .withTable(table)
                            .withDataBlocks(blocks)
                            .withReader(reader)
                            .build();

            Scan.Result result = scan.exec();

            assertThat(result.success()).isTrue();
            assertThat(reader.scanCount.get()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("T3: ReadExecutorrecordOperationSet")
    class OperationSetRecordingTests {

        @Test
        @DisplayName("E2E-7: Find should record read in OperationSet via ReadExecutor")
        void shouldRecordFindReadInOperationSet() {
            TestTable table = new TestTable("catalog.db.t7");
            CountingReader reader = new CountingReader();
            reader.findResult = Find.Result.notFound();

            OperationSet opSet = new OperationSet();
            RecordingReadExecutor executor = new RecordingReadExecutor(reader, opSet);

            Find find =
                    Find.builder()
                            .withTable(table)
                            .withPrimaryKey(Map.of("id", 42L))
                            .withReadExecutor(executor)
                            .build();

            Find.Result result = find.exec();

            assertThat(result.success()).isTrue();
            assertThat(executor.executeCount.get()).isEqualTo(1);
            assertThat(opSet.isEmpty()).isFalse();
            assertThat(opSet.readEntries()).isNotEmpty();
        }

        @Test
        @DisplayName("E2E-8: KvScan should record range read in OperationSet via ReadExecutor")
        void shouldRecordScanReadSet() {
            TestTable table = new TestTable("catalog.db.t8");
            byte[] startKey = new byte[] {0x01};
            byte[] endKey = new byte[] {0x0A};
            KeyRange keyRange = new KeyRange(startKey, true, endKey, true);
            List<DataBlock> blocks = List.of(createTestBlock(table.uniqId(), keyRange));
            CountingReader reader = new CountingReader();
            reader.scanResult = Scan.Result.ok(List.of(), false);

            OperationSet opSet = new OperationSet();
            RecordingReadExecutor executor = new RecordingReadExecutor(reader, opSet);

            KvScan scan =
                    KvScan.builder()
                            .withTable(table)
                            .withDataBlocks(blocks)
                            .withReadExecutor(executor)
                            .withKeyRange(keyRange)
                            .build();

            Scan.Result result = scan.exec();

            assertThat(result.success()).isTrue();
            assertThat(executor.executeCount.get()).isEqualTo(1);
            assertThat(opSet.isEmpty()).isFalse();
            assertThat(opSet.rangeReadEntries()).isNotEmpty();
        }

        @Test
        @DisplayName("E2E-9: Full range scan should not record range read")
        void shouldNotRecordFullRangeScanInOperationSet() {
            TestTable table = new TestTable("catalog.db.t9");
            List<DataBlock> blocks = List.of(createTestBlock(table.uniqId()));
            CountingReader reader = new CountingReader();
            reader.scanResult = Scan.Result.ok(List.of(), false);

            OperationSet opSet = new OperationSet();
            RecordingReadExecutor executor = new RecordingReadExecutor(reader, opSet);

            KvScan scan =
                    KvScan.builder()
                            .withTable(table)
                            .withDataBlocks(blocks)
                            .withReadExecutor(executor)
                            .withKeyRange(KeyRange.FULL_RANGE)
                            .build();

            Scan.Result result = scan.exec();

            assertThat(result.success()).isTrue();
            assertThat(opSet.rangeReadEntries()).isEmpty();
        }
    }

    @Nested
    @DisplayName("T4: Builderverify")
    class BuilderValidationTests {

        @Test
        @DisplayName("E2E-10: Find build should fail when neither reader nor readExecutor provided")
        void shouldFailWhenNoReaderAndNoExecutor() {
            TestTable table = new TestTable("catalog.db.t10");

            assertThatThrownBy(
                            () ->
                                    Find.builder()
                                            .withTable(table)
                                            .withPrimaryKey(Map.of("id", 42L))
                                            .build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("reader is required");
        }

        @Test
        @DisplayName("E2E-11: Find build should succeed with only ReadExecutor (no reader)")
        void shouldSucceedWithOnlyReadExecutor() {
            TestTable table = new TestTable("catalog.db.t11");
            ReadExecutor executor = read -> Find.Result.notFound();

            Find find =
                    Find.builder()
                            .withTable(table)
                            .withPrimaryKey(Map.of("id", 42L))
                            .withReadExecutor(executor)
                            .build();

            assertThat(find).isNotNull();
        }

        @Test
        @DisplayName(
                "E2E-12: KvScan build should fail when neither reader nor readExecutor provided")
        void shouldFailKvScanWhenNoReaderAndNoExecutor() {
            TestTable table = new TestTable("catalog.db.t12");

            assertThatThrownBy(
                            () ->
                                    KvScan.builder()
                                            .withTable(table)
                                            .withDataBlocks(
                                                    List.of(createTestBlock(table.uniqId())))
                                            .build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("reader is required");
        }
    }
}
