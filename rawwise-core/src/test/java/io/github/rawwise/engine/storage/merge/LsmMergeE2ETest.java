package io.github.rawwise.engine.storage.merge;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.HotDataBlock;
import io.github.rawwise.engine.storage.block.read.DataBlockReader;
import io.github.rawwise.engine.storage.table.read.TableReader;
import io.github.rawwise.engine.txn.SimpleTableRef;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.OpType;
import io.github.rawwise.model.op.Read;
import io.github.rawwise.model.op.Scan;
import io.github.rawwise.model.record.DefaultRecord;
import io.github.rawwise.model.record.ImmutabilitySource;
import io.github.rawwise.model.record.RawWiseType;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Types;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("LsmMergeReader boundary conditions + error E2E test")
class LsmMergeE2ETest {

    static Record makeRecord(long seqNum, Record.Kind kind) {
        Record.Key k = Records.keyOf("stub".getBytes());
        Record.Header h =
                new Record.Header(
                        k, 0, 0, seqNum, 0, kind, true, false, ImmutabilitySource.MUTABLE);
        Record.Body b =
                new Record.Body() {
                    @Override
                    public Schema schema() {
                        return Schema.builder().field("v", Types.binary()).build();
                    }

                    @Override
                    public Record.Value value(String fn) {
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
                                return new byte[0];
                            }

                            @Override
                            public byte[] rawBytes() {
                                return new byte[0];
                            }
                        };
                    }

                    @Override
                    public Record.Value value(Record.FieldId fid) {
                        return value(fid.name());
                    }

                    @Override
                    public boolean has(String fn) {
                        return true;
                    }

                    @Override
                    public boolean has(Record.FieldId fid) {
                        return true;
                    }
                };
        return new DefaultRecord(k, b, h);
    }

    static DataBlock dummyBlock() {
        return new HotDataBlock(
                "dummy",
                "t",
                DataBlock.Kind.MUTABLE_HOT,
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
                null);
    }

    static Find dummyFind() {
        return Find.builder()
                .withTable(new SimpleTableRef("dummy_t"))
                .withPrimaryKey(Map.of("id", 1))
                .withDataBlocks(List.of(dummyBlock()))
                .withReader(
                        new TableReader() {
                            @Override
                            public Read.Result read(Read read) {
                                return Find.Result.notFound();
                            }

                            @Override
                            public Find.Result find(Find f) {
                                return Find.Result.notFound();
                            }

                            @Override
                            public Scan.Result scan(Scan s) {
                                return Scan.Result.ok(List.of(), false);
                            }

                            @Override
                            public void close() {}

                            @Override
                            public boolean isClosed() {
                                return false;
                            }
                        })
                .build();
    }

    static Scan dummyScan() {
        return new Scan() {
            @Override
            public List<DataBlock> plan() {
                return List.of(dummyBlock());
            }

            @Override
            public List<String> projections() {
                return List.of();
            }

            @Override
            public io.github.rawwise.engine.storage.KeyRange keyRange() {
                return null;
            }

            @Override
            public List<DataBlock> getDataBlocks() {
                return List.of(dummyBlock());
            }

            @Override
            public io.github.rawwise.model.expression.Expression getPushedPredicate() {
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

    // ====================================================================
    // S4-1: Empty reader list
    // ====================================================================

    @Nested
    @DisplayName("S4-1: Empty reader list")
    class EmptyReaders {

        @Test
        @DisplayName("find with empty reader list returns notFound")
        void testEmptyReaderFind() throws IOException {
            try (LsmMergeReader reader = new LsmMergeReader(List.of())) {
                Find.Result r = reader.find(dummyFind(), List.of(dummyBlock()));
                assertThat(r.isFound()).isFalse();
            }
        }

        @Test
        @DisplayName("scan with empty reader list returns empty")
        void testEmptyReaderScan() throws IOException {
            try (LsmMergeReader reader = new LsmMergeReader(List.of())) {
                Scan.Result r = reader.scan(dummyScan(), List.of(dummyBlock()));
                assertThat(r.success()).isTrue();
                assertThat(r.data()).isEmpty();
            }
        }
    }

    // ====================================================================
    // S4-2: Close and lifecycle
    // ====================================================================

    @Nested
    @DisplayName("S4-2: Close and lifecycle")
    class CloseLifecycle {

        @Test
        @DisplayName("close marks merge reader as closed")
        void testCloseSetsClosed() throws IOException {
            LsmMergeReader reader = new LsmMergeReader(List.of(new TrackedStub()));
            reader.close();
            assertThat(reader.isClosed()).isTrue();
        }

        @Test
        @DisplayName("supports returns true via delegate")
        void testSupportsDelegation() throws IOException {
            LsmMergeReader reader = new LsmMergeReader(List.of(new TrackedStub()));
            assertThat(reader.supports(dummyBlock())).isTrue();
            reader.close();
        }

        @Test
        @DisplayName("isClosed false before close, true after close")
        void testIsClosedStateTransition() throws IOException {
            LsmMergeReader reader = new LsmMergeReader(List.of(new TrackedStub()));
            assertThat(reader.isClosed()).isFalse();
            reader.close();
            assertThat(reader.isClosed()).isTrue();
        }
    }

    // ====================================================================
    // S4-3: find with various reader results
    // ====================================================================

    @Nested
    @DisplayName("S4-3: Find with reader result combinations")
    class FoundRecords {

        @Test
        @DisplayName("single reader returns found -> merge reader returns found")
        void testSingleReaderFindFound() throws IOException {
            LsmMergeReader reader =
                    new LsmMergeReader(
                            List.of(
                                    new FixedResultReader(
                                            Find.Result.found(
                                                    makeRecord(1L, Record.Kind.INSERT)))));
            Find.Result r = reader.find(dummyFind(), List.of(dummyBlock()));
            assertThat(r.isFound()).isTrue();
            assertThat(r.foundRecord()).isPresent();
            reader.close();
        }

        @Test
        @DisplayName("single reader returns notFound -> merge reader returns notFound")
        void testSingleReaderFindNotFound() throws IOException {
            LsmMergeReader reader =
                    new LsmMergeReader(List.of(new FixedResultReader(Find.Result.notFound())));
            Find.Result r = reader.find(dummyFind(), List.of(dummyBlock()));
            assertThat(r.isFound()).isFalse();
            reader.close();
        }

        @Test
        @DisplayName("two readers, 2nd returns found -> result is found")
        void testTwoReadersSecondReturnsFound() throws IOException {
            LsmMergeReader reader =
                    new LsmMergeReader(
                            List.of(
                                    new FixedResultReader(Find.Result.notFound()),
                                    new FixedResultReader(
                                            Find.Result.found(
                                                    makeRecord(10L, Record.Kind.INSERT)))));
            Find.Result r = reader.find(dummyFind(), List.of(dummyBlock()));
            assertThat(r.isFound()).isTrue();
            reader.close();
        }
    }

    // ====================================================================
    // Stub readers
    // ====================================================================

    static class TrackedStub implements DataBlockReader {
        boolean closed;

        @Override
        public boolean supports(DataBlock b) {
            return true;
        }

        @Override
        public Find.Result find(Find f, List<DataBlock> b) {
            return Find.Result.notFound();
        }

        @Override
        public Scan.Result scan(Scan s, List<DataBlock> b) {
            return Scan.Result.ok(List.of(), false);
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    static class FixedResultReader implements DataBlockReader {
        final Find.Result result;
        boolean closed;

        FixedResultReader(Find.Result result) {
            this.result = result;
        }

        @Override
        public boolean supports(DataBlock b) {
            return true;
        }

        @Override
        public Find.Result find(Find f, List<DataBlock> b) {
            return result;
        }

        @Override
        public Scan.Result scan(Scan s, List<DataBlock> b) {
            return Scan.Result.ok(List.of(), false);
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
