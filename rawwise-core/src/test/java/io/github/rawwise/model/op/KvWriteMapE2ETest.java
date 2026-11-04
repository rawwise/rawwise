package io.github.rawwise.model.op;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.engine.storage.table.key.PrimaryKeyKeyCodec;
import io.github.rawwise.engine.storage.writer.RecordWriter;
import io.github.rawwise.engine.storage.writer.WriteResult;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Types;
import io.github.rawwise.model.record.WriteEntry;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class KvWriteMapE2ETest {

    private Schema createSchemaWithPk() {
        return Schema.builder()
                .field(1, "id", Types.int64(false))
                .field(2, "name", Types.string())
                .primaryKeys("id")
                .build();
    }

    private Schema createSchemaNoPk() {
        return Schema.builder()
                .field(1, "key", Types.string())
                .field(2, "value", Types.string())
                .build();
    }

    private Schema createMultiFieldSchema() {
        return Schema.builder()
                .field(1, "id", Types.int64(false))
                .field(2, "name", Types.string())
                .field(3, "age", Types.int32())
                .primaryKeys("id")
                .build();
    }

    @Nested
    @DisplayName("T1: convertMapToRecord fieldId-firstmatch")
    class FieldIdFirstMatchingTests {

        @Test
        @DisplayName("E2E-1: Map fieldNamewritecancorrectly matches")
        void shouldMatchByFieldName() {
            Schema schema = createSchemaWithPk();
            KeyCodec keyCodec = new PrimaryKeyKeyCodec(List.of("id"));

            Map<String, Object> mapEntry = new HashMap<>();
            mapEntry.put("id", 42L);
            mapEntry.put("name", "alice");

            CollectingWriter writer = new CollectingWriter();
            KvWrite kvWrite =
                    new KvWrite(null, List.of(mapEntry), null, writer, schema, keyCodec, null);
            kvWrite.exec();

            assertThat(writer.entries).hasSize(1);
            assertThat(writer.entries.get(0).key()).isNotEmpty();
            assertThat(writer.entries.get(0).encodedValue()).isNotEmpty();
        }

        @Test
        @DisplayName("E2E-4: Map fieldId( key)writego throughfieldId-firstpath")
        void shouldMatchByFieldIdFirst() {
            Schema schema = createSchemaWithPk();
            KeyCodec keyCodec = new PrimaryKeyKeyCodec(List.of("id"));

            Map<String, Object> mapEntry = new HashMap<>();
            mapEntry.put("1", 42L);
            mapEntry.put("2", "alice");

            CollectingWriter writer = new CollectingWriter();
            KvWrite kvWrite =
                    new KvWrite(null, List.of(mapEntry), null, writer, schema, keyCodec, null);
            kvWrite.exec();

            assertThat(writer.entries).hasSize(1);
        }

        @Test
        @DisplayName("E2E-5: Mapin field skipthefield")
        void shouldHandleMissingField() {
            Schema schema = createMultiFieldSchema();
            KeyCodec keyCodec = new PrimaryKeyKeyCodec(List.of("id"));

            Map<String, Object> mapEntry = new HashMap<>();
            mapEntry.put("id", 42L);

            CollectingWriter writer = new CollectingWriter();
            KvWrite kvWrite =
                    new KvWrite(null, List.of(mapEntry), null, writer, schema, keyCodec, null);
            kvWrite.exec();

            assertThat(writer.entries).hasSize(1);
        }
    }

    @Nested
    @DisplayName("T2: convertSingleWriteEntry Schema")
    class SchemaDrivenConversionTests {

        @Test
        @DisplayName("E2E-2: multifieldMapwritecorrectly encodes")
        void shouldEncodeMultiFieldMap() {
            Schema schema = createMultiFieldSchema();
            KeyCodec keyCodec = new PrimaryKeyKeyCodec(List.of("id"));

            Map<String, Object> mapEntry = new HashMap<>();
            mapEntry.put("id", 42L);
            mapEntry.put("name", "bob");
            mapEntry.put("age", 30);

            CollectingWriter writer = new CollectingWriter();
            KvWrite kvWrite =
                    new KvWrite(null, List.of(mapEntry), null, writer, schema, keyCodec, null);
            kvWrite.exec();

            assertThat(writer.entries).hasSize(1);
            assertThat(writer.entries.get(0).key()).isNotEmpty();
        }

        @Test
        @DisplayName("E2E-3: primary keyfieldofMap KeyCodecencodekey")
        void shouldEncodePrimaryKeyFromMap() throws IOException {
            Schema schema = createSchemaWithPk();
            KeyCodec keyCodec = new PrimaryKeyKeyCodec(List.of("id"));

            Map<String, Comparable> expectedPkValues = new HashMap<>();
            expectedPkValues.put("id", 42L);
            byte[] expectedKey = keyCodec.encodePrimaryKey(expectedPkValues);

            Map<String, Object> mapEntry = new HashMap<>();
            mapEntry.put("id", 42L);
            mapEntry.put("name", "charlie");

            CollectingWriter writer = new CollectingWriter();
            KvWrite kvWrite =
                    new KvWrite(null, List.of(mapEntry), null, writer, schema, keyCodec, null);
            kvWrite.exec();

            assertThat(writer.entries).hasSize(1);
            assertThat(writer.entries.get(0).key()).isEqualTo(expectedKey);
        }

        @Test
        @DisplayName("noSchema go throughfallbackpath")
        void shouldFallbackWhenNoSchema() {
            Map<String, Object> mapEntry = new HashMap<>();
            mapEntry.put("key", "mykey");
            mapEntry.put("value", "myvalue");

            CollectingWriter writer = new CollectingWriter();
            KvWrite kvWrite = new KvWrite(null, List.of(mapEntry), writer);
            kvWrite.exec();

            assertThat(writer.entries).hasSize(1);
        }

        @Test
        @DisplayName("Mapcolumntable 0count")
        void shouldReturnZeroForEmptyData() {
            CollectingWriter writer = new CollectingWriter();
            KvWrite kvWrite = new KvWrite(null, List.of(), writer);
            Write.Result result = kvWrite.exec();

            assertThat(result.count()).isEqualTo(0);
        }

        @Test
        @DisplayName("directlyKVoperations")
        void shouldNotAffectDirectKvOperation() {
            CollectingWriter writer = new CollectingWriter();
            KvWrite kvWrite =
                    new KvWrite(null, "mykey".getBytes(), "myvalue".getBytes(), null, writer);
            Write.Result result = kvWrite.exec();

            assertThat(result.success()).isTrue();
            assertThat(result.count()).isEqualTo(1);
        }
    }

    private static class CollectingWriter implements RecordWriter<WriteEntry> {
        final List<WriteEntry> entries = new java.util.ArrayList<>();
        private long written;
        private long failed;
        private boolean closed;

        @Override
        public WriteResult write(WriteEntry entry) throws IOException {
            entries.add(entry);
            written++;
            return WriteResult.SUCCESS;
        }

        @Override
        public long recordsWritten() {
            return written;
        }

        @Override
        public long recordsFailed() {
            return failed;
        }

        @Override
        public void flush() throws IOException {}

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }
}
