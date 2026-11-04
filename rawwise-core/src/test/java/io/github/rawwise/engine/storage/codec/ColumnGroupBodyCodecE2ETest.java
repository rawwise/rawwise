package io.github.rawwise.engine.storage.codec;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ColumnGroupBodyCodecE2ETest {

    private Schema createMultiCgSchema() {
        return Schema.builder()
                .field(1, "id", Types.int64())
                .field(2, "name", Types.string())
                .field(3, "email", Types.string())
                .field(4, "age", Types.int32())
                .field(5, "score", Types.float64())
                .build()
                .withColumnGroups(
                        List.of(
                                new ColumnGroup(
                                        "cg_profile",
                                        List.of("name", "email"),
                                        List.of(Types.string(), Types.string()),
                                        ColumnGroup.Encoding.FIXED_OFFSET,
                                        1),
                                new ColumnGroup(
                                        "cg_stats",
                                        List.of("age", "score"),
                                        List.of(Types.int32(), Types.float64()),
                                        ColumnGroup.Encoding.FIXED_OFFSET,
                                        1)));
    }

    private Map<String, Object> createRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 42L);
        row.put("name", "Alice");
        row.put("email", "alice@example.com");
        row.put("age", 30);
        row.put("score", 95.5);
        return row;
    }

    @Nested
    @DisplayName("FixedOffsetBodyCodec with ColumnGroup")
    class FixedOffsetColumnGroup {

        @Test
        @DisplayName("should encode and decode by ColumnGroup")
        void shouldEncodeDecodeByColumnGroup() {
            Schema schema = createMultiCgSchema();
            Map<String, Object> original = createRow();

            FixedOffsetBodyCodec codec = new FixedOffsetBodyCodec();
            byte[] encoded = codec.encode(schema, original);
            Map<String, Object> decoded = codec.decode(schema, encoded);

            assertThat(decoded.get("id")).isEqualTo(42L);
            assertThat(decoded.get("name")).isEqualTo("Alice");
            assertThat(decoded.get("email")).isEqualTo("alice@example.com");
            assertThat(decoded.get("age")).isEqualTo(30);
            assertThat(decoded.get("score")).isEqualTo(95.5);
        }

        @Test
        @DisplayName("should decode single field from ColumnGroup")
        void shouldDecodeSingleFieldFromColumnGroup() {
            Schema schema = createMultiCgSchema();
            Map<String, Object> original = createRow();

            FixedOffsetBodyCodec codec = new FixedOffsetBodyCodec();
            byte[] encoded = codec.encode(schema, original);

            assertThat(codec.decodeField(schema, encoded, "name")).isEqualTo("Alice");
            assertThat(codec.decodeField(schema, encoded, "score")).isEqualTo(95.5);
            assertThat(codec.decodeField(schema, encoded, "id")).isEqualTo(42L);
        }

        @Test
        @DisplayName("should fallback to _row when no ColumnGroups defined")
        void shouldFallbackToRowWhenNoColumnGroups() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "age", Types.int32())
                            .build();

            Map<String, Object> original = new LinkedHashMap<>();
            original.put("id", 1L);
            original.put("name", "Bob");
            original.put("age", 25);

            FixedOffsetBodyCodec codec = new FixedOffsetBodyCodec();
            byte[] encoded = codec.encode(schema, original);
            Map<String, Object> decoded = codec.decode(schema, encoded);

            assertThat(decoded.get("id")).isEqualTo(1L);
            assertThat(decoded.get("name")).isEqualTo("Bob");
            assertThat(decoded.get("age")).isEqualTo(25);

            assertThat(codec.decodeField(schema, encoded, "name")).isEqualTo("Bob");
        }
    }

    @Nested
    @DisplayName("TlvBodyCodec with ColumnGroup")
    class TlvColumnGroup {

        @Test
        @DisplayName("should handle TLV ColumnGroup encoding")
        void shouldHandleTlvColumnGroupEncoding() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "email", Types.string())
                            .field(4, "age", Types.int32())
                            .field(5, "score", Types.float64())
                            .build()
                            .withColumnGroups(
                                    List.of(
                                            new ColumnGroup(
                                                    "cg_profile",
                                                    List.of("name", "email"),
                                                    List.of(Types.string(), Types.string()),
                                                    ColumnGroup.Encoding.TLV,
                                                    1),
                                            new ColumnGroup(
                                                    "cg_stats",
                                                    List.of("age", "score"),
                                                    List.of(Types.int32(), Types.float64()),
                                                    ColumnGroup.Encoding.TLV,
                                                    1)));

            Map<String, Object> original = createRow();

            TlvBodyCodec codec = new TlvBodyCodec();
            byte[] encoded = codec.encode(schema, original);
            Map<String, Object> decoded = codec.decode(schema, encoded);

            assertThat(decoded.get("id")).isEqualTo(42L);
            assertThat(decoded.get("name")).isEqualTo("Alice");
            assertThat(decoded.get("email")).isEqualTo("alice@example.com");
            assertThat(decoded.get("age")).isEqualTo(30);
            assertThat(decoded.get("score")).isEqualTo(95.5);

            assertThat(codec.decodeField(schema, encoded, "name")).isEqualTo("Alice");
            assertThat(codec.decodeField(schema, encoded, "score")).isEqualTo(95.5);
            assertThat(codec.decodeField(schema, encoded, "id")).isEqualTo(42L);
        }
    }
}
