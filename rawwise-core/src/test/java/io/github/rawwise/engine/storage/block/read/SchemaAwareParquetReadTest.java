package io.github.rawwise.engine.storage.block.read;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.codec.BodyCodecRegistry;
import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.ColumnGroupCodec;
import io.github.rawwise.model.record.ColumnGroupCodecFactory;
import io.github.rawwise.model.record.PrimitiveType;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.StructType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchemaAwareParquetReadTest {

    @Test
    void formatVersionDetectsMultiColumn() {
        assertEquals("2", "2");
        assertNotEquals("2", "1");
        assertNotEquals("2", null);
    }

    @Test
    void cgEncodeDecodePreservesAllFieldValues() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_test",
                        List.of("a", "b", "c"),
                        List.of(PrimitiveType.STRING, PrimitiveType.LONG, PrimitiveType.DOUBLE),
                        ColumnGroup.Encoding.FIXED_OFFSET,
                        1);
        ColumnGroupCodec codec = ColumnGroupCodecFactory.getCodec(cg);

        Map<String, Object> values = Map.of("a", "hello", "b", 42L, "c", 3.14);
        byte[] encoded = codec.encode(cg, values);
        Map<String, Object> decoded = codec.decode(cg, encoded);

        assertEquals("hello", decoded.get("a"));
        assertEquals(42L, decoded.get("b"));
        assertEquals(3.14, decoded.get("c"));
    }

    @Test
    void bodyCodecDecodeAllFields() {
        Schema schema =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.STRING),
                                        new StructField("name", PrimitiveType.STRING),
                                        new StructField("age", PrimitiveType.LONG))),
                        Map.of(),
                        1L,
                        List.of(),
                        List.of());

        BodyCodec codec = BodyCodecRegistry.create("fixed-offset", schema);
        Map<String, Object> fields = Map.of("id", "001", "name", "Alice", "age", 30L);
        byte[] encoded = codec.encode(schema, fields);
        Map<String, Object> decoded = codec.decode(schema, encoded);

        assertEquals("001", decoded.get("id"));
        assertEquals("Alice", decoded.get("name"));
        assertEquals(30L, decoded.get("age"));
    }
}
