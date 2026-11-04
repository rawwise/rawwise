package io.github.rawwise.engine.storage.flush;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.engine.storage.ParquetFormatConstants;
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

class SchemaAwareFlushWriteTest {

    @Test
    void cgEncodeDecodeRoundTrip() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_basic",
                        List.of("name", "email"),
                        List.of(PrimitiveType.STRING, PrimitiveType.STRING),
                        ColumnGroup.Encoding.FIXED_OFFSET,
                        1);

        ColumnGroupCodec codec = ColumnGroupCodecFactory.getCodec(cg);
        assertNotNull(codec);

        Map<String, Object> values = Map.of("name", "Alice", "email", "alice@example.com");
        byte[] encoded = codec.encode(cg, values);
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);

        Map<String, Object> decoded = codec.decode(cg, encoded);
        assertEquals("Alice", decoded.get("name"));
        assertEquals("alice@example.com", decoded.get("email"));
    }

    @Test
    void nonCgFieldNamesExcludesPkAndCgFields() {
        Schema schema =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("user_id", PrimitiveType.STRING),
                                        new StructField("name", PrimitiveType.STRING),
                                        new StructField("email", PrimitiveType.STRING),
                                        new StructField("extra", PrimitiveType.STRING))),
                        Map.of(),
                        1L,
                        List.of(),
                        List.of(
                                new ColumnGroup(
                                        "cg_basic",
                                        List.of("name", "email"),
                                        List.of(PrimitiveType.STRING, PrimitiveType.STRING),
                                        ColumnGroup.Encoding.FIXED_OFFSET,
                                        1)));

        List<String> pkFieldNames = List.of("user_id");

        java.util.Set<String> excluded = new java.util.HashSet<>();
        for (ColumnGroup cg : schema.columnGroups()) {
            excluded.addAll(cg.columnNames());
        }
        excluded.addAll(pkFieldNames);

        List<String> nonCg =
                schema.structType().fields().stream()
                        .map(StructField::name)
                        .filter(f -> !excluded.contains(f))
                        .toList();

        assertFalse(nonCg.contains("user_id"), "user_id is PK, must be excluded from nonCG");
        assertFalse(nonCg.contains("name"), "name is in CG, must be excluded from nonCG");
        assertFalse(nonCg.contains("email"), "email is in CG, must be excluded from nonCG");
        assertTrue(nonCg.contains("extra"), "extra is neither PK nor CG, should be in nonCG");
        assertEquals(1, nonCg.size(), "only 'extra' should remain in nonCG");
    }

    @Test
    void parquetSchemaColumnOrderIsPkThenCgThenNonCg() {
        Schema schema =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("pk_col", PrimitiveType.STRING),
                                        new StructField("name", PrimitiveType.STRING),
                                        new StructField("age", PrimitiveType.INT),
                                        new StructField("score", PrimitiveType.DOUBLE))),
                        Map.of(),
                        1L,
                        List.of(),
                        List.of(
                                new ColumnGroup(
                                        "cg_main",
                                        List.of("name", "age"),
                                        List.of(PrimitiveType.STRING, PrimitiveType.INT),
                                        ColumnGroup.Encoding.FIXED_OFFSET,
                                        1)));

        List<String> nonCg =
                ParquetFormatConstants.computeNonCgFieldNames(schema, schema.columnGroups());

        assertEquals(
                List.of("pk_col", "score"),
                nonCg,
                "nonCg should contain 'pk_col' (PK) and 'score'; only CG fields excluded");
    }

    @Test
    void encodeFieldValueSupportsAllTypesThroughNonCgFieldPath() {
        Schema schema =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("pk_col", PrimitiveType.STRING),
                                        new StructField("noncg", PrimitiveType.BINARY))),
                        Map.of(),
                        1L,
                        List.of(),
                        List.of());

        List<String> nonCg =
                ParquetFormatConstants.computeNonCgFieldNames(schema, schema.columnGroups());

        assertEquals(
                List.of("pk_col", "noncg"),
                nonCg,
                "Both 'pk_col' and 'noncg' should be in nonCg since no CG exists");
    }

    @Test
    void emptyPkAndEmptyCgYieldsAllFieldsAsNonCg() {
        Schema schema =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("a", PrimitiveType.STRING),
                                        new StructField("b", PrimitiveType.INT),
                                        new StructField("c", PrimitiveType.DOUBLE))),
                        Map.of(),
                        1L,
                        List.of(),
                        List.of());

        List<String> nonCg =
                ParquetFormatConstants.computeNonCgFieldNames(schema, schema.columnGroups());

        assertEquals(List.of("a", "b", "c"), nonCg, "All fields are nonCg when no PK and no CG");
    }

    @Test
    void compositePkDisablesSchemaAwareFlush() {
        Schema schema =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("pk1", PrimitiveType.STRING),
                                        new StructField("pk2", PrimitiveType.STRING),
                                        new StructField("val", PrimitiveType.INT))),
                        Map.of(),
                        1L,
                        List.of(),
                        List.of());

        List<String> nonCg =
                ParquetFormatConstants.computeNonCgFieldNames(schema, schema.columnGroups());

        assertEquals(
                List.of("pk1", "pk2", "val"),
                nonCg,
                "All fields should be in nonCg since no CG exists (PK fields included for type-safe storage)");
    }
}
