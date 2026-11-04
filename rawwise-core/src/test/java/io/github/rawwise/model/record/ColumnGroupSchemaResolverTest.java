package io.github.rawwise.model.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ColumnGroupSchemaResolverTest {

    @Test
    void shouldResolveFixedFieldOffsetsWithColumnTypes() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_stats",
                        List.of("age", "login_count", "score"),
                        List.of(PrimitiveType.INT, PrimitiveType.LONG, PrimitiveType.DOUBLE),
                        ColumnGroup.Encoding.FIXED_OFFSET);
        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        Map<String, ColumnGroupSchemaResolver.FieldLayout> layouts = resolver.fieldLayouts();

        int bitmapBytes = (3 + 7) / 8;
        int varFieldCount = 0;
        int varOffsetTableSize = varFieldCount * 4;
        int headerSize = 4 + 4 + bitmapBytes + varOffsetTableSize;

        assertThat(layouts.get("age").offset()).isEqualTo(headerSize);
        assertThat(layouts.get("age").size()).isEqualTo(4);
        assertThat(layouts.get("age").fieldType())
                .isEqualTo(ColumnGroupSchemaResolver.FieldType.INT);
        assertThat(layouts.get("age").isVariable()).isFalse();
        assertThat(layouts.get("age").varFieldIndex()).isEqualTo(-1);

        assertThat(layouts.get("login_count").offset()).isEqualTo(headerSize + 4);
        assertThat(layouts.get("login_count").size()).isEqualTo(8);
        assertThat(layouts.get("login_count").fieldType())
                .isEqualTo(ColumnGroupSchemaResolver.FieldType.LONG);

        assertThat(layouts.get("score").offset()).isEqualTo(headerSize + 4 + 8);
        assertThat(layouts.get("score").size()).isEqualTo(8);
        assertThat(layouts.get("score").fieldType())
                .isEqualTo(ColumnGroupSchemaResolver.FieldType.DOUBLE);
    }

    @Test
    void shouldResolveFixedFieldOffsetsWithStructType() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_stats",
                        List.of("age", "login_count", "score"),
                        ColumnGroup.Encoding.FIXED_OFFSET);
        StructType structType =
                new StructType(
                        List.of(
                                new StructField("age", Types.int32()),
                                new StructField("login_count", Types.int64()),
                                new StructField("score", Types.float64())));
        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg, structType);
        Map<String, ColumnGroupSchemaResolver.FieldLayout> layouts = resolver.fieldLayouts();

        int bitmapBytes = (3 + 7) / 8;
        int varFieldCount = 0;
        int varOffsetTableSize = varFieldCount * 4;
        int headerSize = 4 + 4 + bitmapBytes + varOffsetTableSize;

        assertThat(layouts.get("age").offset()).isEqualTo(headerSize);
        assertThat(layouts.get("age").size()).isEqualTo(4);
        assertThat(layouts.get("age").fieldType())
                .isEqualTo(ColumnGroupSchemaResolver.FieldType.INT);
        assertThat(layouts.get("age").isVariable()).isFalse();

        assertThat(layouts.get("login_count").offset()).isEqualTo(headerSize + 4);
        assertThat(layouts.get("login_count").size()).isEqualTo(8);
        assertThat(layouts.get("login_count").fieldType())
                .isEqualTo(ColumnGroupSchemaResolver.FieldType.LONG);

        assertThat(layouts.get("score").offset()).isEqualTo(headerSize + 4 + 8);
        assertThat(layouts.get("score").size()).isEqualTo(8);
        assertThat(layouts.get("score").fieldType())
                .isEqualTo(ColumnGroupSchemaResolver.FieldType.DOUBLE);
    }

    @Test
    void shouldReportRecordSizeForAllFixedFields() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_stats",
                        List.of("age", "login_count", "score"),
                        List.of(PrimitiveType.INT, PrimitiveType.LONG, PrimitiveType.DOUBLE),
                        ColumnGroup.Encoding.FIXED_OFFSET);
        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        assertThat(resolver.fixedRecordSize()).isPresent();
        int bitmapBytes = (3 + 7) / 8;
        int headerSize = 4 + 4 + bitmapBytes;
        assertThat(resolver.fixedRecordSize().get()).isEqualTo(headerSize + 4 + 8 + 8);
    }

    @Test
    void shouldHandleVariableFieldsAfterFixedFields() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_mixed",
                        List.of("id", "name", "salary"),
                        List.of(PrimitiveType.INT, PrimitiveType.STRING, PrimitiveType.DOUBLE),
                        ColumnGroup.Encoding.FIXED_OFFSET);
        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        Map<String, ColumnGroupSchemaResolver.FieldLayout> layouts = resolver.fieldLayouts();

        int bitmapBytes = (3 + 7) / 8;
        int varFieldCount = 1;
        int varOffsetTableSize = varFieldCount * 4;
        int headerSize = 4 + 4 + bitmapBytes + varOffsetTableSize;

        assertThat(layouts.get("id").offset()).isEqualTo(headerSize);
        assertThat(layouts.get("id").size()).isEqualTo(4);
        assertThat(layouts.get("id").isVariable()).isFalse();

        assertThat(layouts.get("salary").offset()).isEqualTo(headerSize + 4);
        assertThat(layouts.get("salary").size()).isEqualTo(8);
        assertThat(layouts.get("salary").isVariable()).isFalse();

        assertThat(layouts.get("name").offset()).isEqualTo(headerSize + 4 + 8);
        assertThat(layouts.get("name").size()).isEqualTo(-1);
        assertThat(layouts.get("name").isVariable()).isTrue();
        assertThat(layouts.get("name").fieldType())
                .isEqualTo(ColumnGroupSchemaResolver.FieldType.STRING);
        assertThat(layouts.get("name").varFieldIndex()).isEqualTo(0);

        assertThat(resolver.fixedRecordSize()).isEmpty();
        assertThat(resolver.varFieldCount()).isEqualTo(1);
    }

    @Test
    void shouldThrowForTlvEncoding() {
        ColumnGroup cg = new ColumnGroup("cg_tlv", List.of("a", "b"), ColumnGroup.Encoding.TLV);
        assertThatThrownBy(() -> ColumnGroupSchemaResolver.resolve(cg))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("FIXED_OFFSET");
    }

    @Test
    void shouldThrowForEmptyColumnTypes() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_no_types", List.of("a", "b"), ColumnGroup.Encoding.FIXED_OFFSET);
        assertThatThrownBy(() -> ColumnGroupSchemaResolver.resolve(cg))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("columnTypes");
    }

    @Test
    void shouldMapFloat32Correctly() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_float",
                        List.of("f"),
                        List.of(PrimitiveType.FLOAT),
                        ColumnGroup.Encoding.FIXED_OFFSET);
        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        ColumnGroupSchemaResolver.FieldLayout layout = resolver.fieldLayouts().get("f");

        assertThat(layout.size()).isEqualTo(4);
        assertThat(layout.fieldType()).isEqualTo(ColumnGroupSchemaResolver.FieldType.FLOAT);
        assertThat(layout.isVariable()).isFalse();
    }

    @Test
    void shouldMapBinaryTypeAsVariable() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_bin",
                        List.of("data"),
                        List.of(PrimitiveType.BINARY),
                        ColumnGroup.Encoding.FIXED_OFFSET);
        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        ColumnGroupSchemaResolver.FieldLayout layout = resolver.fieldLayouts().get("data");

        assertThat(layout.size()).isEqualTo(-1);
        assertThat(layout.fieldType()).isEqualTo(ColumnGroupSchemaResolver.FieldType.BYTES);
        assertThat(layout.isVariable()).isTrue();
        assertThat(layout.varFieldIndex()).isEqualTo(0);
    }

    @Test
    void shouldReturnImmutableFieldLayouts() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_stats",
                        List.of("age"),
                        List.of(PrimitiveType.INT),
                        ColumnGroup.Encoding.FIXED_OFFSET);
        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);

        assertThatThrownBy(() -> resolver.fieldLayouts().put("hack", null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldCalculateVarFieldIndicesCorrectly() {
        ColumnGroup cg =
                new ColumnGroup(
                        "cg_multi_var",
                        List.of("id", "name", "age", "email", "score"),
                        List.of(
                                PrimitiveType.INT,
                                PrimitiveType.STRING,
                                PrimitiveType.INT,
                                PrimitiveType.STRING,
                                PrimitiveType.DOUBLE),
                        ColumnGroup.Encoding.FIXED_OFFSET);
        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        Map<String, ColumnGroupSchemaResolver.FieldLayout> layouts = resolver.fieldLayouts();

        assertThat(layouts.get("id").isVariable()).isFalse();
        assertThat(layouts.get("id").varFieldIndex()).isEqualTo(-1);

        assertThat(layouts.get("name").isVariable()).isTrue();
        assertThat(layouts.get("name").varFieldIndex()).isEqualTo(0);

        assertThat(layouts.get("age").isVariable()).isFalse();
        assertThat(layouts.get("age").varFieldIndex()).isEqualTo(-1);

        assertThat(layouts.get("email").isVariable()).isTrue();
        assertThat(layouts.get("email").varFieldIndex()).isEqualTo(1);

        assertThat(layouts.get("score").isVariable()).isFalse();
        assertThat(layouts.get("score").varFieldIndex()).isEqualTo(-1);

        assertThat(resolver.varFieldCount()).isEqualTo(2);
    }
}
