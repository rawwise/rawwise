package io.github.rawwise.backend.spark;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.engine.storage.codec.DefaultValueCodec;
import io.github.rawwise.engine.storage.codec.ValueCodec;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("SparkRecordConverter - Spark InternalRow Record")
class SparkRecordConverterTest {

    private Schema createTestSchema() {
        return Schema.builder()
                .field("id", Types.int32())
                .field("name", Types.string())
                .field("score", Types.float64())
                .field("active", Types.bool())
                .primaryKey("id")
                .build();
    }

    private StructType createSparkSchema() {
        return new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("score", DataTypes.DoubleType, true)
                .add("active", DataTypes.BooleanType, true);
    }

    private final ValueCodec valueCodec = new DefaultValueCodec(new DefaultKeyCodec());

    @Test
    @DisplayName("fromInternalRow correct type")
    void fromInternalRowBasicTypes() {
        Schema schema = createTestSchema();
        StructType sparkSchema = createSparkSchema();

        InternalRow row =
                new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
                        new Object[] {42, UTF8String.fromString("alice"), 95.5, true});

        Record record =
                SparkRecordConverter.fromInternalRow(
                        row, sparkSchema, schema, valueCodec, 1L, 1L, 100L, Record.Kind.INSERT);

        assertThat(record.body().value("id").raw()).isEqualTo(42);
        assertThat(record.body().value("name").raw()).isEqualTo("alice");
        assertThat(record.body().value("score").raw()).isEqualTo(95.5);
        assertThat(record.body().value("active").raw()).isEqualTo(true);
        assertThat(record.header().tableId()).isEqualTo(1L);
        assertThat(record.header().kind()).isEqualTo(Record.Kind.INSERT);
    }

    @Test
    @DisplayName("fromInternalRow handle null value")
    void fromInternalRowWithNulls() {
        Schema schema = createTestSchema();
        StructType sparkSchema = createSparkSchema();

        InternalRow row =
                new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
                        new Object[] {1, null, null, null});

        Record record =
                SparkRecordConverter.fromInternalRow(
                        row, sparkSchema, schema, valueCodec, 1L, 1L, 100L, Record.Kind.INSERT);

        assertThat(record.body().value("id").raw()).isEqualTo(1);
        assertThat(record.body().value("name").isNull()).isTrue();
        assertThat(record.body().value("score").isNull()).isTrue();
        assertThat(record.body().value("active").isNull()).isTrue();
    }
}
