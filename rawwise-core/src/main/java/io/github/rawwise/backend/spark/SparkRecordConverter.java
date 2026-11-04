package io.github.rawwise.backend.spark;

import io.github.rawwise.engine.storage.codec.ValueCodec;
import io.github.rawwise.model.record.DefaultBody;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SparkRecordConverter {
    private static final Logger log = LoggerFactory.getLogger(SparkRecordConverter.class);

    private SparkRecordConverter() {}

    /**
     * Convert a Spark InternalRow to a Record with proper primary key encoding.
     *
     * <p>When the schema has primary keys, the key is encoded using the ValueCodec. When no primary
     * keys are defined, a synthetic UUID key is generated.
     */
    public static Record fromInternalRow(
            InternalRow row,
            StructType sparkSchema,
            Schema rawwiseSchema,
            ValueCodec codec,
            long tableId,
            long sequenceNumber,
            long commitTimestamp,
            Record.Kind kind) {
        Object[] values = new Object[rawwiseSchema.fields().size()];
        for (int i = 0; i < rawwiseSchema.fields().size() && i < sparkSchema.fields().length; i++) {
            StructField nf = rawwiseSchema.fields().get(i);
            if (row.isNullAt(i)) {
                values[i] = null;
            } else {
                values[i] = convertSparkValue(row, i, sparkSchema.fields()[i].dataType());
            }
        }

        Record.Key key;
        List<String> pkFields = rawwiseSchema.primaryKeys();
        if (pkFields.isEmpty()) {
            // No primary key defined: generate synthetic UUID key
            byte[] syntheticKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
            key = Records.keyOf(syntheticKey);
        } else {
            // Encode primary key using the codec
            if (codec == null) {
                throw new IllegalArgumentException(
                        "Schema has primary keys "
                                + pkFields
                                + " but no ValueCodec provided. "
                                + "Use fromInternalRow() with ValueCodec parameter.");
            }
            Map<String, Object> pkValues = new LinkedHashMap<>();
            for (String pk : pkFields) {
                int idx = rawwiseSchema.fieldIndex(pk);
                pkValues.put(pk, idx >= 0 && idx < values.length ? values[idx] : null);
            }
            byte[] encodedKey = codec.encodePrimaryKey(rawwiseSchema, pkValues);
            key = Records.keyOf(encodedKey);
        }

        Record.Body body = new DefaultBody(rawwiseSchema, values);
        return Records.create(key, body, tableId, sequenceNumber, commitTimestamp, kind, true);
    }

    private static Object convertSparkValue(InternalRow row, int ordinal, DataType dataType) {
        if (row.isNullAt(ordinal)) return null;

        try {
            if (dataType instanceof org.apache.spark.sql.types.IntegerType) {
                return row.getInt(ordinal);
            }
            if (dataType instanceof org.apache.spark.sql.types.LongType) {
                return row.getLong(ordinal);
            }
            if (dataType instanceof org.apache.spark.sql.types.FloatType) {
                return row.getFloat(ordinal);
            }
            if (dataType instanceof org.apache.spark.sql.types.DoubleType) {
                return row.getDouble(ordinal);
            }
            if (dataType instanceof org.apache.spark.sql.types.BooleanType) {
                return row.getBoolean(ordinal);
            }
            if (dataType instanceof org.apache.spark.sql.types.StringType) {
                UTF8String utf8 = row.getUTF8String(ordinal);
                return utf8 != null ? utf8.toString() : null;
            }
            if (dataType instanceof org.apache.spark.sql.types.BinaryType) {
                return row.getBinary(ordinal);
            }
            if (dataType instanceof org.apache.spark.sql.types.DateType) {
                return LocalDate.ofEpochDay(row.getInt(ordinal));
            }
            if (dataType instanceof org.apache.spark.sql.types.TimestampType) {
                long micros = row.getLong(ordinal);
                return Instant.ofEpochSecond(
                        TimeUnit.MICROSECONDS.toSeconds(micros), (micros % 1_000_000L) * 1000L);
            }
            if (dataType instanceof org.apache.spark.sql.types.DecimalType) {
                org.apache.spark.sql.types.Decimal decimal = row.getDecimal(ordinal, 38, 18);
                return decimal != null ? decimal.toJavaBigDecimal() : null;
            }
        } catch (Exception e) {
            log.warn(
                    "Spark value conversion failed for ordinal {}, dataType={}",
                    ordinal,
                    dataType.simpleString(),
                    e);
        }
        return row.get(ordinal, dataType);
    }

    public static InternalRow toInternalRow(Record record, StructType sparkSchema) {
        Object[] values = new Object[sparkSchema.fields().length];
        for (int i = 0; i < sparkSchema.fields().length; i++) {
            org.apache.spark.sql.types.StructField sparkField = sparkSchema.fields()[i];
            Record.Value fieldValue = record.body().value(sparkField.name());
            if (fieldValue.isNull()) {
                values[i] = null;
            } else {
                values[i] = convertRawWiseValue(fieldValue.raw(), sparkField.dataType());
            }
        }
        return new GenericInternalRow(values);
    }

    private static Object convertRawWiseValue(Object raw, DataType sparkType) {
        if (sparkType instanceof org.apache.spark.sql.types.ByteType) {
            return (raw instanceof Number n) ? n.byteValue() : Byte.parseByte(raw.toString());
        }
        if (sparkType instanceof org.apache.spark.sql.types.ShortType) {
            return (raw instanceof Number n) ? n.shortValue() : Short.parseShort(raw.toString());
        }
        if (sparkType instanceof org.apache.spark.sql.types.IntegerType) {
            return (raw instanceof Number n) ? n.intValue() : Integer.parseInt(raw.toString());
        }
        if (sparkType instanceof org.apache.spark.sql.types.LongType) {
            return (raw instanceof Number n) ? n.longValue() : Long.parseLong(raw.toString());
        }
        if (sparkType instanceof org.apache.spark.sql.types.FloatType) {
            return (raw instanceof Number n) ? n.floatValue() : Float.parseFloat(raw.toString());
        }
        if (sparkType instanceof org.apache.spark.sql.types.DoubleType) {
            return (raw instanceof Number n) ? n.doubleValue() : Double.parseDouble(raw.toString());
        }
        if (sparkType instanceof org.apache.spark.sql.types.BooleanType) {
            return (raw instanceof Boolean b) ? b : Boolean.parseBoolean(raw.toString());
        }
        if (sparkType instanceof org.apache.spark.sql.types.StringType) {
            if (raw instanceof byte[] bytes) return UTF8String.fromBytes(bytes);
            return UTF8String.fromString(raw.toString());
        }
        if (sparkType instanceof org.apache.spark.sql.types.BinaryType) {
            return (raw instanceof byte[] bytes)
                    ? bytes
                    : raw.toString().getBytes(StandardCharsets.UTF_8);
        }
        if (sparkType instanceof org.apache.spark.sql.types.DateType) {
            if (raw instanceof LocalDate ld) return (int) ld.toEpochDay();
            if (raw instanceof java.sql.Date sd) return (int) sd.toLocalDate().toEpochDay();
            return (raw instanceof Number n) ? n.intValue() : Integer.parseInt(raw.toString());
        }
        if (sparkType instanceof org.apache.spark.sql.types.TimestampType) {
            if (raw instanceof Instant inst) {
                return TimeUnit.SECONDS.toMicros(inst.getEpochSecond()) + inst.getNano() / 1000L;
            }
            if (raw instanceof java.sql.Timestamp ts) {
                return TimeUnit.MILLISECONDS.toMicros(ts.getTime())
                        + (ts.getNanos() % 1_000_000) / 1000L;
            }
            return (raw instanceof Number n) ? n.longValue() : Long.parseLong(raw.toString());
        }
        if (sparkType instanceof org.apache.spark.sql.types.DecimalType dt) {
            BigDecimal bd = (raw instanceof BigDecimal b) ? b : new BigDecimal(raw.toString());
            return org.apache.spark.sql.types.Decimal.apply(bd, dt.precision(), dt.scale());
        }
        if (sparkType instanceof org.apache.spark.sql.types.ArrayType
                || sparkType instanceof org.apache.spark.sql.types.MapType
                || sparkType instanceof org.apache.spark.sql.types.StructType) {
            throw new UnsupportedOperationException("Unsupported complex type: " + sparkType);
        }
        throw new IllegalArgumentException("Unsupported Spark type: " + sparkType);
    }
}
