package io.github.rawwise.engine.storage.table.key;

import io.github.rawwise.model.record.TypeId;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DefaultKeyCodec implements KeyCodec {

    @Override
    public byte[] encodePrimaryKey(Map<String, Comparable> primaryKeyValues) throws IOException {
        Objects.requireNonNull(primaryKeyValues, "primaryKeyValues cannot be null");
        if (primaryKeyValues.isEmpty()) {
            throw new IllegalArgumentException("Primary key values cannot be empty");
        }
        List<Map.Entry<String, Comparable>> entries = List.copyOf(primaryKeyValues.entrySet());
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream data = new DataOutputStream(out)) {
            data.writeInt(entries.size());
            for (Map.Entry<String, Comparable> entry : entries) {
                data.writeUTF(entry.getKey());
                writeValue(data, entry.getValue());
            }
            data.flush();
            return out.toByteArray();
        }
    }

    private void writeValue(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(TypeId.TYPE_NULL);
            out.writeInt(0);
            return;
        }
        if (value instanceof byte[] bytes) {
            out.writeByte(TypeId.TYPE_BINARY);
            out.writeInt(bytes.length);
            out.write(bytes);
            return;
        }
        if (value instanceof String stringValue) {
            writeUtf8(out, TypeId.TYPE_STRING, stringValue);
            return;
        }
        if (value instanceof Integer intValue) {
            out.writeByte(TypeId.TYPE_INT);
            out.writeInt(Integer.BYTES);
            out.writeInt(intValue);
            return;
        }
        if (value instanceof Long longValue) {
            out.writeByte(TypeId.TYPE_LONG);
            out.writeInt(Long.BYTES);
            out.writeLong(longValue);
            return;
        }
        if (value instanceof Float floatValue) {
            out.writeByte(TypeId.TYPE_FLOAT);
            out.writeInt(Float.BYTES);
            out.writeFloat(floatValue);
            return;
        }
        if (value instanceof Double doubleValue) {
            out.writeByte(TypeId.TYPE_DOUBLE);
            out.writeInt(Double.BYTES);
            out.writeDouble(doubleValue);
            return;
        }
        if (value instanceof Boolean booleanValue) {
            out.writeByte(TypeId.TYPE_BOOL);
            out.writeInt(1);
            out.writeBoolean(booleanValue);
            return;
        }
        if (value instanceof BigDecimal decimalValue) {
            writeUtf8(out, TypeId.TYPE_DECIMAL, decimalValue.toPlainString());
            return;
        }
        if (value instanceof LocalDate localDate) {
            writeUtf8(out, TypeId.TYPE_DATE, localDate.toString());
            return;
        }
        if (value instanceof Instant instant) {
            writeUtf8(out, TypeId.TYPE_TIMESTAMP, instant.toString());
            return;
        }
        writeUtf8(out, TypeId.TYPE_FALLBACK, value.toString());
    }

    private void writeUtf8(DataOutputStream out, byte type, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeByte(type);
        out.writeInt(bytes.length);
        out.write(bytes);
    }
}
