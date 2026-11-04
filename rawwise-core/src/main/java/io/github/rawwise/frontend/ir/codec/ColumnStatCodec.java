package io.github.rawwise.frontend.ir.codec;

import io.github.rawwise.engine.compaction.merger.TimestampCodec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public final class ColumnStatCodec {

    private ColumnStatCodec() {}

    @SuppressWarnings("unchecked")
    public static Comparable<?> decode(byte[] bytes, String logicalType) {
        if (bytes == null || bytes.length == 0) return null;

        if (TimestampCodec.isTimestampType(logicalType)) {
            long millis = TimestampCodec.deserializeToMillis(bytes, logicalType);
            if (millis == Long.MIN_VALUE) return null;
            return millis;
        }

        return switch (upperSnake(logicalType)) {
            case "INT32", "INT_32" -> {
                if (bytes.length < 4) yield null;
                yield ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            }
            case "INT64", "INT_64", "LONG" -> {
                if (bytes.length < 8) yield null;
                yield ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
            }
            case "FLOAT", "FLOAT32" -> {
                if (bytes.length < 4) yield null;
                yield ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
            }
            case "DOUBLE", "FLOAT64" -> {
                if (bytes.length < 8) yield null;
                yield ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
            }
            case "STRING", "UTF8" -> new String(bytes, StandardCharsets.UTF_8);
            case "BOOLEAN", "BOOL" -> {
                if (bytes.length < 1) yield null;
                yield bytes[0] != 0;
            }
            case "DECIMAL" -> {
                try {
                    yield new java.math.BigDecimal(new java.math.BigInteger(bytes));
                } catch (Exception e) {
                    yield null;
                }
            }
            default -> null;
        };
    }

    private static String upperSnake(String s) {
        return s == null ? "" : s.toUpperCase().replace(' ', '_');
    }
}
