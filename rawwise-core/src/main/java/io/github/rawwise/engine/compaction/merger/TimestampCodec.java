package io.github.rawwise.engine.compaction.merger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class TimestampCodec {

    private TimestampCodec() {}

    public static long deserializeToMillis(byte[] bytes, String logicalType) {
        if (bytes == null || bytes.length == 0) return Long.MIN_VALUE;
        if (logicalType == null) return Long.MIN_VALUE;

        return switch (logicalType) {
            case "TIMESTAMP_MILLIS" -> {
                if (bytes.length < 8) yield Long.MIN_VALUE;
                yield ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
            }
            case "TIMESTAMP_MICROS" -> {
                if (bytes.length < 8) yield Long.MIN_VALUE;
                yield ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong() / 1000;
            }
            case "TIMESTAMP_NANOS" -> {
                if (bytes.length < 8) yield Long.MIN_VALUE;
                yield ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong() / 1_000_000;
            }
            case "DATE" -> {
                if (bytes.length < 4) yield Long.MIN_VALUE;
                yield (long) ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt()
                        * 86400000L;
            }
            default -> Long.MIN_VALUE;
        };
    }

    public static boolean isTimestampType(String logicalType) {
        if (logicalType == null) return false;
        return logicalType.equals("TIMESTAMP_MILLIS")
                || logicalType.equals("TIMESTAMP_MICROS")
                || logicalType.equals("TIMESTAMP_NANOS")
                || logicalType.equals("DATE");
    }
}
