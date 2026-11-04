package io.github.rawwise.engine.storage.table.key;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Fixed-length order-preserving encoding, supports byte[] lexicographic comparison.
 *
 * <p>Usage: when range-based routing requires encoding PK as comparable byte[], to use {@link
 * io.github.rawwise.engine.storage.KeyRange#contains(byte[])} to determine whether key falls within
 * a shard's range. Encoding rules (each type maintains lexicographic order == logical order after
 * encoding):
 *
 * <ul>
 *   <li>Byte / Short / Integer / Long: big-endian + sign bit flip (XOR 0x80..., maintains signed
 *       lexicographic order)
 *   <li>Boolean: 1 byte(0 or 1)
 *   <li>Float / Double: big-endian IEEE 754 + sign bit flip (maintains lexicographic order)
 *   <li>String: UTF-8 bytes + 0x00 terminator (avoids prefix conflicts)
 *   <li>Composite PK: concatenate encoded fields
 * </ul>
 *
 * <p>Unsupported types throw {@link UnsupportedOperationException}.
 *
 * <p>Does not affect existing {@link DefaultKeyCodec}(both coexist, DefaultKeyCodec for LMDB
 * storage, OrderPreservingKeyEncoder for routing determination).
 */
public final class OrderPreservingKeyEncoder {

    private OrderPreservingKeyEncoder() {}

    public static byte[] encode(Map<String, Comparable<?>> primaryKeyValues) {
        if (primaryKeyValues == null || primaryKeyValues.isEmpty()) {
            throw new IllegalArgumentException("Primary key values cannot be empty");
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            for (Map.Entry<String, Comparable<?>> entry : primaryKeyValues.entrySet()) {
                writeValue(out, entry.getValue());
            }
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Order-preserving key encode failed", e);
        }
    }

    private static void writeValue(ByteArrayOutputStream out, Comparable<?> value) {
        if (value == null) {
            throw new IllegalArgumentException("PK value cannot be null");
        }
        if (value instanceof Integer v) {
            int ordered = v ^ 0x80000000;
            out.write((ordered >>> 24) & 0xFF);
            out.write((ordered >>> 16) & 0xFF);
            out.write((ordered >>> 8) & 0xFF);
            out.write(ordered & 0xFF);
        } else if (value instanceof Long v) {
            long ordered = v ^ 0x8000000000000000L;
            out.write((int) (ordered >>> 56) & 0xFF);
            out.write((int) (ordered >>> 48) & 0xFF);
            out.write((int) (ordered >>> 40) & 0xFF);
            out.write((int) (ordered >>> 32) & 0xFF);
            out.write((int) (ordered >>> 24) & 0xFF);
            out.write((int) (ordered >>> 16) & 0xFF);
            out.write((int) (ordered >>> 8) & 0xFF);
            out.write((int) ordered & 0xFF);
        } else if (value instanceof Short v) {
            int ordered = (v & 0xFFFF) ^ 0x8000;
            out.write((ordered >>> 8) & 0xFF);
            out.write(ordered & 0xFF);
        } else if (value instanceof Byte v) {
            int ordered = (v & 0xFF) ^ 0x80;
            out.write(ordered & 0xFF);
        } else if (value instanceof Boolean v) {
            out.write(v ? 1 : 0);
        } else if (value instanceof Float v) {
            // Float: convert to int bits then flip sign bit (maintains lexicographic order ==
            // numeric order)
            int bits = Float.floatToIntBits(v);
            int ordered = (bits >= 0) ? (bits ^ 0x80000000) : (~bits);
            out.write((ordered >>> 24) & 0xFF);
            out.write((ordered >>> 16) & 0xFF);
            out.write((ordered >>> 8) & 0xFF);
            out.write(ordered & 0xFF);
        } else if (value instanceof Double v) {
            long bits = Double.doubleToLongBits(v);
            long ordered = (bits >= 0) ? (bits ^ 0x8000000000000000L) : (~bits);
            out.write((int) (ordered >>> 56) & 0xFF);
            out.write((int) (ordered >>> 48) & 0xFF);
            out.write((int) (ordered >>> 40) & 0xFF);
            out.write((int) (ordered >>> 32) & 0xFF);
            out.write((int) (ordered >>> 24) & 0xFF);
            out.write((int) (ordered >>> 16) & 0xFF);
            out.write((int) (ordered >>> 8) & 0xFF);
            out.write((int) ordered & 0xFF);
        } else if (value instanceof String v) {
            byte[] bytes = v.getBytes(StandardCharsets.UTF_8);
            out.writeBytes(bytes);
            out.write(0x00); // terminator avoids prefix conflicts
        } else {
            throw new UnsupportedOperationException(
                    "OrderPreservingKeyEncoder does not support: " + value.getClass());
        }
    }
}
