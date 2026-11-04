package io.github.rawwise.engine.storage.codec;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

public final class NullableValueCodec {
    private static final byte NULL_TAG = 0;
    private static final byte VALUE_TAG = 1;
    private static final byte TOMBSTONE_TAG = 2;
    private static final int TOMBSTONE_ENCODED_LENGTH = 9;

    private NullableValueCodec() {}

    public static byte[] encode(byte[] value) {
        if (value == null) {
            return new byte[] {NULL_TAG};
        }
        byte[] out = new byte[value.length + 1];
        out[0] = VALUE_TAG;
        System.arraycopy(value, 0, out, 1, value.length);
        return out;
    }

    public static byte[] encodeTombstone(long commitTimestamp) {
        ByteBuffer buf = ByteBuffer.allocate(TOMBSTONE_ENCODED_LENGTH);
        buf.put(TOMBSTONE_TAG);
        buf.putLong(commitTimestamp);
        return buf.array();
    }

    public static boolean isTombstone(byte[] stored) {
        return stored != null
                && stored.length >= TOMBSTONE_ENCODED_LENGTH
                && stored[0] == TOMBSTONE_TAG;
    }

    public static long decodeTombstoneTimestamp(byte[] stored) {
        if (stored == null
                || stored.length < TOMBSTONE_ENCODED_LENGTH
                || stored[0] != TOMBSTONE_TAG) {
            throw new IllegalArgumentException("Not a valid tombstone encoding");
        }
        ByteBuffer buf = ByteBuffer.wrap(stored, 1, 8);
        return buf.getLong();
    }

    public static byte[] decode(byte[] stored) {
        if (stored == null) {
            return null;
        }
        if (stored.length == 0) {
            return stored;
        }
        byte tag = stored[0];
        if (tag == NULL_TAG && stored.length == 1) {
            return null;
        }
        if (tag == TOMBSTONE_TAG) {
            throw new IllegalStateException(
                    "Attempted to decode a tombstone as a value. Use isTombstone() to check first.");
        }
        if (tag == VALUE_TAG) {
            return Arrays.copyOfRange(stored, 1, stored.length);
        }
        return stored;
    }

    public static byte[] encodeUpdateValues(Map<String, Object> updates) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(updates.size());
        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            dos.writeInt(keyBytes.length);
            dos.write(keyBytes);
            if (entry.getValue() == null) {
                dos.writeByte(NULL_TAG);
            } else {
                byte[] valBytes = entry.getValue().toString().getBytes(StandardCharsets.UTF_8);
                dos.writeByte(VALUE_TAG);
                dos.writeInt(valBytes.length);
                dos.write(valBytes);
            }
        }
        return encode(baos.toByteArray());
    }
}
