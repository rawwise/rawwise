package io.github.rawwise.frontend.ir.routing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class RoutingKeys {

    private static final String ROUTING_PREFIX = "routing:";
    private static final String TABLE_PREFIX = "table:";
    private static final String ENTRY_PREFIX = ":entry:";

    private RoutingKeys() {}

    public static byte[] versionKey(String tableId) {
        return (ROUTING_PREFIX + TABLE_PREFIX + tableId + ":version")
                .getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] defaultKey(String tableId) {
        return (ROUTING_PREFIX + TABLE_PREFIX + tableId + ":default")
                .getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] entryKey(String tableId, int seq) {
        return (ROUTING_PREFIX + TABLE_PREFIX + tableId + ENTRY_PREFIX + seq)
                .getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] entryScanPrefix(String tableId) {
        return (ROUTING_PREFIX + TABLE_PREFIX + tableId + ENTRY_PREFIX)
                .getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    public static long bytesToLong(byte[] b) {
        return ByteBuffer.wrap(b).getLong();
    }

    public static byte[] intToBytes(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    public static int bytesToInt(byte[] b) {
        return ByteBuffer.wrap(b).getInt();
    }

    public static long bytesToLong(byte[] b, int offset) {
        return ByteBuffer.wrap(b, offset, 8).getLong();
    }

    public static int bytesToInt(byte[] b, int offset) {
        return ByteBuffer.wrap(b, offset, 4).getInt();
    }
}
