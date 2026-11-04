package io.github.rawwise.backend.wal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

public final class WALRecordCodec {

    public static final byte RECORD_TYPE_DATA = 1;
    public static final byte RECORD_TYPE_SYNC_MARKER = 2;
    public static final byte RECORD_TYPE_TXN_BEGIN = 3;
    public static final byte RECORD_TYPE_TXN_DATA = 4;
    public static final byte RECORD_TYPE_TXN_PREPARED = 5;
    public static final byte RECORD_TYPE_TXN_COMMIT = 6;
    public static final byte RECORD_TYPE_TXN_ABORT = 7;
    public static final byte RECORD_TYPE_FASTPATH_WRITE = 8;
    public static final byte RECORD_TYPE_FASTPATH_COMMIT = 9;

    private WALRecordCodec() {}

    public static byte[] encode(String tableId, byte[] key, byte[] value) {
        byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
        int keyLen = key != null ? key.length : 0;
        int valueLen = value != null ? value.length : 0;
        int total = 4 + tableIdBytes.length + 4 + keyLen + 4 + valueLen;

        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);
        buffer.putInt(keyLen);
        if (key != null) {
            buffer.put(key);
        }
        buffer.putInt(valueLen);
        if (value != null) {
            buffer.put(value);
        }
        return buffer.array();
    }

    public static byte[] serializeEnvelope(WALRecord entry) {
        byte recordType =
                "SYNC_MARKER".equals(entry.marker()) ? RECORD_TYPE_SYNC_MARKER : RECORD_TYPE_DATA;
        byte[] tableIdBytes =
                entry.uniqTableIdentifier() != null
                        ? entry.uniqTableIdentifier().getBytes(StandardCharsets.UTF_8)
                        : new byte[0];
        byte[] dataBytes = entry.value() != null ? entry.value() : new byte[0];

        int payloadSize = 1 + 8 + 8 + 4 + tableIdBytes.length + 8 + 4 + dataBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(payloadSize);
        buffer.put(recordType);
        buffer.putLong(entry.sequence());
        buffer.putLong(entry.timestamp());
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);
        buffer.putLong(entry.commitId());
        buffer.putInt(dataBytes.length);
        buffer.put(dataBytes);

        return buffer.array();
    }

    public static byte[] serializeRecord(WALRecord entry) {
        byte recordType =
                "SYNC_MARKER".equals(entry.marker()) ? RECORD_TYPE_SYNC_MARKER : RECORD_TYPE_DATA;
        byte[] tableIdBytes =
                entry.uniqTableIdentifier() != null
                        ? entry.uniqTableIdentifier().getBytes(StandardCharsets.UTF_8)
                        : new byte[0];
        byte[] dataBytes = entry.value() != null ? entry.value() : new byte[0];

        int payloadSize = 1 + 8 + 8 + 4 + tableIdBytes.length + 8 + 4 + dataBytes.length;
        int totalSize = 4 + payloadSize + 4;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(payloadSize);
        buffer.put(recordType);
        buffer.putLong(entry.sequence());
        buffer.putLong(entry.timestamp());
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);
        buffer.putLong(entry.commitId());
        buffer.putInt(dataBytes.length);
        buffer.put(dataBytes);

        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 4, payloadSize);
        buffer.putInt((int) crc32.getValue());

        return buffer.array();
    }

    public static WALRecord deserializeEnvelope(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);

        byte recordType = buffer.get();
        long sequence = buffer.getLong();
        long timestamp = buffer.getLong();

        int tableIdLength = buffer.getInt();
        if (tableIdLength < 0 || tableIdLength > buffer.remaining()) {
            throw new WALRecoveryException(
                    String.format(
                            "Invalid tableId length: %d, remaining: %d in record seq=%d",
                            tableIdLength, buffer.remaining(), sequence));
        }
        byte[] tableIdBytes = new byte[tableIdLength];
        buffer.get(tableIdBytes);
        String tableId = new String(tableIdBytes, StandardCharsets.UTF_8);

        long commitId = buffer.getLong();

        int dataLength = buffer.getInt();
        if (dataLength < 0 || dataLength > buffer.remaining()) {
            throw new WALRecoveryException(
                    String.format(
                            "Invalid data length: %d, remaining: %d in record seq=%d",
                            dataLength, buffer.remaining(), sequence));
        }
        byte[] data = new byte[dataLength];
        buffer.get(data);

        String marker = (recordType == RECORD_TYPE_SYNC_MARKER) ? "SYNC_MARKER" : "DATA";

        return new DefaultWALRecord(sequence, data, timestamp, commitId, tableId, marker);
    }

    public static String extractTableId(byte[] data) {
        if (data == null || data.length < 4) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int tableIdLen = buffer.getInt();
        if (tableIdLen < 0 || tableIdLen > data.length - 4) {
            return null;
        }
        byte[] tableIdBytes = new byte[tableIdLen];
        buffer.get(tableIdBytes);
        return new String(tableIdBytes, StandardCharsets.UTF_8);
    }

    public static byte[] extractKey(byte[] data) {
        if (data == null || data.length < 8) {
            return new byte[0];
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);

        int tableIdLen = buffer.getInt();
        if (tableIdLen < 0 || tableIdLen > data.length - 4) {
            return new byte[0];
        }
        buffer.position(buffer.position() + tableIdLen);

        if (buffer.remaining() < 4) {
            return new byte[0];
        }
        int keyLength = buffer.getInt();
        if (keyLength < 0 || keyLength > buffer.remaining()) {
            return new byte[0];
        }
        byte[] key = new byte[keyLength];
        buffer.get(key);
        return key;
    }

    public static byte[] extractValue(byte[] data) {
        if (data == null || data.length < 8) {
            return new byte[0];
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);

        int tableIdLen = buffer.getInt();
        if (tableIdLen < 0 || tableIdLen > data.length - 4) {
            return new byte[0];
        }
        buffer.position(buffer.position() + tableIdLen);

        if (buffer.remaining() < 4) {
            return new byte[0];
        }
        int keyLen = buffer.getInt();
        if (keyLen < 0 || keyLen > buffer.remaining() - 4) {
            return new byte[0];
        }
        buffer.position(buffer.position() + keyLen);

        if (buffer.remaining() < 4) {
            return new byte[0];
        }
        int valueLen = buffer.getInt();
        if (valueLen < 0 || valueLen > buffer.remaining()) {
            return new byte[0];
        }
        byte[] value = new byte[valueLen];
        buffer.get(value);
        return value;
    }
}
