package io.github.rawwise.backend.wal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class TxnWALRecordCodec {

    private static final int NULL_LENGTH = -1;
    static final byte V2_MARKER = (byte) 0x80;

    private TxnWALRecordCodec() {}

    public static byte[] encode(TxnWALRecord record) {
        return switch (record.type()) {
            case TXN_BEGIN -> encodeBegin(record);
            case TXN_DATA -> encodeData(record);
            case TXN_PREPARED -> encodePrepared(record);
            case TXN_COMMIT -> encodeCommit(record);
            case TXN_ABORT -> encodeAbort(record);
            case FASTPATH_WRITE -> encodeFastpathWrite(record);
            case FASTPATH_COMMIT -> encodeFastpathCommit(record);
        };
    }

    public static TxnWALRecord decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte typeByte = buffer.get();
        if ((typeByte & V2_MARKER) != 0) {
            byte baseType = (byte) (typeByte & ~V2_MARKER);
            return switch (baseType) {
                case WALRecordCodec.RECORD_TYPE_TXN_DATA -> decodeV2Data(buffer);
                default ->
                        throw new WALRecoveryException(
                                String.format("Unknown v2 txn record type: %d", baseType));
            };
        }
        return switch (typeByte) {
            case WALRecordCodec.RECORD_TYPE_TXN_BEGIN -> decodeBegin(buffer);
            case WALRecordCodec.RECORD_TYPE_TXN_DATA -> decodeData(buffer);
            case WALRecordCodec.RECORD_TYPE_TXN_PREPARED -> decodePrepared(buffer);
            case WALRecordCodec.RECORD_TYPE_TXN_COMMIT -> decodeCommit(buffer);
            case WALRecordCodec.RECORD_TYPE_TXN_ABORT -> decodeAbort(buffer);
            case WALRecordCodec.RECORD_TYPE_FASTPATH_WRITE -> decodeFastpathWrite(buffer);
            case WALRecordCodec.RECORD_TYPE_FASTPATH_COMMIT -> decodeFastpathCommit(buffer);
            default ->
                    throw new WALRecoveryException(
                            String.format("Unknown txn record type: %d", typeByte));
        };
    }

    private static byte[] encodeBegin(TxnWALRecord record) {
        byte[] isolationLevelBytes = record.isolationLevel().getBytes(StandardCharsets.UTF_8);

        List<byte[]> tableIdBytesList = new ArrayList<>();
        int tableIdsTotalLen = 0;
        for (String tid : record.tableIds()) {
            byte[] tidBytes = tid.getBytes(StandardCharsets.UTF_8);
            tableIdBytesList.add(tidBytes);
            tableIdsTotalLen += 4 + tidBytes.length;
        }

        int total = 1 + 8 + 4 + tableIdsTotalLen + 4 + isolationLevelBytes.length + 8 + 8;

        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.put(WALRecordCodec.RECORD_TYPE_TXN_BEGIN);
        buffer.putLong(record.txnId());
        buffer.putInt(record.tableIds().size());
        for (byte[] tidBytes : tableIdBytesList) {
            buffer.putInt(tidBytes.length);
            buffer.put(tidBytes);
        }
        buffer.putInt(isolationLevelBytes.length);
        buffer.put(isolationLevelBytes);
        buffer.putLong(record.beginTs());
        buffer.putLong(record.baseSnapshotId());

        return buffer.array();
    }

    private static byte[] encodeData(TxnWALRecord record) {
        byte[] tableIdBytes = record.tableId().getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = record.key();
        byte[] valueBytes = record.value();
        byte[] writeTypeBytes = record.writeType().getBytes(StandardCharsets.UTF_8);

        int keyLen = keyBytes != null ? keyBytes.length : 0;
        int valueLen = valueBytes != null ? valueBytes.length : 0;
        boolean valueIsNull = valueBytes == null;

        int total =
                1
                        + 8
                        + 4
                        + tableIdBytes.length
                        + 4
                        + keyLen
                        + (valueIsNull ? 4 : 4 + valueLen)
                        + 4
                        + writeTypeBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.put(WALRecordCodec.RECORD_TYPE_TXN_DATA);
        buffer.putLong(record.txnId());
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);
        buffer.putInt(keyLen);
        if (keyBytes != null) {
            buffer.put(keyBytes);
        }
        if (valueIsNull) {
            buffer.putInt(NULL_LENGTH);
        } else {
            buffer.putInt(valueLen);
            buffer.put(valueBytes);
        }
        buffer.putInt(writeTypeBytes.length);
        buffer.put(writeTypeBytes);

        return buffer.array();
    }

    private static byte[] encodePrepared(TxnWALRecord record) {
        byte[] tableIdBytes = record.tableId().getBytes(StandardCharsets.UTF_8);

        int total = 1 + 8 + 4 + tableIdBytes.length + 8;

        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.put(WALRecordCodec.RECORD_TYPE_TXN_PREPARED);
        buffer.putLong(record.txnId());
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);
        buffer.putLong(record.commitTs());

        return buffer.array();
    }

    private static byte[] encodeCommit(TxnWALRecord record) {
        byte[] tableIdBytes = record.tableId().getBytes(StandardCharsets.UTF_8);

        int total = 1 + 8 + 4 + tableIdBytes.length + 8 + 8;

        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.put(WALRecordCodec.RECORD_TYPE_TXN_COMMIT);
        buffer.putLong(record.txnId());
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);
        buffer.putLong(record.commitTs());
        buffer.putLong(record.newSnapshotId());

        return buffer.array();
    }

    private static byte[] encodeAbort(TxnWALRecord record) {
        byte[] tableIdBytes = record.tableId().getBytes(StandardCharsets.UTF_8);

        int total = 1 + 8 + 4 + tableIdBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.put(WALRecordCodec.RECORD_TYPE_TXN_ABORT);
        buffer.putLong(record.txnId());
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);

        return buffer.array();
    }

    private static TxnWALRecord decodeBegin(ByteBuffer buffer) {
        long txnId = buffer.getLong();
        int tableIdCount = buffer.getInt();
        List<String> tableIds = new ArrayList<>(tableIdCount);
        for (int i = 0; i < tableIdCount; i++) {
            tableIds.add(readString(buffer));
        }
        String isolationLevel = readString(buffer);
        long beginTs = buffer.getLong();
        long baseSnapshotId = buffer.getLong();
        return TxnWALRecord.begin(txnId, tableIds, isolationLevel, beginTs, baseSnapshotId);
    }

    private static TxnWALRecord decodeData(ByteBuffer buffer) {
        long txnId = buffer.getLong();
        String tableId = readString(buffer);
        int keyLen = buffer.getInt();
        if (keyLen < 0 || keyLen > buffer.remaining()) {
            throw new WALRecoveryException(
                    String.format(
                            "Invalid key length: %d, remaining: %d", keyLen, buffer.remaining()));
        }
        byte[] key = new byte[keyLen];
        buffer.get(key);
        int valueLen = buffer.getInt();
        byte[] value;
        if (valueLen == NULL_LENGTH) {
            value = null;
        } else {
            if (valueLen < 0 || valueLen > buffer.remaining()) {
                throw new WALRecoveryException(
                        String.format(
                                "Invalid value length: %d, remaining: %d",
                                valueLen, buffer.remaining()));
            }
            value = new byte[valueLen];
            buffer.get(value);
        }
        String writeType = readString(buffer);
        return TxnWALRecord.data(txnId, tableId, key, value, writeType);
    }

    private static TxnWALRecord decodePrepared(ByteBuffer buffer) {
        long txnId = buffer.getLong();
        String tableId = readString(buffer);
        long commitTs = buffer.getLong();
        return TxnWALRecord.prepared(txnId, tableId, commitTs);
    }

    private static TxnWALRecord decodeCommit(ByteBuffer buffer) {
        long txnId = buffer.getLong();
        String tableId = readString(buffer);
        long commitTs = buffer.getLong();
        long newSnapshotId = buffer.getLong();
        return TxnWALRecord.commit(txnId, tableId, commitTs, newSnapshotId);
    }

    private static TxnWALRecord decodeAbort(ByteBuffer buffer) {
        long txnId = buffer.getLong();
        String tableId = readString(buffer);
        return TxnWALRecord.abort(txnId, tableId);
    }

    private static String readString(ByteBuffer buffer) {
        int len = buffer.getInt();
        if (len < 0 || len > buffer.remaining()) {
            throw new WALRecoveryException(
                    String.format(
                            "Invalid string length: %d, remaining: %d", len, buffer.remaining()));
        }
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] encodeV2Data(
            long txnId,
            long numericTableId,
            long schemaVersion,
            byte[] key,
            byte[] value,
            String writeType) {
        byte[] writeTypeBytes = writeType.getBytes(StandardCharsets.UTF_8);
        int keyLen = key != null ? key.length : 0;
        int valueLen = value != null ? value.length : 0;
        boolean valueIsNull = value == null;

        int total =
                1
                        + 8
                        + 8
                        + 8
                        + 4
                        + keyLen
                        + (valueIsNull ? 4 : 4 + valueLen)
                        + 4
                        + writeTypeBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.put((byte) (WALRecordCodec.RECORD_TYPE_TXN_DATA | V2_MARKER));
        buffer.putLong(txnId);
        buffer.putLong(numericTableId);
        buffer.putLong(schemaVersion);
        buffer.putInt(keyLen);
        if (key != null) buffer.put(key);
        if (valueIsNull) {
            buffer.putInt(NULL_LENGTH);
        } else {
            buffer.putInt(valueLen);
            buffer.put(value);
        }
        buffer.putInt(writeTypeBytes.length);
        buffer.put(writeTypeBytes);

        return buffer.array();
    }

    public static boolean isV2Format(byte[] data) {
        return data != null && data.length > 0 && (data[0] & V2_MARKER) != 0;
    }

    public static TxnWALRecord decodeV2Data(ByteBuffer buffer) {
        long txnId = buffer.getLong();
        long numericTableId = buffer.getLong();
        long schemaVersion = buffer.getLong();
        int keyLen = buffer.getInt();
        if (keyLen < 0 || keyLen > buffer.remaining()) {
            throw new WALRecoveryException(
                    String.format(
                            "Invalid key length: %d, remaining: %d", keyLen, buffer.remaining()));
        }
        byte[] key = new byte[keyLen];
        buffer.get(key);
        int valueLen = buffer.getInt();
        byte[] value;
        if (valueLen == NULL_LENGTH) {
            value = null;
        } else {
            if (valueLen < 0 || valueLen > buffer.remaining()) {
                throw new WALRecoveryException(
                        String.format(
                                "Invalid value length: %d, remaining: %d",
                                valueLen, buffer.remaining()));
            }
            value = new byte[valueLen];
            buffer.get(value);
        }
        String writeType = readString(buffer);
        return TxnWALRecord.data(txnId, String.valueOf(numericTableId), key, value, writeType);
    }

    private static byte[] encodeFastpathWrite(TxnWALRecord record) {
        byte[] tableIdBytes = record.tableId().getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = record.key();
        byte[] valueBytes = record.value();
        byte[] writeTypeBytes = record.writeType().getBytes(StandardCharsets.UTF_8);

        int keyLen = keyBytes != null ? keyBytes.length : 0;
        int valueLen = valueBytes != null ? valueBytes.length : 0;
        boolean valueIsNull = valueBytes == null;

        int total =
                1
                        + 8
                        + 4
                        + tableIdBytes.length
                        + 4
                        + keyLen
                        + (valueIsNull ? 4 : 4 + valueLen)
                        + 4
                        + writeTypeBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.put(WALRecordCodec.RECORD_TYPE_FASTPATH_WRITE);
        buffer.putLong(record.txnId());
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);
        buffer.putInt(keyLen);
        if (keyBytes != null) {
            buffer.put(keyBytes);
        }
        if (valueIsNull) {
            buffer.putInt(NULL_LENGTH);
        } else {
            buffer.putInt(valueLen);
            buffer.put(valueBytes);
        }
        buffer.putInt(writeTypeBytes.length);
        buffer.put(writeTypeBytes);

        return buffer.array();
    }

    private static byte[] encodeFastpathCommit(TxnWALRecord record) {
        byte[] tableIdBytes =
                record.tableId() != null
                        ? record.tableId().getBytes(StandardCharsets.UTF_8)
                        : new byte[0];

        int total = 1 + 8 + 4 + tableIdBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.put(WALRecordCodec.RECORD_TYPE_FASTPATH_COMMIT);
        buffer.putLong(record.txnId());
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);

        return buffer.array();
    }

    private static TxnWALRecord decodeFastpathWrite(ByteBuffer buffer) {
        long commitTs = buffer.getLong();
        String tableId = readString(buffer);
        int keyLen = buffer.getInt();
        if (keyLen < 0 || keyLen > buffer.remaining()) {
            throw new WALRecoveryException(
                    String.format(
                            "Invalid key length: %d, remaining: %d", keyLen, buffer.remaining()));
        }
        byte[] key = new byte[keyLen];
        buffer.get(key);
        int valueLen = buffer.getInt();
        byte[] value;
        if (valueLen == NULL_LENGTH) {
            value = null;
        } else {
            if (valueLen < 0 || valueLen > buffer.remaining()) {
                throw new WALRecoveryException(
                        String.format(
                                "Invalid value length: %d, remaining: %d",
                                valueLen, buffer.remaining()));
            }
            value = new byte[valueLen];
            buffer.get(value);
        }
        String writeType = readString(buffer);
        return TxnWALRecord.fastpathWrite(commitTs, tableId, key, value, writeType);
    }

    private static TxnWALRecord decodeFastpathCommit(ByteBuffer buffer) {
        long batchId = buffer.getLong();
        String tableId = readString(buffer);
        return TxnWALRecord.fastpathCommit(batchId, tableId);
    }
}
