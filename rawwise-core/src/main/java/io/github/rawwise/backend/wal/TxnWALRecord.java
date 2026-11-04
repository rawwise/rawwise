package io.github.rawwise.backend.wal;

import java.util.List;

public final class TxnWALRecord {

    public enum Type {
        TXN_BEGIN,
        TXN_DATA,
        TXN_PREPARED,
        TXN_COMMIT,
        TXN_ABORT,
        FASTPATH_WRITE,
        FASTPATH_COMMIT
    }

    private final Type type;
    private final long txnId;
    private final List<String> tableIds;
    private final String tableId;
    private final String isolationLevel;
    private final long beginTs;
    private final long baseSnapshotId;
    private final byte[] key;
    private final byte[] value;
    private final String writeType;
    private final long commitTs;
    private final long newSnapshotId;

    private TxnWALRecord(
            Type type,
            long txnId,
            List<String> tableIds,
            String tableId,
            String isolationLevel,
            long beginTs,
            long baseSnapshotId,
            byte[] key,
            byte[] value,
            String writeType,
            long commitTs,
            long newSnapshotId) {
        this.type = type;
        this.txnId = txnId;
        this.tableIds = tableIds;
        this.tableId = tableId;
        this.isolationLevel = isolationLevel;
        this.beginTs = beginTs;
        this.baseSnapshotId = baseSnapshotId;
        this.key = key != null ? key.clone() : null;
        this.value = value != null ? value.clone() : null;
        this.writeType = writeType;
        this.commitTs = commitTs;
        this.newSnapshotId = newSnapshotId;
    }

    public static TxnWALRecord begin(
            long txnId,
            List<String> tableIds,
            String isolationLevel,
            long beginTs,
            long baseSnapshotId) {
        return new TxnWALRecord(
                Type.TXN_BEGIN,
                txnId,
                tableIds,
                null,
                isolationLevel,
                beginTs,
                baseSnapshotId,
                null,
                null,
                null,
                0L,
                0L);
    }

    public static TxnWALRecord data(
            long txnId, String tableId, byte[] key, byte[] value, String writeType) {
        return new TxnWALRecord(
                Type.TXN_DATA, txnId, null, tableId, null, 0L, 0L, key, value, writeType, 0L, 0L);
    }

    public static TxnWALRecord prepared(long txnId, String tableId, long commitTs) {
        return new TxnWALRecord(
                Type.TXN_PREPARED,
                txnId,
                null,
                tableId,
                null,
                0L,
                0L,
                null,
                null,
                null,
                commitTs,
                0L);
    }

    public static TxnWALRecord commit(
            long txnId, String tableId, long commitTs, long newSnapshotId) {
        return new TxnWALRecord(
                Type.TXN_COMMIT,
                txnId,
                null,
                tableId,
                null,
                0L,
                0L,
                null,
                null,
                null,
                commitTs,
                newSnapshotId);
    }

    public static TxnWALRecord abort(long txnId, String tableId) {
        return new TxnWALRecord(
                Type.TXN_ABORT, txnId, null, tableId, null, 0L, 0L, null, null, null, 0L, 0L);
    }

    public static TxnWALRecord fastpathWrite(
            long commitTs, String tableId, byte[] key, byte[] value, String writeType) {
        return new TxnWALRecord(
                Type.FASTPATH_WRITE,
                commitTs,
                null,
                tableId,
                null,
                0L,
                0L,
                key,
                value,
                writeType,
                commitTs,
                0L);
    }

    public static TxnWALRecord fastpathCommit(long batchId, String tableId) {
        return new TxnWALRecord(
                Type.FASTPATH_COMMIT,
                batchId,
                null,
                tableId,
                null,
                0L,
                0L,
                null,
                null,
                null,
                0L,
                0L);
    }

    public Type type() {
        return type;
    }

    public long txnId() {
        return txnId;
    }

    public List<String> tableIds() {
        return tableIds;
    }

    public String tableId() {
        return tableId;
    }

    public String isolationLevel() {
        return isolationLevel;
    }

    public long beginTs() {
        return beginTs;
    }

    public long baseSnapshotId() {
        return baseSnapshotId;
    }

    public byte[] key() {
        return key != null ? key.clone() : null;
    }

    public byte[] value() {
        return value != null ? value.clone() : null;
    }

    public String writeType() {
        return writeType;
    }

    public long commitTs() {
        return commitTs;
    }

    public long newSnapshotId() {
        return newSnapshotId;
    }
}
