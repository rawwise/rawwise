package io.github.rawwise.engine.txn.wal;

import io.github.rawwise.engine.txn.IsolationLevel;
import java.util.List;
import java.util.Set;

/**
 * transaction WAL recordtypeenumandcorrespondingrecordcarrier.
 *
 * <p>eachtype hex encode:
 *
 * <ul>
 *   <li>0x10 TXN_BEGIN — transactionstart
 *   <li>0x11 TXN_WRITE_INTENT — singletimewriteintent
 *   <li>0x12 TXN_BATCH_WRITE_INTENT — batchingestionwriteintent(B7 fastpath)
 *   <li>0x13 TXN_PREPARE_AND_DURABLE — prepare+datapersistence(merge PREPARE + DURABLE_DATA)
 *   <li>0x14 TXN_COMMITTED — transactioncommittedconfirm
 *   <li>0x15 TXN_ABORT — transactionrollback
 *   <li>0x16 TXN_VECTOR_PREPARED — vectorwritepreparecomplete
 *   <li>0x20 FASTPATH_WRITE — FastPath singlepointwrite(INSERT/UPDATE/DELETE)
 *   <li>0x21 FASTPATH_COMMIT — FastPath group commit batchconfirm
 * </ul>
 */
public final class TxnWALRecord {

    private TxnWALRecord() {
        /* utility class */
    }

    /** WAL recordtypeenum. */
    public enum TxnWALRecordType {
        TXN_BEGIN(0x10),
        TXN_WRITE_INTENT(0x11),
        TXN_BATCH_WRITE_INTENT(0x12),
        TXN_PREPARE_AND_DURABLE(0x13),
        TXN_COMMITTED(0x14),
        TXN_ABORT(0x15),
        TXN_VECTOR_PREPARED(0x16),
        FASTPATH_WRITE(0x20),
        FASTPATH_COMMIT(0x21);

        private final int code;

        TxnWALRecordType(int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }

        public static TxnWALRecordType fromCode(int code) {
            return switch (code) {
                case 0x10 -> TXN_BEGIN;
                case 0x11 -> TXN_WRITE_INTENT;
                case 0x12 -> TXN_BATCH_WRITE_INTENT;
                case 0x13 -> TXN_PREPARE_AND_DURABLE;
                case 0x14 -> TXN_COMMITTED;
                case 0x15 -> TXN_ABORT;
                case 0x16 -> TXN_VECTOR_PREPARED;
                case 0x20 -> FASTPATH_WRITE;
                case 0x21 -> FASTPATH_COMMIT;
                default ->
                        throw new IllegalArgumentException(
                                "Unknown WAL record type: 0x" + Integer.toHexString(code));
            };
        }
    }

    /** writetypeenum. */
    public enum WriteType {
        UPSERT,
        DELETE,
        INSERT,
        UPDATE,
        REPLACE
    }

    // --- WAL Record Carriers ---

    /** TXN_BEGIN record: transactionstart, WAL BEGIN fsync afterwrite. */
    public record TxnBeginRecord(
            long txnId,
            long readSnapshotTs,
            IsolationLevel isolationLevel,
            Set<String> tableIds,
            long timestamp) {

        public TxnBeginRecord {
            if (txnId <= 0) throw new IllegalArgumentException("txnId must be positive");
            if (isolationLevel == null)
                throw new IllegalArgumentException("isolationLevel must not be null");
            if (tableIds == null || tableIds.isEmpty())
                throw new IllegalArgumentException("tableIds must not be empty");
        }
    }

    /**
     * TXN_WRITE_INTENT record: singletimewriteintent, include key/valueRef/baseVersion
     * etccompletewriteinfo.
     */
    public record TxnWriteIntent(
            long txnId,
            Set<String> tableIds,
            String primaryTableId,
            int shardId,
            long readSnapshotTs,
            IsolationLevel isolationLevel,
            int operationSeq,
            byte[] keyBytes,
            byte[] valueRef,
            WriteType writeType,
            long baseVersion,
            int schemaVersion,
            long crc32c) {

        public TxnWriteIntent {
            if (txnId <= 0) throw new IllegalArgumentException("txnId must be positive");
            if (primaryTableId == null || primaryTableId.isBlank())
                throw new IllegalArgumentException("primaryTableId must not be blank");
            if (keyBytes == null) throw new IllegalArgumentException("keyBytes must not be null");
            if (writeType == null) throw new IllegalArgumentException("writeType must not be null");
        }
    }

    /**
     * TXN_BATCH_WRITE_INTENT record: batchingestion fast path(B7), includingfilereferencerather
     * than perrow key-value.
     */
    public record TxnBatchWriteIntent(
            long txnId,
            String tableId,
            long readSnapshotTs,
            List<FileRef> fileRefs,
            long totalRowCount,
            long totalByteSize,
            byte[] fileDigest) {

        public TxnBatchWriteIntent {
            if (txnId <= 0) throw new IllegalArgumentException("txnId must be positive");
            if (tableId == null || tableId.isBlank())
                throw new IllegalArgumentException("tableId must not be blank");
            if (fileRefs == null || fileRefs.isEmpty())
                throw new IllegalArgumentException("fileRefs must not be empty");
        }
    }

    /** TXN_PREPARE_AND_DURABLE record: prepare+datapersistencemergerecord. */
    public record TxnPrepareAndDurable(
            long txnId,
            byte[] writeSetDigest,
            List<byte[]> stagingRefs,
            byte[] sealedDigest,
            long durableTimestamp,
            List<byte[]> writtenKeys) {

        public TxnPrepareAndDurable {
            if (txnId <= 0) throw new IllegalArgumentException("txnId must be positive");
            if (sealedDigest == null)
                throw new IllegalArgumentException("sealedDigest must not be null");
            if (writtenKeys == null) writtenKeys = List.of();
        }

        /** Convenience constructor for backward compatibility without writtenKeys. */
        public TxnPrepareAndDurable(
                long txnId,
                byte[] writeSetDigest,
                List<byte[]> stagingRefs,
                byte[] sealedDigest,
                long durableTimestamp) {
            this(txnId, writeSetDigest, stagingRefs, sealedDigest, durableTimestamp, List.of());
        }
    }

    /** TXN_COMMITTED record: transactioncommittedconfirm. */
    public record TxnCommitted(long txnId, long commitTs, byte[] commitResult) {

        public TxnCommitted {
            if (txnId <= 0) throw new IllegalArgumentException("txnId must be positive");
            if (commitTs <= 0) throw new IllegalArgumentException("commitTs must be positive");
        }
    }

    /** TXN_ABORT record: transactionrollback. */
    public record TxnAbort(long txnId, String reason, long abortTimestamp) {

        public TxnAbort {
            if (txnId <= 0) throw new IllegalArgumentException("txnId must be positive");
        }
    }

    /** filereference(forbatchingestion fast path B7). */
    public record FileRef(
            String path,
            long minSequenceNumber,
            long maxSequenceNumber,
            long rowCount,
            long byteSize,
            long crc32c) {

        public FileRef {
            if (path == null || path.isBlank())
                throw new IllegalArgumentException("path must not be blank");
        }
    }

    /**
     * TXN_VECTOR_PREPARED record: vectorwritepreparecomplete, forcrash
     * recoverywhencheckvectordatacompleteity.
     */
    public record TxnVectorPrepared(
            long txnId,
            String namespace,
            String entryKey,
            String embeddingProvider,
            int dimension,
            long timestamp) {

        public TxnVectorPrepared {
            if (txnId <= 0) throw new IllegalArgumentException("txnId must be positive");
            if (namespace == null || namespace.isBlank())
                throw new IllegalArgumentException("namespace required");
        }
    }
}
