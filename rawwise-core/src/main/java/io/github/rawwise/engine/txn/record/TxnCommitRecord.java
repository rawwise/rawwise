package io.github.rawwise.engine.txn.record;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Set;

/**
 * Transaction commit record — the durable, globally-visible marker that a transaction has been
 * committed.
 *
 * <p>This record is written to the Metastore after the snapshot metadata CAS succeeds. It serves as
 * the cross-domain atomic commit contract: once a {@code TxnCommitRecord} exists for a given {@code
 * txnId}, the transaction is irrevocably committed and its writes are visible to all readers
 * (CCI-010: No partial commit).
 *
 * <h3>Key format</h3>
 *
 * <pre>
 * /txn_commit_record/{catalog}/{db}/{table}/{txnId}
 * </pre>
 *
 * <h3>Idempotency</h3>
 *
 * <p>The record is written via Metastore CAS with a {@code NotExists} precondition. If the CAS
 * fails (the key already exists), the write is treated as an idempotent success — the transaction
 * was already committed by a prior attempt (e.g., crash recovery replay).
 *
 * <h3>unified strategy (TxnId ≡ CommitTs)</h3>
 *
 * <p>{@code txnId} and {@code commitTs} are the same value. Both fields are retained for semantic
 * clarity and forward compatibility (future designs may decouple them).
 *
 * <h3>Append-only / Forward compatibility</h3>
 *
 * <p>The JSON serialization uses {@code FAIL_ON_UNKNOWN_PROPERTIES=false}, so old code ignores new
 * fields added in future versions. This makes the record safe to extend without breaking existing
 * readers.
 */
public record TxnCommitRecord(
        long txnId,
        long commitTs,
        String tableId,
        Set<String> tableIds,
        long snapshotId,
        long previousSnapshotId,
        int schemaVersion,
        long createdAtMs,
        CommitType commitType) {

    /** Sentinel value for an unassigned snapshot ID. */
    public static final long UNASSIGNED_SNAPSHOT_ID = -1L;

    private static final ObjectMapper MAPPER = createMapper();

    public TxnCommitRecord {
        if (txnId <= 0) {
            throw new IllegalArgumentException("txnId must be positive, got: " + txnId);
        }
        if (commitTs <= 0) {
            throw new IllegalArgumentException("commitTs must be positive, got: " + commitTs);
        }
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (tableIds == null || tableIds.isEmpty()) {
            throw new IllegalArgumentException("tableIds must not be null or empty");
        }
        if (commitType == null) {
            throw new IllegalArgumentException("commitType must not be null");
        }
        tableIds = Set.copyOf(tableIds);
    }

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    /**
     * Serialize this record to JSON bytes.
     *
     * @return JSON byte array
     * @throws IOException if serialization fails
     */
    public byte[] toJson() throws IOException {
        return MAPPER.writeValueAsBytes(this);
    }

    /**
     * Deserialize a {@link TxnCommitRecord} from JSON bytes.
     *
     * @param data JSON byte array
     * @return deserialized record
     * @throws IOException if deserialization fails
     */
    public static TxnCommitRecord fromJson(byte[] data) throws IOException {
        return MAPPER.readValue(data, TxnCommitRecord.class);
    }

    /**
     * Build the Metastore key for a TxnCommitRecord.
     *
     * <p>Key format: {@code /txn_commit_record/{catalog}/{db}/{table}/{txnId}}
     *
     * @param catalog catalog name
     * @param db database name
     * @param table table name
     * @param txnId transaction ID
     * @return UTF-8 encoded key bytes
     */
    public static byte[] buildMetastoreKey(String catalog, String db, String table, long txnId) {
        return ("/txn_commit_record/" + catalog + "/" + db + "/" + table + "/" + txnId)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Build the Metastore prefix for scanning all TxnCommitRecords of a table.
     *
     * <p>Prefix format: {@code /txn_commit_record/{catalog}/{db}/{table}/}
     *
     * @param catalog catalog name
     * @param db database name
     * @param table table name
     * @return UTF-8 encoded prefix bytes
     */
    public static byte[] buildMetastorePrefix(String catalog, String db, String table) {
        return ("/txn_commit_record/" + catalog + "/" + db + "/" + table + "/")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
