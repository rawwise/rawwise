package io.github.rawwise.engine.txn;

import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TxnCommitLogDao {

    private static final String PREFIX_TXN_COMMIT_LOG = "txn-commit-log:";

    private final Metastore store;

    public record TxnCommitRecord(long txnId, long commitId, long snapshotId, long commitTs) {}

    public TxnCommitLogDao(Metastore store) {
        if (store == null) {
            throw new IllegalArgumentException("Metastore must not be null");
        }
        this.store = store;
    }

    public static byte[] commitLogKey(long txnId) {
        return (PREFIX_TXN_COMMIT_LOG + txnId).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] serializeRecord(TxnCommitRecord record) {
        String serialized =
                record.txnId()
                        + ":"
                        + record.commitId()
                        + ":"
                        + record.snapshotId()
                        + ":"
                        + record.commitTs();
        return serialized.getBytes(StandardCharsets.UTF_8);
    }

    public static TxnCommitRecord deserializeRecord(byte[] data) throws IOException {
        String[] parts = new String(data, StandardCharsets.UTF_8).split(":");
        if (parts.length != 4) {
            throw new IOException(
                    "Invalid txn_commit_log format: expected 4 fields, got " + parts.length);
        }
        try {
            return new TxnCommitRecord(
                    Long.parseLong(parts[0]),
                    Long.parseLong(parts[1]),
                    Long.parseLong(parts[2]),
                    Long.parseLong(parts[3]));
        } catch (NumberFormatException e) {
            throw new IOException("Invalid txn_commit_log numeric format", e);
        }
    }

    public Optional<TxnCommitRecord> findByTxnId(long txnId) throws IOException {
        byte[] key = commitLogKey(txnId);
        Optional<byte[]> data = store.get(key);
        if (data.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(deserializeRecord(data.get()));
    }

    public void insert(TxnCommitRecord record) throws IOException {
        byte[] key = commitLogKey(record.txnId());
        byte[] value = serializeRecord(record);
        boolean success =
                store.compareAndSwap(
                        List.of(new Precondition.NotExists(key)),
                        List.of(new Mutation.Put(key, value)));
        if (!success) {
            throw new IOException("Duplicate txn_commit_log entry for txnId=" + record.txnId());
        }
        log.debug(
                "Inserted txn_commit_log: txnId={}, commitId={}, snapshotId={}",
                record.txnId(),
                record.commitId(),
                record.snapshotId());
    }
}
