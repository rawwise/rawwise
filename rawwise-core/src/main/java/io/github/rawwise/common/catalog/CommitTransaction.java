package io.github.rawwise.common.catalog;

import io.github.rawwise.model.record.TableId;
import java.util.Arrays;
import java.util.List;

/**
 * An in-progress commit transaction. {@link Catalog#newCommit(TableId)} returns this handle.
 *
 * <p>Design notes:
 *
 * <ul>
 *   <li>Pure record (no behavior methods), conforming to Java record data carrier semantics
 *   <li>commit/abort behavior is on {@link Catalog} interface, CommitTransaction only carries data
 *   <li>Implements {@link AutoCloseable}, close is no-op, for try-with-resources syntax support
 *   <li>commitId is persistently allocated via Metastore at creation time (idempotency guarantee)
 * </ul>
 *
 * @param commitId Persistently allocated unique commit ID
 * @param tableId Target table
 * @param baseSnapshotId Snapshot ID the transaction is based on (OCC check point)
 * @param operations List of table operations included in this commit
 */
public record CommitTransaction(
        long commitId,
        TableId tableId,
        long baseSnapshotId,
        List<CommitTransaction.TableOperation> operations)
        implements AutoCloseable {

    /**
     * Marker implementation for try-with-resources. notauto abort——abort requires Catalog
     * reference, handled explicitly by caller.
     */
    @Override
    public void close() {
        // no-op: does not auto-abort, handled explicitly by Catalog.abort
    }

    /**
     * A table operation (ADD/DELETE/UPDATE/MERGE, etc.). Pure data carrier, operationType
     * identifies operation category, payload is the serialized content of the operation.
     */
    public record TableOperation(String operationType, String summary, byte[] payload) {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TableOperation that)) return false;
            return operationType.equals(that.operationType)
                    && summary.equals(that.summary)
                    && Arrays.equals(payload, that.payload);
        }

        @Override
        public int hashCode() {
            int result = operationType.hashCode();
            result = 31 * result + summary.hashCode();
            result = 31 * result + Arrays.hashCode(payload);
            return result;
        }

        @Override
        public String toString() {
            return "TableOperation[type="
                    + operationType
                    + ", summary="
                    + summary
                    + ", payload.len="
                    + (payload != null ? payload.length : 0)
                    + "]";
        }
    }
}
