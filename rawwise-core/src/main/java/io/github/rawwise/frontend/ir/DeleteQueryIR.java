package io.github.rawwise.frontend.ir;

import java.io.Serializable;
import java.util.*;

/**
 * DELETE point delete IR.
 *
 * <p>Produced by {@code SqlQueryParser.tryMatchDelete} only when WHERE is a PK equality predicate,
 * {@link UpdateMode#POINT} goes through FAST_PATH, {@link UpdateMode#LAZY} goes through
 * LOGICAL_PLAN.
 *
 * @param tableMeta Table metadata
 * @param pkValues primary keycolumn name → literal value(supports composite PK)
 * @param encodedKey Encoded LMDB key
 * @param snapshotId Snapshot ID (0 means read latest from LMDB)
 * @param mode Execution mode
 * @param targetExecutorId Target Executor ID
 * @param shardId Shard ID
 * @param shardEpoch Shard epoch
 */
public record DeleteQueryIR(
        TableMetaIR tableMeta,
        Map<String, Comparable<?>> pkValues,
        byte[] encodedKey,
        long snapshotId,
        UpdateMode mode,
        String targetExecutorId,
        int shardId,
        long shardEpoch)
        implements QueryIR, Serializable {

    public DeleteQueryIR {
        Objects.requireNonNull(tableMeta, "tableMeta");
        Objects.requireNonNull(pkValues, "pkValues");
        Objects.requireNonNull(mode, "mode");
        pkValues = Map.copyOf(pkValues);
        encodedKey = encodedKey == null ? new byte[0] : encodedKey.clone();
    }

    @Override
    public Set<String> tableIdentifiers() {
        return Set.of(tableMeta.tableIdentifier());
    }

    @Override
    public IRType type() {
        return IRType.POINT_DELETE;
    }
}
