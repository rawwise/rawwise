package io.github.rawwise.frontend.ir;

import java.io.Serializable;
import java.util.*;

/**
 * UPDATE point update IR.
 *
 * <p>Produced by {@code SqlQueryParser.tryMatchUpdate} only when WHERE is a PK equality predicate,
 * {@link UpdateMode#POINT} goes through FAST_PATH, {@link UpdateMode#LAZY} goes through
 * LOGICAL_PLAN.
 *
 * @param tableMeta Table metadata
 * @param pkValues primary keycolumn name → literal value(supports composite PK)
 * @param encodedKey Encoded LMDB key (produced by KeyCodec, can be null for Backend-side encoding)
 * @param setClauses SET clause: column name → new value
 * @param snapshotId Snapshot ID (0 means read latest from LMDB)
 * @param mode Execution mode
 * @param targetExecutorId Target Executor ID (for FAST_PATH routing)
 * @param shardId Shard ID
 * @param shardEpoch Shard epoch
 */
public record UpdateQueryIR(
        TableMetaIR tableMeta,
        Map<String, Comparable<?>> pkValues,
        byte[] encodedKey,
        Map<String, Comparable<?>> setClauses,
        long snapshotId,
        UpdateMode mode,
        String targetExecutorId,
        int shardId,
        long shardEpoch)
        implements QueryIR, Serializable {

    public UpdateQueryIR {
        Objects.requireNonNull(tableMeta, "tableMeta");
        Objects.requireNonNull(pkValues, "pkValues");
        Objects.requireNonNull(setClauses, "setClauses");
        Objects.requireNonNull(mode, "mode");
        pkValues = Map.copyOf(pkValues);
        setClauses = Map.copyOf(setClauses);
        encodedKey = encodedKey == null ? new byte[0] : encodedKey.clone();
    }

    @Override
    public Set<String> tableIdentifiers() {
        return Set.of(tableMeta.tableIdentifier());
    }

    @Override
    public IRType type() {
        return IRType.POINT_UPDATE;
    }
}
