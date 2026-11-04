package io.github.rawwise.frontend.ir;

import io.github.rawwise.model.expression.Expression;
import java.io.Serializable;
import java.util.*;

public record ScanQueryIR(
        TableMetaIR tableMeta,
        long snapshotId,
        List<ProjectionColumn> projection,
        Expression filter,
        List<SortKey> sortKeys,
        int limit,
        int offset,
        Expression preFilter,
        LockMode lockMode)
        implements QueryIR, Serializable {

    public ScanQueryIR {
        projection = List.copyOf(projection);
        sortKeys = (sortKeys == null) ? List.of() : List.copyOf(sortKeys);
        lockMode = (lockMode == null) ? LockMode.NONE : lockMode;
    }

    /** Compatibility constructor (no lockMode, defaults to {@link LockMode#NONE}). */
    public ScanQueryIR(
            TableMetaIR tableMeta,
            long snapshotId,
            List<ProjectionColumn> projection,
            Expression filter,
            List<SortKey> sortKeys,
            int limit,
            int offset,
            Expression preFilter) {
        this(
                tableMeta,
                snapshotId,
                projection,
                filter,
                sortKeys,
                limit,
                offset,
                preFilter,
                LockMode.NONE);
    }

    @Override
    public Set<String> tableIdentifiers() {
        return Set.of(tableMeta.tableIdentifier());
    }

    @Override
    public IRType type() {
        return IRType.SCAN;
    }
}
