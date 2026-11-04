package io.github.rawwise.frontend.ir;

import io.github.rawwise.model.expression.Expression;
import java.io.Serializable;
import java.util.*;

public record UpdateRangeIR(
        TableMetaIR tableMeta, Map<String, Comparable<?>> setClauses, Expression rangeFilter)
        implements QueryIR, Serializable {

    public UpdateRangeIR {
        setClauses = Map.copyOf(setClauses);
        Objects.requireNonNull(tableMeta, "tableMeta");
        // null rangeFilter means "apply to all rows"
    }

    @Override
    public Set<String> tableIdentifiers() {
        return Set.of(tableMeta.tableIdentifier());
    }

    @Override
    public IRType type() {
        return IRType.UPDATE_RANGE;
    }
}
