package io.github.rawwise.frontend.ir;

import io.github.rawwise.model.expression.Expression;
import java.io.Serializable;
import java.util.*;

public record DeleteRangeIR(TableMetaIR tableMeta, Expression rangeFilter)
        implements QueryIR, Serializable {

    public DeleteRangeIR {
        Objects.requireNonNull(tableMeta, "tableMeta");
        // null rangeFilter means "apply to all rows"
    }

    @Override
    public Set<String> tableIdentifiers() {
        return Set.of(tableMeta.tableIdentifier());
    }

    @Override
    public IRType type() {
        return IRType.DELETE_RANGE;
    }
}
