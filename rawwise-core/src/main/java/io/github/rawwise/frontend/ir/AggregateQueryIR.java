package io.github.rawwise.frontend.ir;

import io.github.rawwise.model.expression.Expression;
import java.io.Serializable;
import java.util.*;

public record AggregateQueryIR(
        List<TableMetaIR> tableMetas,
        long snapshotId,
        List<AggregateColumn> aggregates,
        List<Expression> groupByKeys,
        Expression havingFilter,
        Expression whereFilter)
        implements QueryIR, Serializable {

    public AggregateQueryIR {
        tableMetas = List.copyOf(tableMetas);
        aggregates = List.copyOf(aggregates);
        groupByKeys = (groupByKeys == null) ? List.of() : List.copyOf(groupByKeys);
        Objects.requireNonNull(tableMetas, "tableMetas");
    }

    @Override
    public Set<String> tableIdentifiers() {
        Set<String> ids = new HashSet<>();
        for (TableMetaIR m : tableMetas) ids.add(m.tableIdentifier());
        return Collections.unmodifiableSet(ids);
    }

    @Override
    public IRType type() {
        return IRType.AGGREGATE;
    }
}
