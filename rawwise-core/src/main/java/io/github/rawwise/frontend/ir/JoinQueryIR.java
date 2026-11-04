package io.github.rawwise.frontend.ir;

import io.github.rawwise.model.expression.Expression;
import java.io.Serializable;
import java.util.*;

public record JoinQueryIR(
        List<TableMetaIR> tableMetas,
        long snapshotId,
        QueryIR left,
        QueryIR right,
        JoinType joinType,
        Expression joinCondition,
        List<ProjectionColumn> projection,
        Expression postFilter,
        int limit)
        implements QueryIR, Serializable {

    public JoinQueryIR {
        tableMetas = List.copyOf(tableMetas);
        projection = List.copyOf(projection);
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        Objects.requireNonNull(joinType, "joinType");
    }

    @Override
    public Set<String> tableIdentifiers() {
        Set<String> ids = new HashSet<>();
        for (TableMetaIR m : tableMetas) ids.add(m.tableIdentifier());
        ids.addAll(left.tableIdentifiers());
        ids.addAll(right.tableIdentifiers());
        return Collections.unmodifiableSet(ids);
    }

    @Override
    public IRType type() {
        return IRType.JOIN;
    }
}
