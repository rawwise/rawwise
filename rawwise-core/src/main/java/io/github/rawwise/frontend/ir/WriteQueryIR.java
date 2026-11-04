package io.github.rawwise.frontend.ir;

import java.io.Serializable;
import java.util.*;

public record WriteQueryIR(
        TableMetaIR tableMeta,
        WriteType writeType,
        List<Map<String, Comparable<?>>> rows,
        QueryIR insertFromQuery)
        implements QueryIR, Serializable {

    public WriteQueryIR {
        rows = (rows == null) ? List.of() : rows.stream().map(Map::copyOf).toList();
        Objects.requireNonNull(tableMeta, "tableMeta");
        Objects.requireNonNull(writeType, "writeType");
    }

    @Override
    public Set<String> tableIdentifiers() {
        return Set.of(tableMeta.tableIdentifier());
    }

    @Override
    public IRType type() {
        return writeType == WriteType.BATCH_INSERT ? IRType.BATCH_INSERT : IRType.INSERT;
    }
}
