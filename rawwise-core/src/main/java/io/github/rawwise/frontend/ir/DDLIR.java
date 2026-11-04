package io.github.rawwise.frontend.ir;

import java.io.Serializable;
import java.util.*;

public record DDLIR(TableMetaIR tableMeta, DDLType ddlType, Map<String, Object> options)
        implements QueryIR, Serializable {

    public DDLIR {
        options = (options == null) ? Map.of() : Map.copyOf(options);
        Objects.requireNonNull(tableMeta, "tableMeta");
        Objects.requireNonNull(ddlType, "ddlType");
    }

    @Override
    public Set<String> tableIdentifiers() {
        return Set.of(tableMeta.tableIdentifier());
    }

    @Override
    public IRType type() {
        return switch (ddlType) {
            case CREATE_TABLE -> IRType.CREATE_TABLE;
            case DROP_TABLE -> IRType.DROP_TABLE;
            case ALTER_TABLE -> IRType.ALTER_TABLE;
            case TRUNCATE -> IRType.TRUNCATE;
            case REFRESH -> IRType.REFRESH;
            case CREATE_INDEX -> IRType.CREATE_INDEX;
            case DROP_INDEX -> IRType.DROP_INDEX;
        };
    }
}
