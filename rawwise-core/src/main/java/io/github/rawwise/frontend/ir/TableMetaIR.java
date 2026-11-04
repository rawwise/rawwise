package io.github.rawwise.frontend.ir;

import io.github.rawwise.model.record.StructType;
import io.github.rawwise.model.record.TableId;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public record TableMetaIR(
        String tableIdentifier,
        long tableId,
        TableType tableType,
        StructType schema,
        List<String> primaryKeyColumns,
        Map<String, String> properties,
        int schemaVersion)
        implements Serializable {

    public TableMetaIR {
        primaryKeyColumns = List.copyOf(primaryKeyColumns);
        properties = Map.copyOf(properties);
    }

    public TableId getTableId() {
        return TableId.parseLenient(tableIdentifier);
    }
}
