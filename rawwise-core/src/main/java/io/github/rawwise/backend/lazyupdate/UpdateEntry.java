package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionUtils;
import java.io.Serializable;
import java.util.*;
import lombok.Getter;

public class UpdateEntry implements Serializable {
    @Getter private final String tableName;
    @Getter private final Expression predicate;
    // Stores concrete values instead of assignment statements for convenient value validation
    // (type), comparable; alternatively a valueRef could be defined to represent the value
    private final Map<String, Comparable<?>> updates;
    @Getter private final long timestamp;
    @Getter private final String entryId;

    public UpdateEntry(String tableName, Expression predicate, Map<String, Comparable<?>> updates) {
        this.tableName = tableName;
        this.predicate = predicate;
        this.updates = new HashMap<>(updates);
        this.timestamp = System.currentTimeMillis();
        this.entryId = UUID.randomUUID().toString();
    }

    public boolean isPrimaryKeyUpdate(Set<String> primaryKeys) {
        return ExpressionUtils.isPrimaryKeyEquals(predicate, primaryKeys);
    }

    public Map<String, Comparable<?>> getPrimaryKeyValues(Set<String> primaryKeys) {
        return ExpressionUtils.extractPrimaryKeyValues(predicate, primaryKeys);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map<String, Comparable> getUpdates() {
        return Collections.unmodifiableMap(updates);
    }

    @Override
    public String toString() {
        return String.format("UPDATE SET %s WHERE %s", updates, predicate);
    }
}
