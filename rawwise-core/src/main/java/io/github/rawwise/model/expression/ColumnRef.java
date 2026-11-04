package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class ColumnRef implements Expression {
    private final String columnName;

    public ColumnRef(String columnName) {
        this.columnName = Objects.requireNonNull(columnName, "Column name cannot be null");
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public boolean evaluate(Record row) {
        return row.body().has(columnName);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.COLUMN_REF;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return Collections.singleton(columnName);
    }

    @Override
    public Expression simplify() {
        return this;
    }

    @Override
    public Expression copy() {
        return new ColumnRef(columnName);
    }

    @Override
    public String toString() {
        return columnName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ColumnRef that = (ColumnRef) obj;
        return columnName.equals(that.columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}
