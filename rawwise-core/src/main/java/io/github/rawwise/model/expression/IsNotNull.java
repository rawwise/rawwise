package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Set;

public class IsNotNull implements Expression {
    private final Expression child;

    public IsNotNull(Expression child) {
        this.child = child;
    }

    @Override
    public boolean evaluate(Record row) {
        if (child instanceof ColumnRef col) {
            Record.Value v = row.body().value(col.getColumnName());
            return !v.isNull();
        }
        return true;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.IS_NOT_NULL;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return child.getReferencedColumns();
    }

    @Override
    public Expression simplify() {
        return this;
    }

    @Override
    public Expression copy() {
        return new IsNotNull(child.copy());
    }

    @Override
    public String toString() {
        return child.toString() + " IS NOT NULL";
    }

    public Expression getColumn() {
        return child;
    }
}
