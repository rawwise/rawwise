package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Set;

public class StartWith implements Expression {
    private final Expression left;
    private final String prefix;

    public StartWith(Expression left, String prefix) {
        this.left = left;
        this.prefix = prefix;
    }

    @Override
    public boolean evaluate(Record row) {
        Object rawValue;
        if (left instanceof ColumnRef col) {
            Record.Value v = row.body().value(col.getColumnName());
            if (v.isNull()) return false;
            rawValue = v.raw();
        } else if (left instanceof Literal lit) {
            rawValue = lit.getValue();
        } else {
            return false;
        }

        if (!(rawValue instanceof String str)) return false;
        return str.startsWith(prefix);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.STARTS_WITH;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return left.getReferencedColumns();
    }

    @Override
    public Expression simplify() {
        return this;
    }

    @Override
    public Expression copy() {
        return new StartWith(left.copy(), prefix);
    }

    @Override
    public String toString() {
        return left.toString() + " STARTS WITH '" + prefix + "'";
    }

    public Expression getLeft() {
        return left;
    }

    public String getPrefix() {
        return prefix;
    }
}
