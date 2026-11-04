package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Set;

public class EndWith implements Expression {
    private final Expression left;
    private final String suffix;

    public EndWith(Expression left, String suffix) {
        this.left = left;
        this.suffix = suffix;
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
        return str.endsWith(suffix);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.ENDS_WITH;
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
        return new EndWith(left.copy(), suffix);
    }

    @Override
    public String toString() {
        return left.toString() + " ENDS WITH '" + suffix + "'";
    }

    public Expression getLeft() {
        return left;
    }

    public String getSuffix() {
        return suffix;
    }
}
