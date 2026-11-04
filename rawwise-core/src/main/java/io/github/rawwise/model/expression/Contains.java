package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Set;

public class Contains implements Expression {
    private final Expression left;
    private final String substring;

    public Contains(Expression left, String substring) {
        this.left = left;
        this.substring = substring;
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
        return str.contains(substring);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.CONTAINS;
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
        return new Contains(left.copy(), substring);
    }

    @Override
    public String toString() {
        return left.toString() + " CONTAINS '" + substring + "'";
    }

    public Expression getLeft() {
        return left;
    }

    public String getSubstring() {
        return substring;
    }
}
