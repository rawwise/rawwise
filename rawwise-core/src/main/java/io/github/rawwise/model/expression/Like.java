package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Objects;
import java.util.Set;

public class Like implements Expression {
    private final Expression left;
    private final String pattern;

    public Like(Expression left, String pattern) {
        this.left = left;
        this.pattern = Objects.requireNonNull(pattern, "Pattern cannot be null");
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
        return str.contains(pattern.replace("%", ""));
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.LIKE;
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
        return new Like(left.copy(), pattern);
    }

    @Override
    public String toString() {
        return left.toString() + " LIKE '" + pattern + "'";
    }

    public Expression getLeft() {
        return left;
    }

    public String getPattern() {
        return pattern;
    }
}
