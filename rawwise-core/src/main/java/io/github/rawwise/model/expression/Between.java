package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.HashSet;
import java.util.Set;

public class Between implements Expression {
    private final Expression left;
    private final Expression lower;
    private final Expression upper;

    public Between(Expression left, Expression lower, Expression upper) {
        this.left = left;
        this.lower = lower;
        this.upper = upper;
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

        Object lowerVal = (lower instanceof Literal lit) ? lit.getValue() : null;
        Object upperVal = (upper instanceof Literal lit) ? lit.getValue() : null;

        if (!(rawValue instanceof Comparable) || lowerVal == null || upperVal == null) return false;

        @SuppressWarnings("unchecked")
        Comparable<Object> cmp = (Comparable<Object>) rawValue;
        return BinaryComparison.numericAwareCompare(cmp, lowerVal) >= 0
                && BinaryComparison.numericAwareCompare(cmp, upperVal) <= 0;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.BETWEEN;
    }

    @Override
    public Set<String> getReferencedColumns() {
        Set<String> columns = new HashSet<>();
        columns.addAll(left.getReferencedColumns());
        columns.addAll(lower.getReferencedColumns());
        columns.addAll(upper.getReferencedColumns());
        return columns;
    }

    @Override
    public Expression simplify() {
        return new And(new GtEq(left, lower), new LtEq(left, upper)).simplify();
    }

    @Override
    public Expression copy() {
        return new Between(left.copy(), lower.copy(), upper.copy());
    }

    @Override
    public String toString() {
        return left.toString() + " BETWEEN " + lower.toString() + " AND " + upper.toString();
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getLower() {
        return lower;
    }

    public Expression getUpper() {
        return upper;
    }
}
