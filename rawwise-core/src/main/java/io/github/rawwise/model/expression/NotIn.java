package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.*;
import java.util.stream.Collectors;

public class NotIn implements Expression {
    private final Expression left;
    private final Set<Comparable<?>> values;

    public NotIn(Expression left, Set<Comparable<?>> values) {
        this.left = left;
        this.values = new LinkedHashSet<>(values);
    }

    @Override
    public boolean evaluate(Record row) {
        Object rawValue;
        if (left instanceof ColumnRef col) {
            Record.Value v = row.body().value(col.getColumnName());
            if (v.isNull()) return true;
            rawValue = v.raw();
        } else if (left instanceof Literal lit) {
            rawValue = lit.getValue();
        } else {
            return true;
        }

        if (rawValue == null) return true;
        for (Comparable<?> v : values) {
            if (v != null && BinaryComparison.numericAwareEquals(v, rawValue)) return false;
        }
        return true;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.NOT_IN;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return left.getReferencedColumns();
    }

    @Override
    public Expression simplify() {
        if (values.isEmpty()) {
            return new Literal(true);
        }
        return this;
    }

    @Override
    public Expression copy() {
        return new NotIn(left.copy(), new LinkedHashSet<>(values));
    }

    @Override
    public String toString() {
        String valueStr =
                values.stream()
                        .map(v -> v instanceof String ? "'" + v + "'" : String.valueOf(v))
                        .collect(Collectors.joining(", "));
        return left.toString() + " NOT IN (" + valueStr + ")";
    }

    public Expression getLeft() {
        return left;
    }

    public Set<Comparable<?>> getValues() {
        return Collections.unmodifiableSet(values);
    }
}
