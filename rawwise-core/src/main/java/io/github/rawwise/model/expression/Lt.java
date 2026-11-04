package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;

public class Lt extends BinaryComparison {

    public Lt(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evaluate(Record row) {
        if (left instanceof ColumnRef col && right instanceof Literal lit) {
            String column = col.getColumnName();
            Comparable<?> value = lit.getValue();

            Record.Value rowValue = row.body().value(column);
            if (rowValue.isNull() || value == null) return false;
            Object raw = rowValue.raw();
            if (!(raw instanceof Comparable)) return false;
            return numericAwareCompare(raw, value) < 0;
        }

        Comparable<?> leftVal = evaluateSide(left, row);
        Comparable<?> rightVal = evaluateSide(right, row);

        if (leftVal == null || rightVal == null) {
            return false;
        }

        return ((Comparable) leftVal).compareTo(rightVal) < 0;
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        return left != null && right != null && left.compareTo(right) < 0;
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new Lt(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.LESS_THAN;
    }

    @Override
    public String toString() {
        return left.toString() + " < " + right.toString();
    }
}
