package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Objects;

public class GtEq extends BinaryComparison {

    public GtEq(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evaluate(Record row) {
        if (left instanceof ColumnRef && right instanceof Literal) {
            String column = ((ColumnRef) left).getColumnName();
            Comparable<?> value = ((Literal) right).getValue();

            Record.Value rowValue = row.body().value(column);
            if (rowValue.isNull() || value == null) return false;
            Object raw = rowValue.raw();
            if (!(raw instanceof Comparable)) return false;
            return numericAwareCompare(raw, value) >= 0;
        }

        Comparable<?> leftVal = evaluateSide(left, row);
        Comparable<?> rightVal = evaluateSide(right, row);

        if (leftVal == null || rightVal == null) {
            return false;
        }

        @SuppressWarnings("unchecked")
        int cmp = ((Comparable) leftVal).compareTo(rightVal);
        return cmp >= 0;
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        if (left == null || right == null) return false;
        return left.compareTo(right) >= 0;
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new GtEq(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.GREATER_THAN_OR_EQUAL;
    }

    @Override
    public String toString() {
        return left.toString() + " >= " + right.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GtEq that = (GtEq) obj;
        return Objects.equals(left, that.left) && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, getType());
    }

    @Override
    public Expression copy() {
        return new GtEq(left.copy(), right.copy());
    }
}
