package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Objects;

public class LtEq extends BinaryComparison {

    public LtEq(Expression left, Expression right) {
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
            return numericAwareCompare(raw, value) <= 0;
        }

        Comparable<?> leftVal = evaluateSide(left, row);
        Comparable<?> rightVal = evaluateSide(right, row);

        if (leftVal == null || rightVal == null) {
            return false;
        }

        @SuppressWarnings("unchecked")
        int cmp = ((Comparable) leftVal).compareTo(rightVal);
        return cmp <= 0;
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        if (left == null || right == null) return false;
        return left.compareTo(right) <= 0;
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new LtEq(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.LESS_THAN_OR_EQUAL;
    }

    @Override
    public String toString() {
        return left.toString() + " <= " + right.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        LtEq that = (LtEq) obj;
        return Objects.equals(left, that.left) && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, getType());
    }

    @Override
    public Expression copy() {
        return new LtEq(left.copy(), right.copy());
    }
}
