package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Objects;

public class NotEq extends BinaryComparison {

    public NotEq(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evaluate(Record row) {
        if (left instanceof ColumnRef && right instanceof Literal) {
            String column = ((ColumnRef) left).getColumnName();
            Comparable<?> value = ((Literal) right).getValue();

            Record.Value rowValue = row.body().value(column);
            if (value == null) {
                return !rowValue.isNull();
            }
            if (rowValue.isNull()) return true;
            Object raw = rowValue.raw();
            return !numericAwareEquals(raw, value);
        }

        Comparable<?> leftVal = evaluateSide(left, row);
        Comparable<?> rightVal = evaluateSide(right, row);

        if (leftVal == null && rightVal == null) {
            return false;
        }

        if (leftVal == null || rightVal == null) {
            return true;
        }

        return !numericAwareEquals(leftVal, rightVal);
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        if (left == null && right == null) return false;
        if (left == null || right == null) return true;
        return !left.equals(right);
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new NotEq(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.NOT_EQUAL;
    }

    @Override
    public String toString() {
        return left.toString() + " != " + right.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        NotEq that = (NotEq) obj;
        return Objects.equals(left, that.left) && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, getType());
    }

    @Override
    public Expression copy() {
        return new NotEq(left.copy(), right.copy());
    }
}
