package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;

public class Eq extends BinaryComparison {

    public Eq(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evaluate(Record row) {
        if (left instanceof ColumnRef && right instanceof Literal) {
            String column = ((ColumnRef) left).getColumnName();
            Comparable<?> value = ((Literal) right).getValue();

            Record.Value rowValue = row.body().value(column);
            if (rowValue.isNull()) return false;
            Object raw = rowValue.raw();
            return raw != null && numericAwareEquals(raw, value);
        }

        Comparable<?> leftVal = evaluateSide(left, row);
        Comparable<?> rightVal = evaluateSide(right, row);

        if (leftVal == null || rightVal == null) {
            return false;
        }

        return numericAwareEquals(leftVal, rightVal);
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        return left != null && left.equals(right);
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new Eq(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.EQUAL;
    }

    @Override
    public String toString() {
        return left.toString() + " = " + right.toString();
    }
}
