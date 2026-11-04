package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract class BinaryComparison implements BinaryExpression {
    protected final Expression left;
    protected final Expression right;

    protected BinaryComparison(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public Set<String> getReferencedColumns() {
        Set<String> columns = new HashSet<>();
        columns.addAll(left.getReferencedColumns());
        columns.addAll(right.getReferencedColumns());
        return columns;
    }

    @Override
    public Expression simplify() {
        Expression simplifiedLeft = left.simplify();
        Expression simplifiedRight = right.simplify();

        if (simplifiedLeft instanceof Literal && simplifiedRight instanceof Literal) {
            Comparable leftVal = ((Literal) simplifiedLeft).getValue();
            Comparable rightVal = ((Literal) simplifiedRight).getValue();

            if (leftVal == null || rightVal == null) {
                return new Literal(false);
            }

            return new Literal(compare(leftVal, rightVal));
        }

        return createSimplified(simplifiedLeft, simplifiedRight);
    }

    protected abstract Expression createSimplified(Expression left, Expression right);

    protected abstract boolean compare(Comparable left, Comparable right);

    @Override
    public boolean evaluate(Record row) {
        Comparable<?> leftVal = evaluateSide(left, row);
        Comparable<?> rightVal = evaluateSide(right, row);

        if (leftVal == null || rightVal == null) {
            return false;
        }

        return compare(leftVal, rightVal);
    }

    @Override
    public Expression getLeft() {
        return left;
    }

    @Override
    public Expression getRight() {
        return right;
    }

    protected Comparable<?> evaluateSide(Expression expr, Record row) {
        if (expr instanceof ColumnRef col) {
            return row.body().value(col.getColumnName()).comparable();
        }
        if (expr instanceof Literal lit) {
            Object val = lit.getValue();
            if (!(val instanceof Comparable<?>))
                throw new ClassCastException("Literal not Comparable: " + val.getClass());
            return (Comparable<?>) val;
        }
        throw new IllegalArgumentException("Unsupported expression type: " + expr.getClass());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        BinaryComparison that = (BinaryComparison) obj;
        return Objects.equals(left, that.left) && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, getClass());
    }

    @Override
    public Expression copy() {
        try {
            return getClass()
                    .getDeclaredConstructor(Expression.class, Expression.class)
                    .newInstance(left.copy(), right.copy());
        } catch (Exception e) {
            throw new RuntimeException("Failed to copy expression", e);
        }
    }

    public static boolean numericAwareEquals(Object a, Object b) {
        if (a == null || b == null) return false;
        if (a.equals(b)) return true;
        if (a instanceof Number na && b instanceof Number nb) {
            return na.longValue() == nb.longValue() && na.doubleValue() == nb.doubleValue();
        }
        return false;
    }

    public static int numericAwareCompare(Object a, Object b) {
        if (!(a instanceof Number na) || !(b instanceof Number nb)) {
            if (!(a instanceof Comparable) || !(b instanceof Comparable)) {
                throw new IllegalArgumentException(
                        "Cannot compare " + a.getClass() + " with " + b.getClass());
            }
            @SuppressWarnings("unchecked")
            Comparable<Object> ca = (Comparable<Object>) a;
            return ca.compareTo(b);
        }
        return Long.compare(na.longValue(), nb.longValue());
    }
}
