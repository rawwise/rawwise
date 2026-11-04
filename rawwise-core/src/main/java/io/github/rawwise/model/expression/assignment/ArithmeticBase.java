package io.github.rawwise.model.expression.assignment;

import io.github.rawwise.model.expression.ColumnRef;
import io.github.rawwise.model.expression.Direct;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.Literal;
import io.github.rawwise.model.record.Record;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import lombok.Getter;

public abstract class ArithmeticBase implements Assignment {
    protected final String targetColumn;
    @Getter protected final Expression leftOperand;
    @Getter protected final Expression rightOperand;
    protected final Operator operator;

    protected ArithmeticBase(
            String targetColumn,
            Expression leftOperand,
            Expression rightOperand,
            Operator operator) {
        this.targetColumn = targetColumn;
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
        this.operator = operator;
    }

    @Override
    public boolean evaluate(Record row) {
        throw new UnsupportedOperationException("Arithmetic is for structure representation only");
    }

    @Override
    public String getTargetColumn() {
        return targetColumn;
    }

    @Override
    public Expression getValueExpression() {
        return createArithmeticExpression(leftOperand, rightOperand, operator);
    }

    protected abstract Expression createArithmeticExpression(
            Expression left, Expression right, Operator op);

    private Comparable performOperation(Comparable left, Comparable right, Operator op) {
        // todo simplified arithmetic operation implementation,actual applications need more
        // complete type checking and conversion
        try {
            if (left instanceof Number && right instanceof Number) {
                double leftNum = ((Number) left).doubleValue();
                double rightNum = ((Number) right).doubleValue();

                switch (op) {
                    case ADD:
                        return leftNum + rightNum;
                    case SUBTRACT:
                        return leftNum - rightNum;
                    case MULTIPLY:
                        return leftNum * rightNum;
                    case DIVIDE:
                        if (rightNum == 0) return null;
                        return leftNum / rightNum;
                    case MODULO:
                        if (rightNum == 0) return null;
                        return leftNum % rightNum;
                }
            }
        } catch (Exception e) {
            // todo handle operation exception
        }

        return null;
    }

    @Override
    public boolean selfAssignment() {
        // check if it is self-assignment, age = age + 1
        if (leftOperand instanceof ColumnRef) {
            String leftColumn = ((ColumnRef) leftOperand).getColumnName();
            return targetColumn.equals(leftColumn);
        }
        return false;
    }

    @Override
    public Set<String> getReferencedColumns() {
        Set<String> columns = new HashSet<>();
        columns.add(targetColumn);
        columns.addAll(leftOperand.getReferencedColumns());
        columns.addAll(rightOperand.getReferencedColumns());
        return columns;
    }

    @Override
    public Expression simplify() {
        Expression simplifiedLeft = leftOperand.simplify();
        Expression simplifiedRight = rightOperand.simplify();

        // if all are literals, can pre-compute
        if (simplifiedLeft instanceof Literal && simplifiedRight instanceof Literal) {

            Comparable leftVal = ((Literal) simplifiedLeft).getValue();
            Comparable rightVal = ((Literal) simplifiedRight).getValue();

            if (leftVal != null && rightVal != null) {
                Comparable result = performOperation(leftVal, rightVal, operator);
                if (result != null) {
                    return new Direct(true);
                }
            }
        }

        return createSimplified(simplifiedLeft, simplifiedRight);
    }

    protected abstract ArithmeticBase createSimplified(Expression left, Expression right);

    @Override
    public Expression copy() {
        try {
            return getClass()
                    .getDeclaredConstructor(
                            String.class, Expression.class, Expression.class, Operator.class)
                    .newInstance(targetColumn, leftOperand.copy(), rightOperand.copy(), operator);
        } catch (Exception e) {
            throw new RuntimeException("Failed to copy expression", e);
        }
    }

    @Override
    public String toString() {
        return targetColumn + " = " + leftOperand + " " + operator.getSymbol() + " " + rightOperand;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ArithmeticBase that = (ArithmeticBase) obj;
        return Objects.equals(targetColumn, that.targetColumn)
                && Objects.equals(leftOperand, that.leftOperand)
                && Objects.equals(rightOperand, that.rightOperand)
                && operator == that.operator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetColumn, leftOperand, rightOperand, operator);
    }

    @Getter
    public enum Operator implements Serializable {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULO("%"),
        POWER("^");

        private final String symbol;

        Operator(String symbol) {
            this.symbol = symbol;
        }
    }
}
