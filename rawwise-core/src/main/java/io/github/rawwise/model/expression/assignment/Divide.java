package io.github.rawwise.model.expression.assignment;

import io.github.rawwise.model.expression.Expression;

// example: average = total / count
public class Divide extends ArithmeticBase {

    public Divide(String targetColumn, Expression leftOperand, Expression rightOperand) {
        super(targetColumn, leftOperand, rightOperand, Operator.DIVIDE);
    }

    @Override
    protected Expression createArithmeticExpression(
            Expression left, Expression right, Operator op) {
        return new Divide(targetColumn, left, right);
    }

    @Override
    protected ArithmeticBase createSimplified(Expression left, Expression right) {
        return new Divide(targetColumn, left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.ARITHMETIC_DIVIDE;
    }
}
