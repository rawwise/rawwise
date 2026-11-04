package io.github.rawwise.model.expression.assignment;

import io.github.rawwise.model.expression.Expression;

// example: total = price * quantity
public class Multiply extends ArithmeticBase {

    public Multiply(String targetColumn, Expression leftOperand, Expression rightOperand) {
        super(targetColumn, leftOperand, rightOperand, Operator.MULTIPLY);
    }

    @Override
    protected Expression createArithmeticExpression(
            Expression left, Expression right, Operator op) {
        return new Multiply(targetColumn, left, right);
    }

    @Override
    protected ArithmeticBase createSimplified(Expression left, Expression right) {
        return new Multiply(targetColumn, left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.ARITHMETIC_MULTIPLY;
    }
}
