package io.github.rawwise.model.expression.assignment;

import io.github.rawwise.model.expression.Expression;

// stock = stock - quantity
public class Subtract extends ArithmeticBase {

    public Subtract(String targetColumn, Expression leftOperand, Expression rightOperand) {
        super(targetColumn, leftOperand, rightOperand, Operator.SUBTRACT);
    }

    @Override
    protected Expression createArithmeticExpression(
            Expression left, Expression right, Operator op) {
        return new Subtract(targetColumn, left, right);
    }

    @Override
    protected ArithmeticBase createSimplified(Expression left, Expression right) {
        return new Subtract(targetColumn, left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.ARITHMETIC_SUBTRACT;
    }
}
