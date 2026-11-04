package io.github.rawwise.model.expression.assignment;

import io.github.rawwise.model.expression.Expression;

// example:age = age + 1
public class Add extends ArithmeticBase {

    public Add(String targetColumn, Expression leftOperand, Expression rightOperand) {
        super(targetColumn, leftOperand, rightOperand, Operator.ADD);
    }

    @Override
    protected Expression createArithmeticExpression(
            Expression left, Expression right, Operator op) {
        return new Add(targetColumn, left, right);
    }

    @Override
    protected ArithmeticBase createSimplified(Expression left, Expression right) {
        return new Add(targetColumn, left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.ARITHMETIC_ADD;
    }
}
