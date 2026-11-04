package io.github.rawwise.model.expression;

public interface BinaryExpression extends Expression {
    Expression getLeft();

    Expression getRight();
}
