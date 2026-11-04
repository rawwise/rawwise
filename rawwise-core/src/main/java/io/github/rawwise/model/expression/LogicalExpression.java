package io.github.rawwise.model.expression;

import java.util.List;

public interface LogicalExpression extends Expression {
    List<Expression> children();
}
