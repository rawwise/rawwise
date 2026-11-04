package io.github.rawwise.model.expression.serializer;

import io.github.rawwise.model.expression.Expression;

public interface ExpressionSerializer {
    byte[] serialize(Expression expression);

    Expression deserialize(byte[] bytes);
}
