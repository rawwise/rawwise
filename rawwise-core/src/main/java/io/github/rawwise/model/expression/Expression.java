package io.github.rawwise.model.expression;

import io.github.rawwise.meta.PbExpression;
import io.github.rawwise.model.expression.serializer.ProtobufExpressionSerializer;
import io.github.rawwise.model.record.Record;
import java.io.Serializable;
import java.util.Set;

public interface Expression extends Serializable {
    boolean evaluate(Record row);

    ExpressionType getType();

    Set<String> getReferencedColumns();

    Expression simplify();

    default boolean alwaysTrue() {
        return false;
    }

    default boolean alwaysFalse() {
        return false;
    }

    String toString();

    Expression copy();

    default PbExpression toProtoConverted() {
        return ProtobufExpressionSerializer.toProto(this);
    }

    enum ExpressionType implements Serializable {
        LITERAL,
        COLUMN_REF,
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        IN,
        NOT_IN,
        AND,
        OR,
        NOT,
        BETWEEN,
        LIKE,
        STARTS_WITH,
        ENDS_WITH,
        CONTAINS,
        IS_NULL,
        IS_NOT_NULL,
        DIRECT,
        ARITHMETIC_ADD,
        ARITHMETIC_SUBTRACT,
        ARITHMETIC_MULTIPLY,
        ARITHMETIC_DIVIDE,
        CASE,
        FUNCTION,
        CONDITIONAL
    }
}
