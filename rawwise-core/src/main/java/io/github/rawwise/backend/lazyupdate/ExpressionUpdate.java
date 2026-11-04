package io.github.rawwise.backend.lazyupdate;

import java.util.Objects;

public sealed interface ExpressionUpdate
        permits ExpressionUpdate.LiteralUpdate,
                ExpressionUpdate.ColumnValueUpdate,
                ExpressionUpdate.ArithmeticUpdate {

    record LiteralUpdate(Comparable<?> value) implements ExpressionUpdate {
        public LiteralUpdate {}
    }

    record ColumnValueUpdate(String sourceColumn) implements ExpressionUpdate {
        public ColumnValueUpdate {
            Objects.requireNonNull(sourceColumn, "sourceColumn must not be null");
        }
    }

    record ArithmeticUpdate(String sourceColumn, ArithmeticOp op, Number operand)
            implements ExpressionUpdate {
        public ArithmeticUpdate {
            Objects.requireNonNull(sourceColumn, "sourceColumn must not be null");
            Objects.requireNonNull(op, "op must not be null");
            Objects.requireNonNull(operand, "operand must not be null");
        }
    }

    enum ArithmeticOp {
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE
    }
}
