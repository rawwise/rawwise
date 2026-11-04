package io.github.rawwise.backend.lazyupdate;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.*;

public final class ExpressionUpdateApplier {

    private ExpressionUpdateApplier() {}

    public static Map<String, Comparable<?>> apply(
            Map<String, Comparable<?>> originalRow, Map<String, ExpressionUpdate> updates) {
        Objects.requireNonNull(originalRow, "originalRow");
        Objects.requireNonNull(updates, "updates");

        Map<String, Comparable<?>> result = new HashMap<>(originalRow);

        for (Map.Entry<String, ExpressionUpdate> entry : updates.entrySet()) {
            String targetColumn = entry.getKey();
            ExpressionUpdate update = entry.getValue();
            Comparable<?> newValue = evaluate(update, originalRow);
            result.put(targetColumn, newValue);
        }

        return result;
    }

    private static Comparable<?> evaluate(
            ExpressionUpdate update, Map<String, Comparable<?>> originalRow) {
        return switch (update) {
            case ExpressionUpdate.LiteralUpdate lit -> lit.value();
            case ExpressionUpdate.ColumnValueUpdate col -> {
                if (!originalRow.containsKey(col.sourceColumn())) {
                    throw new IllegalArgumentException(
                            "ColumnValueUpdate: sourceColumn '"
                                    + col.sourceColumn()
                                    + "' does not exist in original row. Available: "
                                    + originalRow.keySet());
                }
                yield originalRow.get(col.sourceColumn());
            }
            case ExpressionUpdate.ArithmeticUpdate arith -> {
                Comparable<?> base = originalRow.get(arith.sourceColumn());
                yield computeArithmetic(base, arith.op(), arith.operand());
            }
        };
    }

    private static Comparable<?> computeArithmetic(
            Comparable<?> base, ExpressionUpdate.ArithmeticOp op, Number operand) {
        if (base == null || operand == null) {
            return null;
        }
        if (!(base instanceof Number b)) {
            throw new IllegalArgumentException(
                    "Arithmetic on non-numeric base type: " + base.getClass().getName());
        }
        BigDecimal bv = toBigDecimal(b);
        BigDecimal ov = toBigDecimal(operand);

        return switch (op) {
            case ADD -> bv.add(ov);
            case SUBTRACT -> bv.subtract(ov);
            case MULTIPLY -> bv.multiply(ov);
            case DIVIDE -> {
                if (ov.compareTo(BigDecimal.ZERO) == 0) {
                    throw new ArithmeticException("Division by zero in expression update");
                }
                yield bv.divide(ov, MathContext.DECIMAL128);
            }
        };
    }

    private static BigDecimal toBigDecimal(Number n) {
        if (n instanceof BigDecimal bd) return bd;
        if (n instanceof Integer i) return BigDecimal.valueOf(i);
        if (n instanceof Long l) return BigDecimal.valueOf(l);
        if (n instanceof Double d) return BigDecimal.valueOf(d);
        if (n instanceof Float f) return BigDecimal.valueOf(f.doubleValue());
        try {
            return new BigDecimal(n.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Cannot convert Number to BigDecimal: "
                            + n
                            + " (type: "
                            + n.getClass().getName()
                            + ")",
                    e);
        }
    }
}
