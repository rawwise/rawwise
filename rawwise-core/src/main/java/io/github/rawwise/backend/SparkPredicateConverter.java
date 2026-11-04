package io.github.rawwise.backend;

import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionBuilder;
import io.github.rawwise.model.expression.Literal;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

public class SparkPredicateConverter {

    private static final String TRUE = "ALWAYS_TRUE";
    private static final String FALSE = "ALWAYS_FALSE";
    private static final String EQ = "=";
    private static final String EQ_NULL_SAFE = "<=>";
    private static final String NOT_EQ = "<>";
    private static final String GT = ">";
    private static final String GT_EQ = ">=";
    private static final String LT = "<";
    private static final String LT_EQ = "<=";
    private static final String IN = "IN";
    private static final String IS_NULL = "IS_NULL";
    private static final String NOT_NULL = "IS_NOT_NULL";
    private static final String AND = "AND";
    private static final String OR = "OR";
    private static final String NOT = "NOT";
    private static final String STARTS_WITH = "STARTS_WITH";
    private static final String END_WITH = "ENDS_WITH";
    private static final String CONTAINS = "CONTAINS";
    private static final String LIKE = "LIKE";

    public static Expression convert(Predicate sparkPredicate) {
        if (sparkPredicate == null) {
            return null;
        }

        String predicateName = sparkPredicate.name();

        try {
            return switch (predicateName) {
                case EQ -> convertEqualTo(sparkPredicate);
                case NOT_EQ -> convertNotEqualTo(sparkPredicate);
                case GT -> convertGreaterThan(sparkPredicate);
                case GT_EQ -> convertGreaterThanOrEqual(sparkPredicate);
                case LT -> convertLessThan(sparkPredicate);
                case LT_EQ -> convertLessThanOrEqual(sparkPredicate);
                case IN -> convertIn(sparkPredicate);
                case EQ_NULL_SAFE, IS_NULL -> convertIsNull(sparkPredicate);
                case NOT_NULL -> convertIsNotNull(sparkPredicate);
                case AND -> convertAnd(sparkPredicate);
                case OR -> convertOr(sparkPredicate);
                case NOT -> convertNot(sparkPredicate);
                case STARTS_WITH -> convertStringStartsWith(sparkPredicate);
                case END_WITH -> convertStringEndsWith(sparkPredicate);
                case CONTAINS -> convertStringContains(sparkPredicate);
                case LIKE -> convertLike(sparkPredicate);
                case TRUE -> new Literal(true);
                case FALSE -> new Literal(false);
                default ->
                        throw new IllegalArgumentException(
                                "Unsupported predicate name: " + predicateName);
            };
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to convert Spark predicate: " + predicateName, e);
        }
    }

    private static Expression convertEqualTo(Predicate predicate) {
        Optional<BinaryPredicateResult> result = extractBinaryPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object value = result.get().getValue();
            return ExpressionBuilder.eq(fieldName, (Comparable) value);
        }
        throw new IllegalArgumentException("Cannot extract EqualTo predicate: " + predicate);
    }

    private static Expression convertNotEqualTo(Predicate predicate) {
        Optional<BinaryPredicateResult> result = extractBinaryPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object value = result.get().getValue();
            return ExpressionBuilder.ne(fieldName, (Comparable) value);
        }
        throw new IllegalArgumentException("Cannot extract NotEqualTo predicate: " + predicate);
    }

    private static Expression convertGreaterThan(Predicate predicate) {
        Optional<BinaryPredicateResult> result = extractBinaryPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object value = result.get().getValue();
            return ExpressionBuilder.gt(fieldName, (Comparable) value);
        }
        throw new IllegalArgumentException("Cannot extract GreaterThan predicate: " + predicate);
    }

    private static Expression convertGreaterThanOrEqual(Predicate predicate) {
        Optional<BinaryPredicateResult> result = extractBinaryPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object value = result.get().getValue();
            return ExpressionBuilder.ge(fieldName, (Comparable) value);
        }
        throw new IllegalArgumentException(
                "Cannot extract GreaterThanOrEqual predicate: " + predicate);
    }

    private static Expression convertLessThan(Predicate predicate) {
        Optional<BinaryPredicateResult> result = extractBinaryPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object value = result.get().getValue();
            return ExpressionBuilder.lt(fieldName, (Comparable) value);
        }
        throw new IllegalArgumentException("Cannot extract LessThan predicate: " + predicate);
    }

    private static Expression convertLessThanOrEqual(Predicate predicate) {
        Optional<BinaryPredicateResult> result = extractBinaryPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object value = result.get().getValue();
            return ExpressionBuilder.le(fieldName, (Comparable) value);
        }
        throw new IllegalArgumentException(
                "Cannot extract LessThanOrEqual predicate: " + predicate);
    }

    private static Expression convertIn(Predicate predicate) {
        Optional<CompoundPredicateResult> result = extractCompoundPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object[] values = result.get().getValues();
            Set<Comparable> valueSet =
                    Arrays.stream(values).map(v -> (Comparable) v).collect(Collectors.toSet());
            return ExpressionBuilder.in(fieldName, valueSet);
        }
        throw new IllegalArgumentException("Cannot extract In predicate: " + predicate);
    }

    private static Expression convertIsNull(Predicate predicate) {
        Optional<UnaryPredicateResult> result = extractUnaryPredicate(predicate);
        if (result.isPresent()) {
            return ExpressionBuilder.isNull(result.get().getFieldName());
        }
        throw new IllegalArgumentException("Cannot extract IsNull predicate: " + predicate);
    }

    private static Expression convertIsNotNull(Predicate predicate) {
        Optional<UnaryPredicateResult> result = extractUnaryPredicate(predicate);
        if (result.isPresent()) {
            return ExpressionBuilder.isNotNull(result.get().getFieldName());
        }
        throw new IllegalArgumentException("Cannot extract IsNotNull predicate: " + predicate);
    }

    private static Expression convertAnd(Predicate predicate) {
        if (predicate instanceof And and) {
            Expression left = convert(and.left());
            Expression right = convert(and.right());
            if (left == null) return right;
            if (right == null) return left;
            return ExpressionBuilder.and(left, right);
        }

        org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();
        if (children.length >= 2) {
            List<Expression> expressions = new ArrayList<>();
            for (org.apache.spark.sql.connector.expressions.Expression child : children) {
                if (child instanceof Predicate) {
                    Expression converted = convert((Predicate) child);
                    if (converted != null) {
                        expressions.add(converted);
                    }
                }
            }
            if (expressions.isEmpty()) {
                return null;
            }
            if (expressions.size() == 1) {
                return expressions.get(0);
            }
            return ExpressionBuilder.and(expressions.toArray(new Expression[0]));
        }

        throw new IllegalArgumentException("Cannot extract And predicate: " + predicate);
    }

    private static Expression convertOr(Predicate predicate) {
        if (predicate instanceof Or or) {
            Expression left = convert(or.left());
            Expression right = convert(or.right());
            if (left == null || right == null) {
                return null;
            }
            return ExpressionBuilder.or(left, right);
        }

        org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();
        if (children.length >= 2) {
            List<Expression> expressions = new ArrayList<>();
            for (org.apache.spark.sql.connector.expressions.Expression child : children) {
                if (child instanceof Predicate) {
                    Expression converted = convert((Predicate) child);
                    if (converted != null) {
                        expressions.add(converted);
                    }
                }
            }
            if (expressions.size() < 2) {
                return null;
            }
            return ExpressionBuilder.or(expressions.toArray(new Expression[0]));
        }

        throw new IllegalArgumentException("Cannot extract Or predicate: " + predicate);
    }

    private static Expression convertNot(Predicate predicate) {
        if (predicate instanceof org.apache.spark.sql.connector.expressions.filter.Not not) {
            Predicate child = not.child();

            if (IN.equals(child.name())) {
                Optional<CompoundPredicateResult> result = extractCompoundPredicate(child);
                if (result.isPresent()) {
                    String fieldName = result.get().getFieldName();
                    Object[] values = result.get().getValues();
                    Set<Comparable> valueSet =
                            Arrays.stream(values)
                                    .map(v -> (Comparable) v)
                                    .collect(Collectors.toSet());
                    return ExpressionBuilder.notIn(fieldName, valueSet);
                }
            }

            Expression childExpr = convert(child);
            if (childExpr == null) {
                return null;
            }
            return ExpressionBuilder.not(childExpr);
        }
        throw new IllegalArgumentException("Cannot extract Not predicate: " + predicate);
    }

    private static Expression convertStringStartsWith(Predicate predicate) {
        Optional<BinaryPredicateResult> result = extractBinaryPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object value = result.get().getValue();
            if (value instanceof String) {
                return ExpressionBuilder.startsWith(fieldName, (String) value);
            }
        }
        throw new IllegalArgumentException(
                "Cannot extract StringStartsWith predicate: " + predicate);
    }

    private static Expression convertStringEndsWith(Predicate predicate) {
        Optional<BinaryPredicateResult> result = extractBinaryPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object value = result.get().getValue();
            if (value instanceof String) {
                return ExpressionBuilder.endsWith(fieldName, (String) value);
            }
        }
        throw new IllegalArgumentException("Cannot extract StringEndsWith predicate: " + predicate);
    }

    private static Expression convertStringContains(Predicate predicate) {
        Optional<BinaryPredicateResult> result = extractBinaryPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object value = result.get().getValue();
            if (value instanceof String) {
                return ExpressionBuilder.contains(fieldName, (String) value);
            }
        }
        throw new IllegalArgumentException("Cannot extract StringContains predicate: " + predicate);
    }

    private static Expression convertLike(Predicate predicate) {
        Optional<BinaryPredicateResult> result = extractBinaryPredicate(predicate);
        if (result.isPresent()) {
            String fieldName = result.get().getFieldName();
            Object value = result.get().getValue();
            if (value instanceof String) {
                return ExpressionBuilder.like(fieldName, (String) value);
            }
        }
        throw new IllegalArgumentException("Cannot extract Like predicate: " + predicate);
    }

    @Getter
    private static class UnaryPredicateResult {
        private final String fieldName;

        private UnaryPredicateResult(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    @Getter
    private static class BinaryPredicateResult {
        private final String fieldName;
        private final Object value;

        private BinaryPredicateResult(String fieldName, Object value) {
            this.fieldName = fieldName;
            this.value = value;
        }
    }

    @Getter
    private static class CompoundPredicateResult {
        private final String fieldName;
        private final Object[] values;

        private CompoundPredicateResult(String fieldName, Object[] values) {
            this.fieldName = fieldName;
            this.values = values;
        }
    }

    private static String toFieldName(NamedReference ref) {
        String[] fieldNames = ref.fieldNames();
        return String.join(".", fieldNames);
    }

    private static Optional<UnaryPredicateResult> extractUnaryPredicate(Predicate sparkPredicate) {
        org.apache.spark.sql.connector.expressions.Expression[] children =
                sparkPredicate.children();
        if (children.length == 1 && children[0] instanceof NamedReference) {
            String fieldName = toFieldName((NamedReference) children[0]);
            return Optional.of(new UnaryPredicateResult(fieldName));
        }
        return Optional.empty();
    }

    private static Optional<BinaryPredicateResult> extractBinaryPredicate(
            Predicate sparkPredicate) {
        org.apache.spark.sql.connector.expressions.Expression[] children =
                sparkPredicate.children();
        if (children.length == 2) {
            if (children[0] instanceof NamedReference && children[1] instanceof Literal) {
                String fieldName = toFieldName((NamedReference) children[0]);
                Object value = ((Literal) children[1]).getValue();
                return Optional.of(new BinaryPredicateResult(fieldName, value));
            } else if (children[0] instanceof Literal && children[1] instanceof NamedReference) {
                String fieldName = toFieldName((NamedReference) children[1]);
                Object value = ((Literal) children[0]).getValue();
                return Optional.of(new BinaryPredicateResult(fieldName, value));
            }
        }
        return Optional.empty();
    }

    private static Optional<CompoundPredicateResult> extractCompoundPredicate(
            Predicate sparkPredicate) {
        org.apache.spark.sql.connector.expressions.Expression[] children =
                sparkPredicate.children();
        if (children.length >= 2) {
            if (children[0] instanceof NamedReference) {
                List<Object> values = new ArrayList<>();
                for (int i = 1; i < children.length; i++) {
                    if (!(children[i] instanceof Literal)) {
                        return Optional.empty();
                    }
                    values.add(((Literal) children[i]).getValue());
                }
                String fieldName = toFieldName((NamedReference) children[0]);
                return Optional.of(new CompoundPredicateResult(fieldName, values.toArray()));
            }
        }
        return Optional.empty();
    }
}
