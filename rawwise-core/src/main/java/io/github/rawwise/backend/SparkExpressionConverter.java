package io.github.rawwise.backend;

import com.google.common.base.Throwables;
import io.github.rawwise.model.expression.ColumnRef;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionBuilder;
import io.github.rawwise.model.expression.Literal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.jdk.javaapi.CollectionConverters;

@Slf4j
public class SparkExpressionConverter {
    public static Expression convert(
            org.apache.spark.sql.catalyst.expressions.Expression sparkExpr) {
        if (sparkExpr == null) {
            return null;
        }

        try {
            return convertExpression(sparkExpr);
        } catch (UnsupportedOperationException e) {
            log.error(
                    "Cannot convert Spark expression: {}, reason: {}",
                    sparkExpr,
                    Throwables.getStackTraceAsString(e));
            return null;
        } catch (Exception e) {
            log.error(
                    "Error converting Spark expression: {}, exception: {}",
                    sparkExpr,
                    Throwables.getStackTraceAsString(e));
            return null;
        }
    }

    private static Expression convertExpression(
            org.apache.spark.sql.catalyst.expressions.Expression sparkExpr) {
        return switch (sparkExpr) {
            case org.apache.spark.sql.catalyst.expressions.Literal literal ->
                    convertLiteral(literal);
            case org.apache.spark.sql.catalyst.expressions.AttributeReference attributeReference ->
                    convertAttributeReference(attributeReference);
            case org.apache.spark.sql.catalyst.expressions.EqualTo equalTo ->
                    convertEqualTo(equalTo);
            case org.apache.spark.sql.catalyst.expressions.Not not -> convertNot(not);
            case org.apache.spark.sql.catalyst.expressions.GreaterThan greaterThan ->
                    convertGreaterThan(greaterThan);
            case org.apache.spark.sql.catalyst.expressions.LessThan lessThan ->
                    convertLessThan(lessThan);
            case org.apache.spark.sql.catalyst.expressions.And and -> convertAnd(and);
            case org.apache.spark.sql.catalyst.expressions.Or or -> convertOr(or);
            case org.apache.spark.sql.catalyst.expressions.In in -> convertIn(in);
            case org.apache.spark.sql.catalyst.expressions.IsNull isNull -> convertIsNull(isNull);
            case org.apache.spark.sql.catalyst.expressions.IsNotNull isNotNull ->
                    convertIsNotNull(isNotNull);
            case org.apache.spark.sql.catalyst.expressions.Like like -> convertLike(like);
                // todo implement assignment statements
                // todo implement string functions
            case org.apache.spark.sql.catalyst.expressions.StartsWith startsWith ->
                    convertStartsWith(startsWith);
            case org.apache.spark.sql.catalyst.expressions.EndsWith endsWith ->
                    convertEndsWith(endsWith);
            case org.apache.spark.sql.catalyst.expressions.Contains contains ->
                    convertContains(contains);
            case null, default ->
                    throw new UnsupportedOperationException("Unsupported expression: " + sparkExpr);
        };
    }

    private static Expression convertLiteral(
            org.apache.spark.sql.catalyst.expressions.Literal literal) {
        Object value = literal.value();

        // Handle different types of literals
        if (value == null) {
            return new Literal(null);
        }

        // Convert based on Spark DataType
        DataType dataType = literal.dataType();
        Comparable comparableValue = convertSparkValueToComparable(value, dataType);

        return ExpressionBuilder.lit(comparableValue);
    }

    // Convert column reference
    private static Expression convertAttributeReference(
            org.apache.spark.sql.catalyst.expressions.AttributeReference attributeRef) {
        String columnName = attributeRef.name();
        return ExpressionBuilder.col(columnName);
    }

    private static Expression convertEqualTo(
            org.apache.spark.sql.catalyst.expressions.EqualTo equalTo) {
        Expression left = convertExpression(equalTo.left());
        Expression right = convertExpression(equalTo.right());

        if (left != null && right != null) {
            return ExpressionBuilder.eq(left, right);
        }
        return null;
    }

    private static Expression convertNot(org.apache.spark.sql.catalyst.expressions.Not not) {
        Expression child = convertExpression(not.child());
        if (child != null) {
            return ExpressionBuilder.not(child);
        }
        return null;
    }

    private static Expression convertGreaterThan(
            org.apache.spark.sql.catalyst.expressions.GreaterThan greaterThan) {
        Expression left = convertExpression(greaterThan.left());
        Expression right = convertExpression(greaterThan.right());

        if (left != null && right != null) {
            return ExpressionBuilder.gt(left, right);
        }
        return null;
    }

    private static Expression convertLessThan(
            org.apache.spark.sql.catalyst.expressions.LessThan lessThan) {
        Expression left = convertExpression(lessThan.left());
        Expression right = convertExpression(lessThan.right());

        if (left != null && right != null) {
            return ExpressionBuilder.lt(left, right);
        }
        return null;
    }

    private static Expression convertAnd(org.apache.spark.sql.catalyst.expressions.And and) {

        List<org.apache.spark.sql.catalyst.expressions.Expression> sparkChildren =
                (List<org.apache.spark.sql.catalyst.expressions.Expression>)
                        CollectionConverters.asJava(and.children());

        List<Expression> children = new ArrayList<>();
        for (org.apache.spark.sql.catalyst.expressions.Expression sparkChild : sparkChildren) {
            Expression child = convertExpression(sparkChild);
            if (child != null) {
                children.add(child);
            }
        }

        if (children.size() == 2) {
            return ExpressionBuilder.and(children.get(0), children.get(1));
        } else if (children.size() > 2) {
            Expression result = ExpressionBuilder.and(children.get(0), children.get(1));
            for (int i = 2; i < children.size(); i++) {
                result = ExpressionBuilder.and(result, children.get(i));
            }
            return result;
        }

        return null;
    }

    private static Expression convertOr(org.apache.spark.sql.catalyst.expressions.Or or) {
        List<org.apache.spark.sql.catalyst.expressions.Expression> sparkChildren =
                (List<org.apache.spark.sql.catalyst.expressions.Expression>)
                        CollectionConverters.asJava(or.children());

        List<Expression> children = new ArrayList<>();
        for (org.apache.spark.sql.catalyst.expressions.Expression sparkChild : sparkChildren) {
            Expression child = convertExpression(sparkChild);
            if (child != null) {
                children.add(child);
            }
        }

        if (children.size() == 2) {
            return ExpressionBuilder.or(children.get(0), children.get(1));
        } else if (children.size() > 2) {
            Expression result = ExpressionBuilder.or(children.get(0), children.get(1));
            for (int i = 2; i < children.size(); i++) {
                result = ExpressionBuilder.or(result, children.get(i));
            }
            return result;
        }

        return null;
    }

    private static Expression convertIn(org.apache.spark.sql.catalyst.expressions.In in) {
        // Extract column reference
        org.apache.spark.sql.catalyst.expressions.Expression valueExpr = in.value();
        Expression columnExpr = convertExpression(valueExpr);

        if (!(columnExpr instanceof ColumnRef)) {
            return null;
        }

        List<org.apache.spark.sql.catalyst.expressions.Expression> listExprs =
                JavaConverters.seqAsJavaListConverter(in.list()).asJava();

        Set<Comparable> values = new HashSet<>();
        for (org.apache.spark.sql.catalyst.expressions.Expression expr : listExprs) {
            if (expr instanceof org.apache.spark.sql.catalyst.expressions.Literal) {
                Object value = ((org.apache.spark.sql.catalyst.expressions.Literal) expr).value();
                DataType dataType = expr.dataType();
                Comparable comparableValue = convertSparkValueToComparable(value, dataType);
                if (comparableValue != null) {
                    values.add(comparableValue);
                }
            } else {
                // If not a literal, cannot convert
                return null;
            }
        }

        String columnName = ((ColumnRef) columnExpr).getColumnName();
        return ExpressionBuilder.in(new ColumnRef(columnName), values);
    }

    private static Expression convertIsNull(
            org.apache.spark.sql.catalyst.expressions.IsNull isNull) {
        Expression child = convertExpression(isNull.child());
        if (child != null) {
            return ExpressionBuilder.isNull(child);
        }
        return null;
    }

    private static Expression convertIsNotNull(
            org.apache.spark.sql.catalyst.expressions.IsNotNull isNotNull) {
        Expression child = convertExpression(isNotNull.child());
        if (child != null) {
            return ExpressionBuilder.not(ExpressionBuilder.isNull(child));
        }
        return null;
    }

    private static Expression convertLike(org.apache.spark.sql.catalyst.expressions.Like like) {
        Expression left = convertExpression(like.left());
        Expression right = convertExpression(like.right());

        if (left != null && right instanceof Literal l && l.getValue() instanceof String s) {
            return ExpressionBuilder.like(left, s);
        }
        return null;
    }

    private static Expression convertStartsWith(
            org.apache.spark.sql.catalyst.expressions.StartsWith startsWith) {
        Expression left = convertExpression(startsWith.left());
        Expression right = convertExpression(startsWith.right());

        if (left != null && right instanceof Literal l && l.getValue() instanceof String s) {
            return ExpressionBuilder.startsWith(left, s);
        }
        return null;
    }

    private static Expression convertEndsWith(
            org.apache.spark.sql.catalyst.expressions.EndsWith endsWith) {
        Expression left = convertExpression(endsWith.left());
        Expression right = convertExpression(endsWith.right());

        if (left != null && right instanceof Literal l && l.getValue() instanceof String s) {
            return ExpressionBuilder.endsWith(left, s);
        }
        return null;
    }

    private static Expression convertContains(
            org.apache.spark.sql.catalyst.expressions.Contains contains) {
        Expression left = convertExpression(contains.left());
        Expression right = convertExpression(contains.right());

        if (left != null && right instanceof Literal l && l.getValue() instanceof String s) {
            return ExpressionBuilder.contains(left, s);
        }
        return null;
    }

    private static Comparable convertSparkValueToComparable(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }

        return switch (dataType) {
            case IntegerType t -> (Integer) value;
            case LongType t -> (Long) value;
            case DoubleType t -> (Double) value;
            case FloatType t -> (Float) value;
            case BooleanType t -> (Boolean) value;
            case StringType t -> value.toString();
            case DateType t -> (java.sql.Date) value;
            case TimestampType t -> (java.sql.Timestamp) value;
            case DecimalType t -> (java.math.BigDecimal) value;
            default -> value.toString();
        };
    }
}
