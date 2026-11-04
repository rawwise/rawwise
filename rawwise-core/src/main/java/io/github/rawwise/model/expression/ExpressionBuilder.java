package io.github.rawwise.model.expression;

import io.github.rawwise.model.expression.assignment.Add;
import io.github.rawwise.model.expression.assignment.Divide;
import io.github.rawwise.model.expression.assignment.Multiply;
import io.github.rawwise.model.expression.assignment.Subtract;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@SuppressWarnings("rawtypes")
public class ExpressionBuilder {
    public static ColumnRef col(String columnName) {
        return new ColumnRef(columnName);
    }

    public static Literal lit(Comparable value) {
        return new Literal(value);
    }

    public static Eq eq(Expression left, Expression right) {
        return new Eq(left, right);
    }

    public static Eq eq(String column, Comparable value) {
        return new Eq(col(column), lit(value));
    }

    public static NotEq ne(Expression left, Expression right) {
        return new NotEq(left, right);
    }

    public static NotEq ne(String column, Comparable value) {
        return new NotEq(col(column), lit(value));
    }

    public static Gt gt(Expression left, Expression right) {
        return new Gt(left, right);
    }

    public static Gt gt(String column, Comparable value) {
        return new Gt(col(column), lit(value));
    }

    public static Lt lt(Expression left, Expression right) {
        return new Lt(left, right);
    }

    public static Lt lt(String column, Comparable value) {
        return new Lt(col(column), lit(value));
    }

    public static GtEq ge(Expression left, Expression right) {
        return new GtEq(left, right);
    }

    public static GtEq ge(String column, Comparable value) {
        return new GtEq(col(column), lit(value));
    }

    public static LtEq le(Expression left, Expression right) {
        return new LtEq(left, right);
    }

    public static LtEq le(String column, Comparable value) {
        return new LtEq(col(column), lit(value));
    }

    @SuppressWarnings("unchecked")
    public static In in(Expression column, Set<? extends Comparable> values) {
        return new In(column, (Set<Comparable<?>>) (Set) values);
    }

    @SuppressWarnings("unchecked")
    public static In in(String column, Set<? extends Comparable> values) {
        return new In(col(column), (Set<Comparable<?>>) (Set) values);
    }

    @SuppressWarnings("unchecked")
    public static NotIn notIn(Expression column, Set<? extends Comparable> values) {
        return new NotIn(column, (Set<Comparable<?>>) (Set) values);
    }

    @SuppressWarnings("unchecked")
    public static NotIn notIn(String column, Set<? extends Comparable> values) {
        return new NotIn(col(column), (Set<Comparable<?>>) (Set) values);
    }

    public static And and(Expression... expressions) {
        return new And(expressions);
    }

    public static Or or(Expression... expressions) {
        return new Or(expressions);
    }

    public static Not not(Expression expression) {
        return new Not(expression);
    }

    public static Between between(Expression column, Expression lower, Expression upper) {
        return new Between(column, lower, upper);
    }

    public static Between between(String column, Comparable lower, Comparable upper) {
        return new Between(col(column), lit(lower), lit(upper));
    }

    public static Like like(Expression column, String pattern) {
        return new Like(column, pattern);
    }

    public static Like like(String column, String pattern) {
        return new Like(col(column), pattern);
    }

    public static StartWith startsWith(Expression column, String prefix) {
        return new StartWith(column, prefix);
    }

    public static StartWith startsWith(String column, String prefix) {
        return new StartWith(col(column), prefix);
    }

    public static EndWith endsWith(Expression column, String suffix) {
        return new EndWith(column, suffix);
    }

    public static EndWith endsWith(String column, String suffix) {
        return new EndWith(col(column), suffix);
    }

    public static Contains contains(Expression column, String substring) {
        return new Contains(column, substring);
    }

    public static Contains contains(String column, String substring) {
        return new Contains(col(column), substring);
    }

    public static IsNull isNull(Expression column) {
        return new IsNull(column);
    }

    public static IsNull isNull(String column) {
        return new IsNull(col(column));
    }

    public static IsNotNull isNotNull(Expression column) {
        return new IsNotNull(column);
    }

    public static IsNotNull isNotNull(String column) {
        return new IsNotNull(col(column));
    }

    public static Expression range(
            String column,
            Comparable min,
            boolean minInclusive,
            Comparable max,
            boolean maxInclusive) {
        List<Expression> conditions = new ArrayList<>();

        if (min != null) {
            if (minInclusive) {
                conditions.add(ge(column, min));
            } else {
                conditions.add(gt(column, min));
            }
        }

        if (max != null) {
            if (maxInclusive) {
                conditions.add(le(column, max));
            } else {
                conditions.add(lt(column, max));
            }
        }

        if (conditions.isEmpty()) {
            return new Literal(true);
        } else if (conditions.size() == 1) {
            return conditions.getFirst();
        } else {
            return new And(conditions.toArray(new Expression[0]));
        }
    }

    public static Add add(String targetColumn, Expression left, Expression right) {
        return new Add(targetColumn, left, right);
    }

    public static Subtract subtract(String targetColumn, Expression left, Expression right) {
        return new Subtract(targetColumn, left, right);
    }

    public static Multiply multiply(String targetColumn, Expression left, Expression right) {
        return new Multiply(targetColumn, left, right);
    }

    public static Divide divide(String targetColumn, Expression left, Expression right) {
        return new Divide(targetColumn, left, right);
    }
}
