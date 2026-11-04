package io.github.rawwise.backend;

import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class SparkV1FilterConverter {

    private SparkV1FilterConverter() {}

    public static Expression convert(org.apache.spark.sql.sources.Filter filter) {
        if (filter == null) {
            return null;
        }
        try {
            return doConvert(filter);
        } catch (Exception e) {
            log.warn("Failed to convert Spark V1 filter: {}, reason: {}", filter, e.getMessage());
            return null;
        }
    }

    public static Expression convertAll(org.apache.spark.sql.sources.Filter[] filters) {
        if (filters == null || filters.length == 0) {
            return null;
        }
        List<Expression> conditions = new ArrayList<>();
        for (org.apache.spark.sql.sources.Filter filter : filters) {
            Expression converted = convert(filter);
            if (converted != null) {
                conditions.add(converted);
            }
        }
        if (conditions.isEmpty()) {
            return null;
        }
        if (conditions.size() == 1) {
            return conditions.get(0);
        }
        return ExpressionBuilder.and(conditions.toArray(new Expression[0]));
    }

    private static Expression doConvert(org.apache.spark.sql.sources.Filter filter) {
        if (filter instanceof org.apache.spark.sql.sources.EqualTo f) {
            return ExpressionBuilder.eq(f.attribute(), (Comparable) f.value());
        }
        if (filter instanceof org.apache.spark.sql.sources.GreaterThan f) {
            return ExpressionBuilder.gt(f.attribute(), (Comparable) f.value());
        }
        if (filter instanceof org.apache.spark.sql.sources.GreaterThanOrEqual f) {
            return ExpressionBuilder.ge(f.attribute(), (Comparable) f.value());
        }
        if (filter instanceof org.apache.spark.sql.sources.LessThan f) {
            return ExpressionBuilder.lt(f.attribute(), (Comparable) f.value());
        }
        if (filter instanceof org.apache.spark.sql.sources.LessThanOrEqual f) {
            return ExpressionBuilder.le(f.attribute(), (Comparable) f.value());
        }
        if (filter instanceof org.apache.spark.sql.sources.And f) {
            Expression left = doConvert(f.left());
            Expression right = doConvert(f.right());
            if (left == null) return right;
            if (right == null) return left;
            return ExpressionBuilder.and(left, right);
        }
        if (filter instanceof org.apache.spark.sql.sources.Or f) {
            Expression left = doConvert(f.left());
            Expression right = doConvert(f.right());
            if (left == null || right == null) {
                return null;
            }
            return ExpressionBuilder.or(left, right);
        }
        if (filter instanceof org.apache.spark.sql.sources.Not f) {
            Expression child = doConvert(f.child());
            if (child == null) return null;
            if (f.child() instanceof org.apache.spark.sql.sources.In inFilter) {
                Set<Comparable> values = extractInValues(inFilter.values());
                if (!values.isEmpty()) {
                    return ExpressionBuilder.notIn(inFilter.attribute(), values);
                }
            }
            return ExpressionBuilder.not(child);
        }
        if (filter instanceof org.apache.spark.sql.sources.IsNull f) {
            return ExpressionBuilder.isNull(f.attribute());
        }
        if (filter instanceof org.apache.spark.sql.sources.IsNotNull f) {
            return ExpressionBuilder.isNotNull(f.attribute());
        }
        if (filter instanceof org.apache.spark.sql.sources.In f) {
            Set<Comparable> values = extractInValues(f.values());
            if (!values.isEmpty()) {
                return ExpressionBuilder.in(f.attribute(), values);
            }
            return null;
        }
        if (filter instanceof org.apache.spark.sql.sources.StringStartsWith f) {
            return ExpressionBuilder.startsWith(f.attribute(), f.value());
        }
        if (filter instanceof org.apache.spark.sql.sources.StringEndsWith f) {
            return ExpressionBuilder.endsWith(f.attribute(), f.value());
        }
        if (filter instanceof org.apache.spark.sql.sources.StringContains f) {
            return ExpressionBuilder.contains(f.attribute(), f.value());
        }
        if (filter instanceof org.apache.spark.sql.sources.AlwaysTrue) {
            return ExpressionBuilder.lit(true);
        }
        if (filter instanceof org.apache.spark.sql.sources.AlwaysFalse) {
            return ExpressionBuilder.lit(false);
        }
        log.debug("Unsupported Spark V1 filter type: {}", filter.getClass().getSimpleName());
        return null;
    }

    private static Set<Comparable> extractInValues(Object[] values) {
        Set<Comparable> result = new HashSet<>();
        if (values == null) return result;
        for (Object v : values) {
            if (v instanceof Comparable) {
                result.add((Comparable) v);
            }
        }
        return result;
    }
}
