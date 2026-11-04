package io.github.rawwise.frontend.ir.adaptor;

import io.github.rawwise.backend.lazyupdate.LazyUpdateBridge;
import io.github.rawwise.backend.lazyupdate.LazyUpdateWriteInterceptor;
import io.github.rawwise.backend.spark.RawWiseSparkTable;
import io.github.rawwise.frontend.ir.AggregateColumn;
import io.github.rawwise.frontend.ir.AggregateFunction;
import io.github.rawwise.frontend.ir.AggregateQueryIR;
import io.github.rawwise.frontend.ir.DDLIR;
import io.github.rawwise.frontend.ir.DeleteRangeIR;
import io.github.rawwise.frontend.ir.JoinQueryIR;
import io.github.rawwise.frontend.ir.MetadataQueryIR;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.frontend.ir.ScanQueryIR;
import io.github.rawwise.frontend.ir.SortKey;
import io.github.rawwise.frontend.ir.TableMetaIR;
import io.github.rawwise.frontend.ir.UpdateRangeIR;
import io.github.rawwise.frontend.ir.WriteQueryIR;
import io.github.rawwise.frontend.ir.WriteType;
import io.github.rawwise.model.expression.BinaryExpression;
import io.github.rawwise.model.expression.ColumnRef;
import io.github.rawwise.model.expression.IsNotNull;
import io.github.rawwise.model.expression.IsNull;
import io.github.rawwise.model.expression.Literal;
import io.github.rawwise.model.expression.LogicalExpression;
import io.github.rawwise.model.record.RawWiseType;
import io.github.rawwise.model.record.StructType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.expressions.Ascending$;
import org.apache.spark.sql.catalyst.expressions.Descending$;
import org.apache.spark.sql.catalyst.expressions.NullOrdering;
import org.apache.spark.sql.catalyst.expressions.NullsFirst$;
import org.apache.spark.sql.catalyst.expressions.NullsLast$;
import org.apache.spark.sql.catalyst.expressions.SortDirection;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.JoinHint;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Offset;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

@Slf4j
public class SparkIRAdaptor implements IRAdaptor<LogicalPlan> {

    @Override
    public LogicalPlan adapt(QueryIR ir) {
        return switch (ir.type()) {
            case SCAN -> adaptScan((ScanQueryIR) ir);
            case AGGREGATE -> adaptAggregate((AggregateQueryIR) ir);
            case JOIN -> adaptJoin((JoinQueryIR) ir);
            case INSERT, BATCH_INSERT -> adaptWrite((WriteQueryIR) ir);
            case UPDATE_RANGE -> adaptUpdateRange((UpdateRangeIR) ir);
            case DELETE_RANGE -> adaptDeleteRange((DeleteRangeIR) ir);
            case CREATE_TABLE,
                            DROP_TABLE,
                            ALTER_TABLE,
                            TRUNCATE,
                            REFRESH,
                            CREATE_INDEX,
                            DROP_INDEX ->
                    adaptDDL((DDLIR) ir);
            case METADATA_COUNT_ALL -> adaptMetadataCountAll((MetadataQueryIR.CountAll) ir);
            case POINT_QUERY,
                            BATCH_POINT_QUERY,
                            POINT_UPDATE,
                            POINT_DELETE,
                            METADATA_COUNT_COLUMN,
                            METADATA_MIN,
                            METADATA_MAX,
                            METADATA_TABLE_INFO ->
                    throw new UnsupportedOperationException(
                            ir.type() + " not supported in Spark adaptor");
        };
    }

    @Override
    public Object adaptPredicate(io.github.rawwise.model.expression.Expression predicate) {
        return toSparkExpression(predicate);
    }

    @Override
    public Object adaptSchema(StructType schema) {
        return toSparkStructType(schema);
    }

    @SuppressWarnings("deprecation")
    LogicalPlan adaptScan(ScanQueryIR ir) {
        LogicalPlan plan = relation(ir.tableMeta());
        plan = applyFilter(plan, ir.filter());
        plan = buildProjection(plan, ir.projection());

        if (!ir.sortKeys().isEmpty()) {
            List<SortOrder> orders = new ArrayList<>();
            for (SortKey sk : ir.sortKeys()) {
                org.apache.spark.sql.catalyst.expressions.Expression expr =
                        new UnresolvedAttribute(toScalaSeq(sk.column()));
                SortDirection dir = sk.ascending() ? Ascending$.MODULE$ : Descending$.MODULE$;
                NullOrdering nulls = sk.nullsFirst() ? NullsFirst$.MODULE$ : NullsLast$.MODULE$;
                orders.add(new SortOrder(expr, dir, nulls, SortOrder.apply$default$3()));
            }
            plan = new Sort(toSeq(orders), true, plan, Sort.$lessinit$greater$default$4());
        }

        if (ir.limit() > 0) {
            plan = new GlobalLimit(sparkLiteral(ir.limit()), plan);
        }

        if (ir.offset() > 0) {
            plan = new Offset(sparkLiteral(ir.offset()), plan);
        }

        return plan;
    }

    @SuppressWarnings("deprecation")
    LogicalPlan adaptAggregate(AggregateQueryIR ir) {
        LogicalPlan plan;
        if (ir.tableMetas().size() == 1) {
            plan = relation(ir.tableMetas().get(0));
        } else {
            List<LogicalPlan> relations =
                    ir.tableMetas().stream().map(this::relation).collect(Collectors.toList());
            plan = relations.get(0);
            for (int i = 1; i < relations.size(); i++) {
                plan =
                        new Join(
                                plan,
                                relations.get(i),
                                org.apache.spark.sql.catalyst.plans.Inner$.MODULE$,
                                Option.empty(),
                                JoinHint.NONE());
            }
        }

        plan = applyFilter(plan, ir.whereFilter());

        Seq<org.apache.spark.sql.catalyst.expressions.Expression> groupKeys =
                toSeq(
                        ir.groupByKeys().stream()
                                .map(this::toSparkExpression)
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()));

        Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> aggExprs =
                toSeq(
                        ir.aggregates().stream()
                                .map(this::toAggregateNamedExpression)
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()));

        plan = new Aggregate(groupKeys, aggExprs, plan, Option.empty());
        plan = applyFilter(plan, ir.havingFilter());

        return plan;
    }

    @SuppressWarnings("deprecation")
    LogicalPlan adaptMetadataCountAll(MetadataQueryIR.CountAll ir) {
        LogicalPlan plan = relation(ir.tableMeta());
        org.apache.spark.sql.catalyst.expressions.Expression starExpr =
                new org.apache.spark.sql.catalyst.expressions.Literal(
                        1, org.apache.spark.sql.types.DataTypes.IntegerType);
        org.apache.spark.sql.catalyst.analysis.UnresolvedFunction countFunc =
                new org.apache.spark.sql.catalyst.analysis.UnresolvedFunction(
                        toScalaSeq("count"),
                        toSeq(List.of(starExpr)),
                        false,
                        org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
                                .$lessinit$greater$default$4(),
                        org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
                                .$lessinit$greater$default$5(),
                        org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
                                .$lessinit$greater$default$6(),
                        org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
                                .$lessinit$greater$default$7());
        org.apache.spark.sql.catalyst.expressions.NamedExpression countAlias =
                new org.apache.spark.sql.catalyst.expressions.Alias(
                        countFunc,
                        "count(1)",
                        org.apache.spark.sql.catalyst.expressions.Alias.$lessinit$greater$default$3(
                                countFunc, "count(1)"),
                        org.apache.spark.sql.catalyst.expressions.Alias.$lessinit$greater$default$4(
                                countFunc, "count(1)"),
                        org.apache.spark.sql.catalyst.expressions.Alias.$lessinit$greater$default$5(
                                countFunc, "count(1)"),
                        org.apache.spark.sql.catalyst.expressions.Alias.$lessinit$greater$default$6(
                                countFunc, "count(1)"));
        plan = new Aggregate(toSeq(List.of()), toSeq(List.of(countAlias)), plan, Option.empty());
        return plan;
    }

    @SuppressWarnings("deprecation")
    LogicalPlan adaptJoin(JoinQueryIR ir) {
        LogicalPlan leftPlan = adapt(ir.left());
        LogicalPlan rightPlan = adapt(ir.right());

        org.apache.spark.sql.catalyst.plans.JoinType sparkJoinType = toSparkJoinType(ir.joinType());

        Option<org.apache.spark.sql.catalyst.expressions.Expression> condition;
        if (ir.joinCondition() != null) {
            org.apache.spark.sql.catalyst.expressions.Expression cond =
                    toSparkExpression(ir.joinCondition());
            condition = Option.apply(cond);
        } else {
            condition = Option.empty();
        }

        LogicalPlan plan = new Join(leftPlan, rightPlan, sparkJoinType, condition, JoinHint.NONE());

        plan = buildProjection(plan, ir.projection());
        plan = applyFilter(plan, ir.postFilter());

        if (ir.limit() > 0) {
            plan = new GlobalLimit(sparkLiteral(ir.limit()), plan);
        }

        return plan;
    }

    @SuppressWarnings("deprecation")
    LogicalPlan adaptWrite(WriteQueryIR q) {
        TableMetaIR meta = q.tableMeta();
        LogicalPlan relation = relation(meta);

        if (q.writeType() == WriteType.INSERT_SELECT && q.insertFromQuery() != null) {
            LogicalPlan sourcePlan = adapt(q.insertFromQuery());
            return new InsertIntoStatement(
                    relation,
                    scala.collection.immutable.Map$.MODULE$.empty(),
                    scala.collection.immutable.Seq$.MODULE$.empty(),
                    sourcePlan,
                    false,
                    false,
                    false);
        }

        // For INSERT / BATCH_INSERT with row values, construct a LocalRelation
        // carrying the VALUES data as InternalRow instances.
        if (!q.rows().isEmpty()) {
            org.apache.spark.sql.types.StructType sparkSchema = toSparkStructType(meta.schema());
            List<org.apache.spark.sql.catalyst.InternalRow> internalRows = new ArrayList<>();
            for (Map<String, Comparable<?>> row : q.rows()) {
                org.apache.spark.sql.types.StructField[] fields = sparkSchema.fields();
                Object[] values = new Object[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    values[i] = coerceValue(row.get(fields[i].name()), fields[i].dataType());
                }
                internalRows.add(
                        new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(values));
            }
            @SuppressWarnings("unchecked")
            scala.collection.immutable.Seq<org.apache.spark.sql.catalyst.InternalRow> rowSeq =
                    scala.collection.immutable.List.from(
                            JavaConverters.asScalaBufferConverter(internalRows).asScala());
            LocalRelation localRel =
                    new LocalRelation(relation.output(), rowSeq, false, scala.Option.apply(null));
            return new InsertIntoStatement(
                    relation,
                    scala.collection.immutable.Map$.MODULE$.empty(),
                    scala.collection.immutable.Seq$.MODULE$.empty(),
                    localRel,
                    false,
                    false,
                    false);
        }

        // Fallback: return DataSourceV2Relation for empty INSERT (no-op) cases
        return relation;
    }

    @SuppressWarnings("deprecation")
    LogicalPlan adaptUpdateRange(UpdateRangeIR q) {
        LazyUpdateWriteInterceptor interceptor = LazyUpdateBridge.interceptor();
        if (interceptor != null) {
            try {
                interceptor.interceptRangeUpdate(
                        normalizeTableId(q.tableMeta().tableIdentifier()),
                        q.rangeFilter() != null ? q.rangeFilter() : new Literal(true),
                        q.setClauses(),
                        0L);
            } catch (java.io.IOException e) {
                throw new RuntimeException(
                        "Failed to write UpdateLog for table=" + q.tableMeta().tableIdentifier(),
                        e);
            }
        }
        return emptyRelation(q.tableMeta());
    }

    @SuppressWarnings("deprecation")
    LogicalPlan adaptDeleteRange(DeleteRangeIR q) {
        LazyUpdateWriteInterceptor interceptor = LazyUpdateBridge.interceptor();
        if (interceptor != null) {
            try {
                interceptor.interceptRangeDelete(
                        normalizeTableId(q.tableMeta().tableIdentifier()),
                        q.rangeFilter() != null ? q.rangeFilter() : new Literal(true),
                        0L);
            } catch (java.io.IOException e) {
                throw new RuntimeException(
                        "Failed to write DeleteLog for table=" + q.tableMeta().tableIdentifier(),
                        e);
            }
        }
        return emptyRelation(q.tableMeta());
    }

    private static String normalizeTableId(String tableId) {
        String[] parts = tableId.split("\\.");
        if (parts.length >= 3) {
            return parts[parts.length - 2] + "." + parts[parts.length - 1];
        }
        return tableId;
    }

    @SuppressWarnings("deprecation")
    LogicalPlan adaptDDL(DDLIR q) {
        TableMetaIR meta = q.tableMeta();
        LogicalPlan relation = relation(meta);

        // DDL operations all produce a DataSourceV2Relation for the target table.
        // The actual DDL execution (create, drop, truncate, refresh) happens at
        // the execution layer; the logical plan simply identifies the target.
        return relation;
    }

    @SuppressWarnings("deprecation")
    org.apache.spark.sql.catalyst.expressions.Expression toSparkExpression(
            io.github.rawwise.model.expression.Expression expr) {
        if (expr == null) return null;

        return switch (expr.getType()) {
            case LITERAL -> toSparkLiteral((Literal) expr);
            case COLUMN_REF -> toSparkColumnRef((ColumnRef) expr);
            case EQUAL -> toSparkBinaryComparison("EqualTo", (BinaryExpression) expr);
            case NOT_EQUAL -> toSparkBinaryComparison("NotEqualTo", (BinaryExpression) expr);
            case GREATER_THAN -> toSparkBinaryComparison("GreaterThan", (BinaryExpression) expr);
            case GREATER_THAN_OR_EQUAL ->
                    toSparkBinaryComparison("GreaterThanOrEqual", (BinaryExpression) expr);
            case LESS_THAN -> toSparkBinaryComparison("LessThan", (BinaryExpression) expr);
            case LESS_THAN_OR_EQUAL ->
                    toSparkBinaryComparison("LessThanOrEqual", (BinaryExpression) expr);
            case AND -> toSparkLogicalAnd((LogicalExpression) expr);
            case OR -> toSparkLogicalOr((LogicalExpression) expr);
            case NOT -> toSparkNot((LogicalExpression) expr);
            case IS_NULL -> toSparkIsNull((IsNull) expr);
            case IS_NOT_NULL -> toSparkIsNotNull((IsNotNull) expr);
            default ->
                    throw new IRAdaptException(
                            "ScanQueryIR",
                            "filter",
                            "Unsupported expression type: " + expr.getClass().getSimpleName());
        };
    }

    @SuppressWarnings("deprecation")
    private org.apache.spark.sql.catalyst.expressions.NamedExpression toAggregateNamedExpression(
            AggregateColumn ac) {
        org.apache.spark.sql.catalyst.expressions.Expression colRef =
                new UnresolvedAttribute(toScalaSeq(ac.column()));

        String funcName = toSparkAggregateFunctionName(ac.function());

        Seq<org.apache.spark.sql.catalyst.expressions.Expression> args = toSeq(List.of(colRef));

        org.apache.spark.sql.catalyst.analysis.UnresolvedFunction uf =
                new org.apache.spark.sql.catalyst.analysis.UnresolvedFunction(
                        toScalaSeq(funcName),
                        args,
                        false, // isDistinct
                        org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
                                .$lessinit$greater$default$4(), // filter
                        org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
                                .$lessinit$greater$default$5(), // ignoreNulls
                        org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
                                .$lessinit$greater$default$6(), // orderingWithinGroup
                        org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
                                .$lessinit$greater$default$7() // isInternal
                        );

        String alias =
                (ac.alias() != null && !ac.alias().isEmpty())
                        ? ac.alias()
                        : funcName + "(" + ac.column() + ")";

        return new org.apache.spark.sql.catalyst.expressions.Alias(
                uf,
                alias,
                org.apache.spark.sql.catalyst.expressions.Alias.$lessinit$greater$default$3(
                        uf, alias),
                org.apache.spark.sql.catalyst.expressions.Alias.$lessinit$greater$default$4(
                        uf, alias),
                org.apache.spark.sql.catalyst.expressions.Alias.$lessinit$greater$default$5(
                        uf, alias),
                org.apache.spark.sql.catalyst.expressions.Alias.$lessinit$greater$default$6(
                        uf, alias));
    }

    private String toSparkAggregateFunctionName(AggregateFunction func) {
        return switch (func) {
            case SUM -> "sum";
            case COUNT -> "count";
            case AVG -> "avg";
            case MIN -> "min";
            case MAX -> "max";
            case COUNT_DISTINCT -> "count_distinct";
        };
    }

    private org.apache.spark.sql.catalyst.expressions.Literal sparkLiteral(int v) {
        return new org.apache.spark.sql.catalyst.expressions.Literal(v, DataTypes.IntegerType);
    }

    /**
     * Coerce a Java value to the Spark-schema-expected type. Fixes ClassCastException when Long is
     * used for INT columns.
     */
    private static Object coerceValue(Object value, org.apache.spark.sql.types.DataType sparkType) {
        if (value == null) return null;
        if (sparkType instanceof org.apache.spark.sql.types.IntegerType) {
            if (value instanceof Number n) return n.intValue();
            if (value instanceof String s) return Integer.valueOf(s);
            return value;
        }
        if (sparkType instanceof org.apache.spark.sql.types.LongType) {
            if (value instanceof Number n) return n.longValue();
            if (value instanceof String s) return Long.valueOf(s);
            return value;
        }
        if (sparkType instanceof org.apache.spark.sql.types.DoubleType) {
            if (value instanceof Number n) return n.doubleValue();
            if (value instanceof String s) return Double.valueOf(s);
            return value;
        }
        if (sparkType instanceof org.apache.spark.sql.types.FloatType) {
            if (value instanceof Number n) return n.floatValue();
            if (value instanceof String s) return Float.valueOf(s);
            return value;
        }
        if (sparkType instanceof org.apache.spark.sql.types.ShortType) {
            if (value instanceof Number n) return n.shortValue();
            if (value instanceof String s) return Short.valueOf(s);
            return value;
        }
        if (sparkType instanceof org.apache.spark.sql.types.StringType) {
            if (value instanceof String s) {
                if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
                    s = s.substring(1, s.length() - 1);
                }
                if (s.length() >= 2 && s.startsWith("'") && s.endsWith("'")) {
                    s = s.substring(1, s.length() - 1);
                }
                return UTF8String.fromString(s);
            }
            if (value instanceof UTF8String utf8) return utf8;
            return UTF8String.fromString(String.valueOf(value));
        }
        return value;
    }

    private org.apache.spark.sql.catalyst.expressions.Expression toSparkLiteral(Literal lit) {
        Comparable<?> value = lit.getValue();
        if (value == null) {
            return sparkLitNull();
        }
        if (value instanceof Integer i) {
            return sparkLit(i, DataTypes.IntegerType);
        } else if (value instanceof Long l) {
            return sparkLit(l, DataTypes.LongType);
        } else if (value instanceof Float f) {
            return sparkLit(f, DataTypes.FloatType);
        } else if (value instanceof Double d) {
            return sparkLit(d, DataTypes.DoubleType);
        } else if (value instanceof Boolean b) {
            return sparkLit(b, DataTypes.BooleanType);
        } else if (value instanceof String s) {
            return sparkLit(
                    org.apache.spark.unsafe.types.UTF8String.fromString(s), DataTypes.StringType);
        }
        return sparkLit(value.toString(), DataTypes.StringType);
    }

    private static org.apache.spark.sql.catalyst.expressions.Literal sparkLit(
            Object v, DataType dt) {
        return new org.apache.spark.sql.catalyst.expressions.Literal(v, dt);
    }

    private static org.apache.spark.sql.catalyst.expressions.Literal sparkLitNull() {
        return new org.apache.spark.sql.catalyst.expressions.Literal(null, DataTypes.NullType);
    }

    @SuppressWarnings("deprecation")
    private org.apache.spark.sql.catalyst.expressions.Expression toSparkColumnRef(ColumnRef cr) {
        return new UnresolvedAttribute(toScalaSeq(cr.getColumnName()));
    }

    @SuppressWarnings("deprecation")
    private org.apache.spark.sql.catalyst.expressions.Expression toSparkBinaryComparison(
            String op, BinaryExpression be) {
        org.apache.spark.sql.catalyst.expressions.Expression left = toSparkExpression(be.getLeft());
        org.apache.spark.sql.catalyst.expressions.Expression right =
                toSparkExpression(be.getRight());
        if (left == null || right == null) return null;

        return switch (op) {
            case "EqualTo" -> new org.apache.spark.sql.catalyst.expressions.EqualTo(left, right);
            case "NotEqualTo" ->
                    new org.apache.spark.sql.catalyst.expressions.Not(
                            new org.apache.spark.sql.catalyst.expressions.EqualTo(left, right));
            case "GreaterThan" ->
                    new org.apache.spark.sql.catalyst.expressions.GreaterThan(left, right);
            case "GreaterThanOrEqual" ->
                    new org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual(left, right);
            case "LessThan" -> new org.apache.spark.sql.catalyst.expressions.LessThan(left, right);
            case "LessThanOrEqual" ->
                    new org.apache.spark.sql.catalyst.expressions.LessThanOrEqual(left, right);
            default -> null;
        };
    }

    private org.apache.spark.sql.catalyst.expressions.Expression toSparkLogicalAnd(
            LogicalExpression le) {
        org.apache.spark.sql.catalyst.expressions.Expression result = null;
        for (var child : le.children()) {
            org.apache.spark.sql.catalyst.expressions.Expression next = toSparkExpression(child);
            if (next == null) {
                throw new IRAdaptException(
                        "ScanQueryIR",
                        "LogicalExpression",
                        "AND sub-expression adaptation failed, cannot safely drop filter conditions");
            }
            result =
                    (result == null)
                            ? next
                            : new org.apache.spark.sql.catalyst.expressions.And(result, next);
        }
        return result;
    }

    private org.apache.spark.sql.catalyst.expressions.Expression toSparkLogicalOr(
            LogicalExpression le) {
        org.apache.spark.sql.catalyst.expressions.Expression result = null;
        for (var child : le.children()) {
            org.apache.spark.sql.catalyst.expressions.Expression next = toSparkExpression(child);
            if (next == null) {
                throw new IRAdaptException(
                        "ScanQueryIR",
                        "LogicalExpression",
                        "OR sub-expression adaptation failed, cannot safely drop filter conditions");
            }
            result =
                    (result == null)
                            ? next
                            : new org.apache.spark.sql.catalyst.expressions.Or(result, next);
        }
        return result;
    }

    private org.apache.spark.sql.catalyst.expressions.Expression toSparkNot(
            LogicalExpression logical) {
        List<io.github.rawwise.model.expression.Expression> children = logical.children();
        if (children.isEmpty()) return null;
        org.apache.spark.sql.catalyst.expressions.Expression child =
                toSparkExpression(children.get(0));
        if (child == null) return null;
        return new org.apache.spark.sql.catalyst.expressions.Not(child);
    }

    private org.apache.spark.sql.catalyst.expressions.Expression toSparkIsNull(IsNull isNull) {
        org.apache.spark.sql.catalyst.expressions.Expression col =
                toSparkExpression(isNull.getColumn());
        if (col == null) return null;
        return new org.apache.spark.sql.catalyst.expressions.IsNull(col);
    }

    private org.apache.spark.sql.catalyst.expressions.Expression toSparkIsNotNull(
            IsNotNull isNotNull) {
        org.apache.spark.sql.catalyst.expressions.Expression col =
                toSparkExpression(isNotNull.getColumn());
        if (col == null) return null;
        return new org.apache.spark.sql.catalyst.expressions.IsNotNull(col);
    }

    private org.apache.spark.sql.catalyst.plans.JoinType toSparkJoinType(
            io.github.rawwise.frontend.ir.JoinType type) {
        return switch (type) {
            case INNER -> org.apache.spark.sql.catalyst.plans.Inner$.MODULE$;
            case LEFT_OUTER -> org.apache.spark.sql.catalyst.plans.LeftOuter$.MODULE$;
            case RIGHT_OUTER -> org.apache.spark.sql.catalyst.plans.RightOuter$.MODULE$;
            case FULL_OUTER -> org.apache.spark.sql.catalyst.plans.FullOuter$.MODULE$;
            case CROSS -> org.apache.spark.sql.catalyst.plans.Cross$.MODULE$;
            case LEFT_SEMI -> org.apache.spark.sql.catalyst.plans.LeftSemi$.MODULE$;
            case LEFT_ANTI -> org.apache.spark.sql.catalyst.plans.LeftAnti$.MODULE$;
        };
    }

    org.apache.spark.sql.types.StructType toSparkStructType(StructType structType) {
        return toSparkStructTypeStatic(structType);
    }

    private StructField toSparkStructField(io.github.rawwise.model.record.StructField field) {
        return toSparkStructFieldStatic(field);
    }

    private DataType toSparkDataType(RawWiseType type) {
        return toSparkDataTypeStatic(type);
    }

    @SuppressWarnings("deprecation")
    private LogicalPlan applyFilter(
            LogicalPlan plan, io.github.rawwise.model.expression.Expression filter) {
        if (filter == null) return plan;
        org.apache.spark.sql.catalyst.expressions.Expression cond = toSparkExpression(filter);
        if (cond == null) {
            throw new IRAdaptException(
                    "ScanQueryIR",
                    "filter",
                    "Filter expression adaptation returned null, cannot safely drop filter");
        }
        return new Filter(cond, plan);
    }

    @SuppressWarnings("deprecation")
    private LogicalPlan buildProjection(
            LogicalPlan plan,
            java.util.List<io.github.rawwise.frontend.ir.ProjectionColumn> projection) {
        if (projection == null || projection.isEmpty()) return plan;
        Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> projList =
                toSeq(
                        projection.stream()
                                .map(
                                        pc -> {
                                            org.apache.spark.sql.catalyst.expressions.Expression
                                                    colRef =
                                                            new UnresolvedAttribute(
                                                                    toScalaSeq(pc.name()));
                                            if (pc.alias() != null && !pc.alias().isEmpty()) {
                                                return (org.apache.spark.sql.catalyst.expressions
                                                                .NamedExpression)
                                                        new org.apache.spark.sql.catalyst
                                                                .expressions.Alias(
                                                                colRef,
                                                                pc.alias(),
                                                                org.apache.spark.sql.catalyst
                                                                        .expressions.Alias
                                                                        .$lessinit$greater$default$3(
                                                                                colRef, pc.alias()),
                                                                org.apache.spark.sql.catalyst
                                                                        .expressions.Alias
                                                                        .$lessinit$greater$default$4(
                                                                                colRef, pc.alias()),
                                                                org.apache.spark.sql.catalyst
                                                                        .expressions.Alias
                                                                        .$lessinit$greater$default$5(
                                                                                colRef, pc.alias()),
                                                                org.apache.spark.sql.catalyst
                                                                        .expressions.Alias
                                                                        .$lessinit$greater$default$6(
                                                                                colRef,
                                                                                pc.alias()));
                                            }
                                            return (org.apache.spark.sql.catalyst.expressions
                                                            .NamedExpression)
                                                    colRef;
                                        })
                                .collect(Collectors.toList()));
        return new Project(projList, plan);
    }

    private LogicalPlan relation(TableMetaIR meta) {
        String basePath = io.github.rawwise.governance.config.WarehousePathResolver.resolve();
        String storeId = meta.properties().getOrDefault("store-id", "spark");
        org.apache.spark.sql.types.StructType sparkSchema = toSparkStructTypeStatic(meta.schema());

        String[] parts = meta.tableIdentifier().split("\\.");
        String databaseName = parts.length >= 2 ? parts[parts.length - 2] : "default";
        String tableName = parts[parts.length - 1];
        String normalizedTableIdentifier = databaseName + "." + tableName;

        io.github.rawwise.model.record.TableMeta tableMeta =
                io.github.rawwise.model.record.TableMeta.builder()
                        .withTableId(meta.tableId())
                        .withDatabaseName(databaseName)
                        .withTableName(tableName)
                        .withTableType(
                                io.github.rawwise.model.record.TableMeta.TableType.PRIMARY_KEY)
                        .withSchema(new io.github.rawwise.model.record.Schema(meta.schema()))
                        .withPrimaryKey(
                                io.github.rawwise.model.record.PrimaryKey.of(
                                        meta.primaryKeyColumns()))
                        .withProperties(meta.properties())
                        .build();
        RawWiseSparkTable table =
                new RawWiseSparkTable(
                        basePath, storeId, normalizedTableIdentifier, sparkSchema, tableMeta);
        return DataSourceV2Relation.create(table, Option.empty(), Option.empty());
    }

    @SuppressWarnings("unchecked")
    private LogicalPlan emptyRelation(TableMetaIR meta) {
        LogicalPlan relation = relation(meta);
        scala.collection.immutable.Seq<org.apache.spark.sql.catalyst.InternalRow> emptyRows =
                (scala.collection.immutable.Seq<org.apache.spark.sql.catalyst.InternalRow>)
                        (scala.collection.immutable.Seq<?>) scala.collection.immutable.Nil$.MODULE$;
        return new LocalRelation(relation.output(), emptyRows, false, scala.Option.apply(null));
    }

    private static org.apache.spark.sql.types.StructType toSparkStructTypeStatic(
            StructType structType) {
        List<StructField> sparkFields =
                structType.fields().stream()
                        .map(SparkIRAdaptor::toSparkStructFieldStatic)
                        .collect(Collectors.toList());
        return new org.apache.spark.sql.types.StructType(sparkFields.toArray(new StructField[0]));
    }

    private static StructField toSparkStructFieldStatic(
            io.github.rawwise.model.record.StructField field) {
        DataType sparkType = toSparkDataTypeStatic(field.dataType());
        return new StructField(
                field.name(),
                sparkType,
                field.dataType().isNullable(),
                org.apache.spark.sql.types.Metadata.empty());
    }

    @SuppressWarnings("unchecked")
    private static DataType toSparkDataTypeStatic(RawWiseType type) {
        String simpleName = type.getClass().getSimpleName();
        return switch (simpleName) {
            case "NullType" -> DataTypes.NullType;
            case "BooleanType" -> DataTypes.BooleanType;
            case "Int8Type" -> DataTypes.ByteType;
            case "Int32Type" -> DataTypes.IntegerType;
            case "Int64Type" -> DataTypes.LongType;
            case "Float32Type" -> DataTypes.FloatType;
            case "Float64Type" -> DataTypes.DoubleType;
            case "StringType" -> DataTypes.StringType;
            case "BinaryType" -> DataTypes.BinaryType;
            case "DateType" -> DataTypes.DateType;
            case "TimestampType" -> DataTypes.TimestampType;
            case "DecimalType" -> {
                try {
                    var precisionMethod = type.getClass().getMethod("precision");
                    var scaleMethod = type.getClass().getMethod("scale");
                    int precision = (int) precisionMethod.invoke(type);
                    int scale = (int) scaleMethod.invoke(type);
                    yield DataTypes.createDecimalType(precision, scale);
                } catch (Exception e) {
                    yield DataTypes.createDecimalType(10, 0);
                }
            }
            case "ArrayType" -> {
                try {
                    var elementTypeMethod = type.getClass().getMethod("elementType");
                    RawWiseType elementType = (RawWiseType) elementTypeMethod.invoke(type);
                    yield DataTypes.createArrayType(toSparkDataTypeStatic(elementType));
                } catch (Exception e) {
                    yield DataTypes.createArrayType(DataTypes.NullType);
                }
            }
            case "MapType" -> {
                try {
                    var keyTypeMethod = type.getClass().getMethod("keyType");
                    var valueTypeMethod = type.getClass().getMethod("valueType");
                    RawWiseType keyType = (RawWiseType) keyTypeMethod.invoke(type);
                    RawWiseType valueType = (RawWiseType) valueTypeMethod.invoke(type);
                    yield DataTypes.createMapType(
                            toSparkDataTypeStatic(keyType), toSparkDataTypeStatic(valueType));
                } catch (Exception e) {
                    yield DataTypes.createMapType(DataTypes.NullType, DataTypes.NullType);
                }
            }
            case "StructType" -> toSparkStructTypeStatic((StructType) type);
            default ->
                    throw new UnsupportedOperationException(
                            "Unsupported RawWiseType: " + simpleName);
        };
    }

    @SuppressWarnings("deprecation")
    private Seq<String> toScalaSeq(String singleElement) {
        return scala.collection.immutable.List.from(
                JavaConverters.asScalaBufferConverter(List.of(singleElement)).asScala());
    }

    @SuppressWarnings("deprecation")
    private Seq<String> toScalaSeq(String[] elements) {
        return scala.collection.immutable.List.from(
                JavaConverters.asScalaBufferConverter(java.util.Arrays.asList(elements)).asScala());
    }

    @SuppressWarnings("deprecation")
    private <T> Seq<T> toSeq(List<T> javaList) {
        return scala.collection.immutable.List.from(
                JavaConverters.asScalaBufferConverter(new ArrayList<>(javaList)).asScala());
    }
}
