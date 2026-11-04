package io.github.rawwise.backend.spark;

import io.github.rawwise.backend.SparkExpressionConverter;
import io.github.rawwise.backend.lazyupdate.LazyUpdateBridge;
import io.github.rawwise.backend.lazyupdate.LazyUpdateWriteInterceptor;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionUtils;
import io.github.rawwise.model.expression.Literal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;

public final class LazyUpdateRewriteRule extends Rule<LogicalPlan> {

    private static final Logger log = LoggerFactory.getLogger(LazyUpdateRewriteRule.class);

    private static final String POINT_UPDATE_ENABLED_KEY = "spark.rawwise.pointUpdate.enabled";

    private final SparkSession sparkSession;

    public LazyUpdateRewriteRule(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (!(plan instanceof UpdateTable updateTable)) {
            return plan;
        }

        LogicalPlan tablePlan = updateTable.table();
        if (!isRawWiseRelation(tablePlan)) {
            log.debug("LazyUpdateRewriteRule: target table is not a RawWise table, skipping");
            return plan;
        }

        Option<org.apache.spark.sql.catalyst.expressions.Expression> conditionOpt =
                updateTable.condition();
        if (conditionOpt.isEmpty()) {
            log.warn(
                    "LazyUpdateRewriteRule: UPDATE without WHERE clause detected on RawWise table. "
                            + "Full-table range update will be intercepted.");
            return interceptRange(updateTable, null);
        }

        org.apache.spark.sql.catalyst.expressions.Expression sparkCondition = conditionOpt.get();

        Expression rawwiseCondition = SparkExpressionConverter.convert(sparkCondition);
        if (rawwiseCondition == null) {
            log.warn(
                    "LazyUpdateRewriteRule: failed to convert condition, proceeding with plan: {}",
                    sparkCondition);
            return plan;
        }

        DataSourceV2Relation relation = (DataSourceV2Relation) tablePlan;
        Set<String> primaryKeys = extractPrimaryKeys(relation);

        if (primaryKeys.isEmpty()) {
            log.info(
                    "LazyUpdateRewriteRule: table has no primary key, intercepting as range update");
            return interceptRange(updateTable, rawwiseCondition);
        }

        boolean isPkUpdate = ExpressionUtils.isPrimaryKeyEquals(rawwiseCondition, primaryKeys);
        String updateType = isPkUpdate ? "PK_EQUALITY" : "RANGE";

        logUpdate(updateTable, rawwiseCondition, updateType);

        if (isPkUpdate) {
            if (isPointUpdateEnabled(sparkSession)) {
                try {
                    Map<String, Comparable<?>> pkValues =
                            ExpressionUtils.extractPrimaryKeyValues(rawwiseCondition, primaryKeys);
                    Map<String, Comparable<?>> updateValues = extractAssignments(updateTable);
                    String tableId = resolveTableId(updateTable);

                    PointUpdateCommand cmd =
                            new PointUpdateCommand(
                                    tableId, pkValues, updateValues, extractSchema(relation));

                    writePointUpdateLog(tableId, rawwiseCondition, updateValues);

                    return cmd;
                } catch (Exception e) {
                    log.warn(
                            "PointUpdateCommand creation failed, falling back to UpdateLog path",
                            e);
                }
            }
            return interceptRange(updateTable, rawwiseCondition);
        }
        return interceptRange(updateTable, rawwiseCondition);
    }

    private LogicalPlan interceptRange(UpdateTable updateTable, Expression condition) {
        LazyUpdateWriteInterceptor interceptor = LazyUpdateBridge.interceptor();
        if (interceptor == null) {
            throw new IllegalStateException(
                    "LazyUpdateBridge not installed. Range update for table="
                            + resolveTableId(updateTable)
                            + " cannot be intercepted. "
                            + "Ensure LazyUpdateBridge is installed before executing UPDATE statements.");
        }

        String tableId = resolveTableId(updateTable);
        Map<String, Comparable<?>> updates = extractAssignments(updateTable);
        Expression effectiveCondition = condition != null ? condition : new Literal(true);

        try {
            interceptor.interceptRangeUpdate(tableId, effectiveCondition, updates, 0L);
            log.info(
                    "Range update intercepted for table={}, stored as lazy UpdateLog entry",
                    tableId);
        } catch (Exception e) {
            log.error(
                    "Failed to intercept range update for table={}: {}",
                    tableId,
                    e.getMessage(),
                    e);
            throw new RuntimeException("LazyUpdate range update failed for table=" + tableId, e);
        }

        @SuppressWarnings("unchecked")
        scala.collection.immutable.Seq<org.apache.spark.sql.catalyst.InternalRow> emptyRows =
                (scala.collection.immutable.Seq<org.apache.spark.sql.catalyst.InternalRow>)
                        (scala.collection.immutable.Seq<?>) scala.collection.immutable.Nil$.MODULE$;
        return new LocalRelation(updateTable.output(), emptyRows, false, scala.Option.apply(null));
    }

    private void writePointUpdateLog(
            String tableId, Expression predicate, Map<String, Comparable<?>> updates) {
        LazyUpdateWriteInterceptor interceptor = LazyUpdateBridge.interceptor();
        if (interceptor == null) {
            throw new IllegalStateException(
                    "LazyUpdateBridge not installed. Point update for table="
                            + tableId
                            + " cannot be persisted. Ensure LazyUpdateBridge is installed before enabling "
                            + "spark.rawwise.pointUpdate.enabled");
        }
        try {
            interceptor.interceptRangeUpdate(tableId, predicate, updates, 0L);
            log.info("Point update intercepted for table={}, stored as UpdateLog entry", tableId);
        } catch (java.io.IOException e) {
            throw new RuntimeException(
                    "Failed to write UpdateLog for point update on table=" + tableId, e);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Map<String, Comparable<?>> extractAssignments(UpdateTable updateTable) {
        Map<String, Comparable<?>> result = new HashMap<>();
        scala.collection.Seq<?> assignments = updateTable.assignments();
        java.util.List<?> list = JavaConverters.seqAsJavaList(assignments);
        for (Object expr : list) {
            if (expr instanceof org.apache.spark.sql.catalyst.expressions.Alias alias) {
                String colName = alias.name();
                Object child = alias.child();
                if (child instanceof org.apache.spark.sql.catalyst.expressions.Literal lit) {
                    Object litValue = lit.value();
                    if (litValue instanceof Comparable) {
                        result.put(colName, (Comparable<?>) litValue);
                    } else if (litValue != null) {
                        result.put(colName, String.valueOf(litValue));
                    }
                }
            }
        }
        return result;
    }

    private static String resolveTableId(UpdateTable updateTable) {
        LogicalPlan tablePlan = updateTable.table();
        if (tablePlan instanceof DataSourceV2Relation relation) {
            return relation.table().name();
        }
        return "unknown";
    }

    private static boolean isRawWiseRelation(LogicalPlan plan) {
        if (plan instanceof DataSourceV2Relation relation) {
            return RawWiseDataSource.isRawWiseTable(relation.table());
        }
        return false;
    }

    private static Set<String> extractPrimaryKeys(DataSourceV2Relation relation) {
        Set<String> pks = new HashSet<>(RawWiseDataSource.primaryKeyColumns(relation.table()));
        if (pks.isEmpty()) {
            pks.add(RawWiseDataSource.KEY_COLUMN);
        }
        return pks;
    }

    private static void logUpdate(
            UpdateTable updateTable, Expression condition, String updateType) {
        int assignmentCount = updateTable.assignments().size();
        String conditionStr = condition != null ? condition.toString() : "NONE";

        log.info(
                "LazyUpdateRewriteRule: detected UPDATE type={}, assignments={}, condition={}",
                updateType,
                assignmentCount,
                conditionStr);
    }

    private static boolean isPointUpdateEnabled(SparkSession session) {
        return session.conf().get(POINT_UPDATE_ENABLED_KEY, "false").equals("true");
    }

    private static StructType extractSchema(DataSourceV2Relation relation) {
        return relation.schema();
    }
}
