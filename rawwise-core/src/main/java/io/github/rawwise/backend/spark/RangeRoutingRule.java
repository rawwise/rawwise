package io.github.rawwise.backend.spark;

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

public final class RangeRoutingRule extends Rule<LogicalPlan> {
    private final SparkSession sparkSession;

    public RangeRoutingRule(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        SparkConf conf = sparkSession.sparkContext().conf();
        int shardCount = RawWiseDataSource.resolveShardCount(conf);
        if (shardCount <= 1 || !(plan instanceof AppendData appendData)) {
            return plan;
        }
        return shouldRewrite(appendData) ? rewrite(appendData, shardCount) : plan;
    }

    private static boolean shouldRewrite(AppendData appendData) {
        return appendData.table() instanceof DataSourceV2Relation relation
                && RawWiseDataSource.isRawWiseTable(relation.table())
                && findKeyAttribute(appendData.query()) != null
                && !hasCompatibleRepartition(appendData.query());
    }

    private static LogicalPlan rewrite(AppendData appendData, int shardCount) {
        Attribute keyAttribute = findKeyAttribute(appendData.query());
        if (keyAttribute == null) {
            return appendData;
        }

        RepartitionByExpression repartition =
                new RepartitionByExpression(
                        toSeq(List.of(keyAttribute)),
                        appendData.query(),
                        Option.apply(shardCount),
                        Option.empty());
        return appendData.withNewQuery(repartition);
    }

    static boolean isRawWiseRelation(NamedRelation relation) {
        return relation instanceof DataSourceV2Relation dataSourceV2Relation
                && RawWiseDataSource.isRawWiseTable(dataSourceV2Relation.table());
    }

    static Attribute findKeyAttribute(LogicalPlan plan) {
        for (Attribute attribute : CollectionConverters.asJava(plan.output())) {
            if (RawWiseDataSource.KEY_COLUMN.equalsIgnoreCase(attribute.name())) {
                return attribute;
            }
        }
        return null;
    }

    static boolean hasCompatibleRepartition(LogicalPlan plan) {
        if (!(plan instanceof RepartitionByExpression repartition)) {
            return false;
        }
        return CollectionConverters.asJava(repartition.partitionExpressions()).stream()
                .filter(Attribute.class::isInstance)
                .map(Attribute.class::cast)
                .anyMatch(
                        attribute ->
                                RawWiseDataSource.KEY_COLUMN.equalsIgnoreCase(attribute.name()));
    }

    @SuppressWarnings("unchecked")
    private static Seq<Expression> toSeq(List<Attribute> attributes) {
        Seq<Expression> seq =
                (Seq<Expression>) (Seq<?>) CollectionConverters.asScala(attributes).toSeq();
        return seq;
    }
}
