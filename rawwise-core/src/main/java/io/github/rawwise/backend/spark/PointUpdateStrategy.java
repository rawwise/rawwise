package io.github.rawwise.backend.spark;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkStrategy;
import scala.collection.immutable.Seq;

public class PointUpdateStrategy extends SparkStrategy {

    @SuppressWarnings("unchecked")
    private static <T> Seq<T> asScalaSeq(java.util.List<T> javaList) {
        return scala.collection.immutable.List.from(
                scala.jdk.javaapi.CollectionConverters.asScala(javaList).toSeq());
    }

    @Override
    public Seq<SparkPlan> apply(LogicalPlan plan) {
        if (plan instanceof PointUpdateCommand cmd) {
            return asScalaSeq(java.util.List.of((SparkPlan) new PointUpdateExec(cmd)));
        }
        return asScalaSeq(java.util.List.<SparkPlan>of());
    }
}
