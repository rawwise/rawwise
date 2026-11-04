package io.github.rawwise.backend.spark;

import java.util.List;
import java.util.Map;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.SparkPlan;
import scala.reflect.ClassTag$;

public final class RawWisePreferredLocationExec extends SparkPlan {
    private final SparkPlan child;
    private final Map<Integer, List<String>> preferredLocations;

    public RawWisePreferredLocationExec(
            SparkPlan child, Map<Integer, List<String>> preferredLocations) {
        this.child = child;
        this.preferredLocations = preferredLocations;
    }

    @Override
    public scala.collection.immutable.Seq<Attribute> output() {
        return child.output();
    }

    @Override
    public org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning() {
        return child.outputPartitioning();
    }

    @Override
    public RDD<InternalRow> doExecute() {
        return new RawWisePreferredLocationRDD<>(
                child.execute(), preferredLocations, ClassTag$.MODULE$.apply(InternalRow.class));
    }

    @Override
    public scala.collection.immutable.Seq<SparkPlan> children() {
        return scala.collection.immutable.List.from(
                scala.jdk.javaapi.CollectionConverters.asScala(List.of(child)));
    }

    @Override
    public SparkPlan withNewChildrenInternal(
            scala.collection.immutable.IndexedSeq<SparkPlan> newChildren) {
        SparkPlan only = newChildren.apply(0);
        return new RawWisePreferredLocationExec(only, preferredLocations);
    }

    @Override
    public int productArity() {
        return 2;
    }

    @Override
    public Object productElement(int n) {
        return switch (n) {
            case 0 -> child;
            case 1 -> preferredLocations;
            default -> throw new IndexOutOfBoundsException(Integer.toString(n));
        };
    }

    @Override
    public boolean canEqual(Object that) {
        return that instanceof RawWisePreferredLocationExec;
    }

    @Override
    public String productPrefix() {
        return "RawWisePreferredLocationExec";
    }
}
