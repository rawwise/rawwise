package io.github.rawwise.backend.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.LeafExecNode;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import scala.collection.Iterator;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag$;

@Slf4j
public final class PointUpdateExec extends SparkPlan implements LeafExecNode {

    private static final io.micrometer.core.instrument.Timer POINT_UPDATE_LATENCY =
            io.micrometer.core.instrument.Metrics.timer("rawwise.point_update", "phase", "execute");

    private final String tableIdentifier;
    private final String primaryKeyValuesJson;
    private final String updatesJson;

    public PointUpdateExec(PointUpdateCommand cmd) {
        this.tableIdentifier = cmd.tableIdentifier();
        this.primaryKeyValuesJson = cmd.primaryKeyValuesJson();
        this.updatesJson = cmd.updatesJson();
    }

    public String tableIdentifier() {
        return tableIdentifier;
    }

    public String primaryKeyValuesJson() {
        return primaryKeyValuesJson;
    }

    public String updatesJson() {
        return updatesJson;
    }

    @SuppressWarnings("unchecked")
    private static <T> Seq<T> asScalaSeq(java.util.List<T> javaList) {
        return scala.collection.immutable.List.from(
                scala.jdk.javaapi.CollectionConverters.asScala(javaList).toSeq());
    }

    @Override
    public Seq<Attribute> output() {
        return asScalaSeq(
                java.util.List.of(
                        (Attribute)
                                new AttributeReference(
                                        "num_affected_rows",
                                        LongType$.MODULE$,
                                        false,
                                        Metadata.empty(),
                                        NamedExpression.newExprId(),
                                        asScalaSeq(java.util.List.<String>of()))));
    }

    @Override
    public RDD<InternalRow> doExecute() {
        io.micrometer.core.instrument.Timer.Sample sample =
                io.micrometer.core.instrument.Timer.start();
        try {
            log.info(
                    "PointUpdateExec: point update already persisted on Driver side for table={}, returning affected_rows=1",
                    tableIdentifier);

            return sparkContext()
                    .parallelize(
                            asScalaSeq(java.util.List.of(0)),
                            1,
                            ClassTag$.MODULE$.apply(Integer.class))
                    .mapPartitions(
                            new PointUpdateResultFunc(),
                            true,
                            ClassTag$.MODULE$.apply(InternalRow.class));
        } finally {
            sample.stop(POINT_UPDATE_LATENCY);
        }
    }

    @Override
    public Seq<SparkPlan> children() {
        return asScalaSeq(java.util.List.<SparkPlan>of());
    }

    @Override
    public SparkPlan withNewChildrenInternal(IndexedSeq<SparkPlan> newChildren) {
        return this;
    }

    @Override
    public Object productElement(int n) {
        return switch (n) {
            case 0 -> tableIdentifier;
            case 1 -> primaryKeyValuesJson;
            case 2 -> updatesJson;
            default -> throw new IndexOutOfBoundsException(Integer.toString(n));
        };
    }

    @Override
    public int productArity() {
        return 3;
    }

    @Override
    public boolean canEqual(Object that) {
        return that instanceof PointUpdateExec;
    }

    @Override
    public String productPrefix() {
        return "PointUpdateExec";
    }

    private static final class PointUpdateResultFunc
            extends scala.runtime.AbstractFunction1<Iterator<Integer>, Iterator<InternalRow>>
            implements java.io.Serializable {

        @Override
        public Iterator<InternalRow> apply(Iterator<Integer> v1) {
            UnsafeRow row = new UnsafeRow(1);
            byte[] bytes = new byte[8];
            row.pointTo(bytes, 8);
            row.setLong(0, 1L);
            return scala.collection.JavaConverters.asScalaIterator(
                    java.util.Collections.singletonList((InternalRow) row).iterator());
        }
    }
}
