package io.github.rawwise.backend.spark;

import java.util.List;
import java.util.Map;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

public final class RawWisePreferredLocationRDD<T> extends RDD<T> {
    private final RDD<T> parent;
    private final Map<Integer, List<String>> preferredLocationsByPartition;

    public RawWisePreferredLocationRDD(
            RDD<T> parent,
            Map<Integer, List<String>> preferredLocationsByPartition,
            ClassTag<T> evidence) {
        super(parent, evidence);
        this.parent = parent;
        this.preferredLocationsByPartition = preferredLocationsByPartition;
    }

    @Override
    public scala.collection.Iterator<T> compute(Partition split, TaskContext context) {
        return parent.iterator(split, context);
    }

    @Override
    public Partition[] getPartitions() {
        return parent.partitions();
    }

    @Override
    public Seq<String> getPreferredLocations(Partition split) {
        List<String> locs = preferredLocationsByPartition.get(split.index());
        if (locs == null || locs.isEmpty()) {
            return parent.preferredLocations(split);
        }
        return JavaConverters.asScalaIteratorConverter(locs.iterator()).asScala().toSeq();
    }
}
