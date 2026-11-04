package io.github.rawwise.backend.spark;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec;

public final class RawWisePreferredLocationRule extends Rule<SparkPlan> {
    private final SparkSession sparkSession;

    public RawWisePreferredLocationRule(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public SparkPlan apply(SparkPlan plan) {
        if (!(plan instanceof AppendDataExec appendDataExec)) {
            return plan;
        }
        if (!RawWiseDataSource.isRawWiseWrite(appendDataExec.write())) {
            return plan;
        }

        int shardCount = RawWiseDataSource.resolveShardCount(sparkSession.sparkContext().conf());
        if (shardCount <= 1) {
            return plan;
        }

        RawWiseWriteTopology topology = RawWiseWriteTopology.inspect(sparkSession.sparkContext());
        if (!topology.supportsPreferredLocations()) {
            return plan;
        }

        if (appendDataExec.query().outputPartitioning().numPartitions() != shardCount) {
            return plan;
        }

        String driverHost = sparkSession.sparkContext().conf().get("spark.driver.host", "");
        Map<Integer, List<String>> preferredLocations =
                topology.preferredLocationsByShard(shardCount, driverHost);
        if (preferredLocations.isEmpty()) {
            return plan;
        }

        SparkPlan newChild =
                new RawWisePreferredLocationExec(appendDataExec.child(), preferredLocations);
        return appendDataExec.withNewChildInternal(newChild);
    }
}
