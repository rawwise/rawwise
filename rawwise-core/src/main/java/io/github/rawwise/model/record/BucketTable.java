package io.github.rawwise.model.record;

import java.util.List;
import java.util.Map;

public interface BucketTable extends RawWiseTable {

    PartitionSpec partitionSpec();

    default int numBuckets() {
        return partitionSpec().numBuckets();
    }

    default int numPartitionLevels() {
        return partitionSpec().numLevels();
    }

    default boolean isSingleBucketHidden() {
        return partitionSpec().isSingleBucketHidden();
    }

    int bucket(Map<String, Object> partitionValues);

    List<String> partitionPaths();

    Map<String, Object> parsePartitionPath(String path);
}
