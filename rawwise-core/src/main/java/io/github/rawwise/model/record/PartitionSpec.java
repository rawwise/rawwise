package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.*;

public class PartitionSpec implements Serializable {

    public enum Transform {
        IDENTITY,
        HASH,
        RANGE,
        VALUE
    }

    public record BucketSpec(
            String sourceColumn, Transform transform, int numBuckets, boolean hidden)
            implements Serializable {

        public static BucketSpec identity(boolean hidden) {
            return new BucketSpec(null, Transform.IDENTITY, 1, hidden);
        }

        public static BucketSpec hash(String column, int numBuckets) {
            return new BucketSpec(column, Transform.HASH, numBuckets, false);
        }

        public static BucketSpec range(String column) {
            return new BucketSpec(column, Transform.RANGE, 0, false);
        }

        public static BucketSpec value(String column) {
            return new BucketSpec(column, Transform.VALUE, 0, false);
        }

        public boolean isIdentity() {
            return transform == Transform.IDENTITY;
        }
    }

    private final List<BucketSpec> bucketSpecs;

    private PartitionSpec(Builder builder) {
        this.bucketSpecs = Collections.unmodifiableList(new ArrayList<>(builder.bucketSpecs));
    }

    public List<BucketSpec> bucketSpecs() {
        return bucketSpecs;
    }

    public boolean isPartitioned() {
        if (bucketSpecs.isEmpty()) return false;
        if (bucketSpecs.size() == 1 && bucketSpecs.get(0).isIdentity()) {
            return !bucketSpecs.get(0).hidden();
        }
        return true;
    }

    public boolean isUnpartitioned() {
        return !isPartitioned();
    }

    public int numLevels() {
        return bucketSpecs.size();
    }

    public int numBuckets() {
        if (bucketSpecs.isEmpty()) return 1;
        int total = 1;
        for (BucketSpec spec : bucketSpecs) {
            total *= Math.max(spec.numBuckets(), 1);
        }
        return total;
    }

    public Optional<BucketSpec> firstLevel() {
        return bucketSpecs.isEmpty() ? Optional.empty() : Optional.of(bucketSpecs.get(0));
    }

    public boolean isSingleBucketHidden() {
        return bucketSpecs.size() == 1
                && bucketSpecs.get(0).isIdentity()
                && bucketSpecs.get(0).hidden();
    }

    public static PartitionSpec unpartitioned(boolean hideSingleBucket) {
        return builder().withBucketSpec(BucketSpec.identity(hideSingleBucket)).build();
    }

    public static PartitionSpec unpartitioned() {
        return unpartitioned(true);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(PartitionSpec spec) {
        return new Builder(spec);
    }

    @Override
    public String toString() {
        return "PartitionSpec{levels=" + numLevels() + ", specs=" + bucketSpecs + "}";
    }

    public static class Builder {
        private final List<BucketSpec> bucketSpecs = new ArrayList<>();

        public Builder() {}

        public Builder(PartitionSpec spec) {
            this.bucketSpecs.addAll(spec.bucketSpecs);
        }

        public Builder withBucketSpec(BucketSpec spec) {
            this.bucketSpecs.add(spec);
            return this;
        }

        public Builder withHashBucket(String column, int numBuckets) {
            return withBucketSpec(BucketSpec.hash(column, numBuckets));
        }

        public Builder withRangePartition(String column) {
            return withBucketSpec(BucketSpec.range(column));
        }

        public Builder withValuePartition(String column) {
            return withBucketSpec(BucketSpec.value(column));
        }

        public Builder withIdentityBucket(boolean hidden) {
            return withBucketSpec(BucketSpec.identity(hidden));
        }

        public PartitionSpec build() {
            if (bucketSpecs.isEmpty()) {
                bucketSpecs.add(BucketSpec.identity(true));
            }
            return new PartitionSpec(this);
        }
    }
}
