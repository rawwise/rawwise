package io.github.rawwise.engine.storage;

public enum QueryCapability {
    POINT_LOOKUP,
    RANGE_SCAN,
    FULL_READ,
    PREDICATE_PUSHDOWN,
    COLUMN_PROJECTION,
    STATISTICS_BASED_PRUNING,
    LIMIT_PUSHDOWN,
    PARTITION_PRUNING
}
