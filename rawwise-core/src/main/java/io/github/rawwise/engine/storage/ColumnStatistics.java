package io.github.rawwise.engine.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

public record ColumnStatistics(Map<String, ColumnStat> columns) implements Serializable {

    public ColumnStatistics {
        columns = columns == null ? Map.of() : Map.copyOf(columns);
    }

    public ColumnStat getColumn(String columnName) {
        return columns.get(columnName);
    }

    public static final ColumnStatistics EMPTY = new ColumnStatistics(Map.of());

    public ColumnStatistics merge(ColumnStatistics other) {
        if (other == null || other.columns.isEmpty()) return this;
        if (this.columns.isEmpty()) return other;
        Map<String, ColumnStat> merged = new java.util.LinkedHashMap<>(this.columns);
        for (var entry : other.columns.entrySet()) {
            ColumnStat existing = merged.get(entry.getKey());
            merged.put(
                    entry.getKey(),
                    existing == null ? entry.getValue() : existing.merge(entry.getValue()));
        }
        return new ColumnStatistics(merged);
    }

    public record ColumnStat(
            long nullCount,
            long valueCount,
            byte[] minBytes,
            byte[] maxBytes,
            String logicalType,
            long sizeBytes)
            implements Serializable {

        public byte[] minBytes() {
            return minBytes.clone();
        }

        public byte[] maxBytes() {
            return maxBytes.clone();
        }

        public ColumnStat {
            minBytes = minBytes == null ? new byte[0] : minBytes.clone();
            maxBytes = maxBytes == null ? new byte[0] : maxBytes.clone();
            logicalType = logicalType == null ? "UNKNOWN" : logicalType;
        }

        public static ColumnStat fromColumnChunkMetaData(
                org.apache.parquet.hadoop.metadata.ColumnChunkMetaData md) {
            org.apache.parquet.column.statistics.Statistics<?> stats = md.getStatistics();
            String logicalType =
                    md.getPrimitiveType().getLogicalTypeAnnotation() != null
                            ? md.getPrimitiveType().getLogicalTypeAnnotation().toString()
                            : md.getPrimitiveType().getPrimitiveTypeName().name();
            return new ColumnStat(
                    stats == null ? 0 : stats.getNumNulls(),
                    md.getValueCount(),
                    stats != null && stats.hasNonNullValue() ? stats.getMinBytes() : new byte[0],
                    stats != null && stats.hasNonNullValue() ? stats.getMaxBytes() : new byte[0],
                    logicalType,
                    md.getTotalSize());
        }

        /**
         * Merges this ColumnStat with another. Note: unsigned byte comparison is correct for BINARY
         * types but may not be correct for signed integer types. For signed types, callers should
         * use Parquet's native Statistics.merge() instead.
         */
        public ColumnStat merge(ColumnStat other) {
            if (!this.logicalType.equals(other.logicalType)) {
                throw new IllegalArgumentException(
                        "Cannot merge ColumnStat with different logicalType: "
                                + this.logicalType
                                + " vs "
                                + other.logicalType);
            }
            byte[] mergedMin = mergeMin(this.minBytes, other.minBytes);
            byte[] mergedMax = mergeMax(this.maxBytes, other.maxBytes);
            return new ColumnStat(
                    this.nullCount + other.nullCount,
                    this.valueCount + other.valueCount,
                    mergedMin,
                    mergedMax,
                    this.logicalType,
                    this.sizeBytes + other.sizeBytes);
        }

        private static byte[] mergeMin(byte[] a, byte[] b) {
            if (a.length == 0) return b;
            if (b.length == 0) return a;
            return Arrays.compareUnsigned(a, b) <= 0 ? a : b;
        }

        private static byte[] mergeMax(byte[] a, byte[] b) {
            if (a.length == 0) return b;
            if (b.length == 0) return a;
            return Arrays.compareUnsigned(a, b) >= 0 ? a : b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ColumnStat that)) return false;
            return nullCount == that.nullCount
                    && valueCount == that.valueCount
                    && sizeBytes == that.sizeBytes
                    && logicalType.equals(that.logicalType)
                    && Arrays.equals(minBytes, that.minBytes)
                    && Arrays.equals(maxBytes, that.maxBytes);
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(nullCount);
            result = 31 * result + Long.hashCode(valueCount);
            result = 31 * result + Arrays.hashCode(minBytes);
            result = 31 * result + Arrays.hashCode(maxBytes);
            result = 31 * result + logicalType.hashCode();
            result = 31 * result + Long.hashCode(sizeBytes);
            return result;
        }
    }
}
