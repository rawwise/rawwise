package io.github.rawwise.frontend.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.rawwise.engine.storage.ColumnStatistics;
import java.io.Serializable;

/**
 * Immutable statistics snapshot for a table snapshot.
 *
 * <p>Includes a {@code computedAtMillis} timestamp to support TTL-based staleness checks. Old
 * cached statistics without this field deserialize with a default timestamp of 0, which is always
 * considered stale, triggering recomputation.
 */
public record SnapshotStatistics(
        long totalRows,
        long totalBytes,
        int fileCount,
        long tombstoneCount,
        ColumnStatistics mergedColumnStats,
        long computedAtMillis)
        implements Serializable {

    /**
     * Compact constructor: defaults computedAtMillis to now if zero. This also handles
     * deserialization of old cached records that lack the computedAtMillis field (Jackson defaults
     * missing longs to 0).
     */
    public SnapshotStatistics {
        if (computedAtMillis == 0) {
            computedAtMillis = System.currentTimeMillis();
        }
    }

    /**
     * Backward-compatible constructor for callers that do not specify a computed timestamp. Sets
     * computedAtMillis to now.
     */
    @JsonCreator
    public SnapshotStatistics(
            @JsonProperty("totalRows") long totalRows,
            @JsonProperty("totalBytes") long totalBytes,
            @JsonProperty("fileCount") int fileCount,
            @JsonProperty("tombstoneCount") long tombstoneCount,
            @JsonProperty("mergedColumnStats") ColumnStatistics mergedColumnStats) {
        this(
                totalRows,
                totalBytes,
                fileCount,
                tombstoneCount,
                mergedColumnStats,
                System.currentTimeMillis());
    }

    public long effectiveRowCount() {
        return totalRows - tombstoneCount;
    }

    /**
     * Returns true if this statistics snapshot is older than {@code maxAgeMs}.
     *
     * @param maxAgeMs maximum allowed age in milliseconds
     * @return true if stale (or computedAtMillis is 0 from old deserialized data)
     */
    public boolean isStale(long maxAgeMs) {
        if (computedAtMillis == 0) {
            return true;
        }
        return System.currentTimeMillis() - computedAtMillis > maxAgeMs;
    }
}
