package io.github.rawwise.axiom.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record ViewMetadata(
        String entryKey, String tableId, float decayFactor, long lastAccessedAt, int accessCount) {
    public ViewMetadata {
        if (entryKey == null || entryKey.isEmpty()) {
            throw new IllegalArgumentException("entryKey must not be null or empty");
        }
        if (tableId == null || tableId.isEmpty()) {
            throw new IllegalArgumentException("tableId must not be null or empty");
        }
        if (decayFactor < 0.0f || decayFactor > 1.0f) {
            throw new IllegalArgumentException(
                    "decayFactor must be in [0.0, 1.0], got: " + decayFactor);
        }
    }

    public ViewMetadata(String entryKey, String tableId) {
        this(entryKey, tableId, 1.0f, System.currentTimeMillis(), 0);
    }

    public ViewMetadata withDecayFactor(float decayFactor) {
        return new ViewMetadata(entryKey, tableId, decayFactor, lastAccessedAt, accessCount);
    }

    public ViewMetadata withAccessUpdate(long accessedAt) {
        return new ViewMetadata(entryKey, tableId, decayFactor, accessedAt, accessCount + 1);
    }

    @JsonCreator
    public static ViewMetadata fromJson(
            @JsonProperty("entryKey") String entryKey,
            @JsonProperty("tableId") String tableId,
            @JsonProperty("decayFactor") Float decayFactor,
            @JsonProperty("lastAccessedAt") Long lastAccessedAt,
            @JsonProperty("accessCount") Integer accessCount) {
        return new ViewMetadata(
                entryKey,
                tableId,
                decayFactor != null ? decayFactor : 1.0f,
                lastAccessedAt != null ? lastAccessedAt : 0,
                accessCount != null ? accessCount : 0);
    }
}
