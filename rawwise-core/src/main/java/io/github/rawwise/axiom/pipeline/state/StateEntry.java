package io.github.rawwise.axiom.pipeline.state;

import java.util.Map;

public record StateEntry(
        String key,
        String value,
        String stateName,
        long version,
        Map<String, String> metadata,
        long createdAt,
        long updatedAt) {
    public StateEntry {
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
