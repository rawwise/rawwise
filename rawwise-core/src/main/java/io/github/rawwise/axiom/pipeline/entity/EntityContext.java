package io.github.rawwise.axiom.pipeline.entity;

import java.util.Collections;
import java.util.Map;

public record EntityContext(
        String sampleId, String tableId, long snapshotId, Map<String, String> properties) {

    public EntityContext {
        properties = properties == null ? Map.of() : Collections.unmodifiableMap(properties);
    }

    public static EntityContext of(String sampleId, String tableId, long snapshotId) {
        return new EntityContext(sampleId, tableId, snapshotId, Map.of());
    }
}
