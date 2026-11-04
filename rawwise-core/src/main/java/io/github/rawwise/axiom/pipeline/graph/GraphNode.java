package io.github.rawwise.axiom.pipeline.graph;

import io.github.rawwise.axiom.pipeline.entity.Entity;
import io.github.rawwise.axiom.pipeline.entity.EntityType;
import java.util.Map;

public record GraphNode(
        String entityId,
        EntityType type,
        String name,
        String canonicalName,
        Map<String, String> attrs,
        double confidence) {

    public static GraphNode fromEntity(Entity entity) {
        return new GraphNode(
                entity.id(),
                entity.type(),
                entity.name(),
                Entity.canonicalize(entity.name()),
                entity.attrs(),
                entity.confidence());
    }
}
