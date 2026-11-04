package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.axiom.pipeline.entity.EntityType;
import java.util.List;
import java.util.Optional;

public interface EntityCentroidStore {

    Optional<EntityCentroid> lookup(String entityId);

    List<EntityCentroid> lookupByType(EntityType type);

    void upsert(EntityCentroid centroid);

    void delete(String entityId);

    List<EntityCentroid> scanAll();

    long lastVersionMs();
}
