package io.github.rawwise.axiom.pipeline.graph;

import io.github.rawwise.axiom.pipeline.entity.Entity;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface GraphQuery {

    Optional<Entity> findEntity(String sampleId, String name);

    Map<String, Double> traverse(Set<String> seedEntityIds, int maxHops, EdgeFilter filter);

    Set<DiaRef> lookupDiaIds(String entityId);

    Set<String> lookupEntitiesByDia(String sampleId, String diaId);

    Optional<Entity> findEntityById(String entityId);
}
