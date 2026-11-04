package io.github.rawwise.axiom.pipeline.graph;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.entity.Entity;
import io.github.rawwise.axiom.pipeline.entity.EntityType;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.KeyValue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class LocalGraphQueryEngine implements GraphQuery {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final double PPR_DECAY = 0.85;
    private static final int QUEUE_CAP_PER_HOP = 1000;

    private final KnowledgeStore knowledgeStore;
    private final KnowledgeDataAccessor dataAccessor;

    public LocalGraphQueryEngine(
            KnowledgeStore knowledgeStore, KnowledgeDataAccessor dataAccessor) {
        this.knowledgeStore = knowledgeStore;
        this.dataAccessor = dataAccessor;
    }

    @Override
    public Optional<Entity> findEntity(String sampleId, String name) {
        try {
            String canon = Entity.canonicalize(name);
            byte[] idxKey = GraphStoreSchema.nameToEntityKey(sampleId, canon);
            Optional<byte[]> entityIdBytes = knowledgeStore.get(idxKey);
            if (entityIdBytes.isEmpty()) return Optional.empty();
            String entityId = new String(entityIdBytes.get(), StandardCharsets.UTF_8);
            return loadEntity(entityId);
        } catch (IOException e) {
            log.warn("findEntity sampleId={} name={} failed: {}", sampleId, name, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public Map<String, Double> traverse(Set<String> seedEntityIds, int maxHops, EdgeFilter filter) {
        if (seedEntityIds == null || seedEntityIds.isEmpty() || maxHops <= 0) {
            return Map.of();
        }
        EdgeFilter effective = filter == null ? EdgeFilter.allowAll() : filter;
        Map<String, Double> scores = new LinkedHashMap<>();
        for (String seed : seedEntityIds) {
            scores.put(seed, 1.0);
        }
        Deque<String> currentLevel = new ArrayDeque<>(seedEntityIds);
        int queueCap = maxHops * QUEUE_CAP_PER_HOP;

        for (int hop = 1; hop <= maxHops && !currentLevel.isEmpty(); hop++) {
            Deque<String> nextLevel = new ArrayDeque<>();
            int processedAtHop = 0;
            while (!currentLevel.isEmpty() && processedAtHop < queueCap) {
                String src = currentLevel.poll();
                processedAtHop++;
                double srcScore = scores.getOrDefault(src, 0.0);
                List<GraphEdge> outEdges = readOutEdges(src);
                for (GraphEdge edge : outEdges) {
                    if (!effective.accept(edge)) continue;
                    if (scores.containsKey(edge.dstId())) continue;
                    double newScore = srcScore * Math.pow(PPR_DECAY, hop) * edge.weight();
                    scores.put(edge.dstId(), newScore);
                    if (scores.size() + nextLevel.size() < queueCap) {
                        nextLevel.add(edge.dstId());
                    }
                }
            }
            currentLevel = nextLevel;
        }
        return scores;
    }

    @Override
    public Set<DiaRef> lookupDiaIds(String entityId) {
        Set<DiaRef> refs = new HashSet<>();
        try {
            List<KeyValue> kvs =
                    dataAccessor.scanWithFallback(GraphStoreSchema.entityToDiaPrefix(entityId));
            for (KeyValue kv : kvs) {
                String fullKey = new String(kv.key(), StandardCharsets.UTF_8);
                String suffix =
                        fullKey.substring(
                                GraphStoreSchema.IDX_ENTITY_TO_DIA.length()
                                        + entityId.length()
                                        + 1);
                String[] parts = suffix.split(":");
                if (parts.length >= 2) {
                    String diaId = parts[0];
                    int turnIdx = 0;
                    try {
                        turnIdx = Integer.parseInt(parts[parts.length - 1]);
                    } catch (NumberFormatException e) {
                        log.debug(
                                "lookupDiaIds: non-numeric turnIdx suffix in key={}, defaulting to 0",
                                fullKey);
                    }
                    String sampleId = "";
                    if (kv.value() != null && kv.value().length > 0) {
                        try {
                            Map<String, Object> meta =
                                    MAPPER.readValue(
                                            kv.value(),
                                            new TypeReference<Map<String, Object>>() {});
                            Object s = meta.get("sampleId");
                            if (s != null) sampleId = s.toString();
                        } catch (IOException e) {
                            log.debug(
                                    "lookupDiaIds: malformed metadata for key={}: {}",
                                    fullKey,
                                    e.getMessage());
                        }
                    }
                    refs.add(new DiaRef(diaId, sampleId, turnIdx));
                }
            }
        } catch (IOException e) {
            log.warn("lookupDiaIds entityId={} failed: {}", entityId, e.getMessage());
        }
        return refs;
    }

    @Override
    public Set<String> lookupEntitiesByDia(String sampleId, String diaId) {
        Set<String> entityIds = new HashSet<>();
        if (sampleId == null || diaId == null) return entityIds;
        try {
            byte[] key = GraphStoreSchema.diaToEntityKey(sampleId, diaId);
            Optional<byte[]> bytes = knowledgeStore.get(key);
            if (bytes.isEmpty()) return entityIds;
            List<String> ids = MAPPER.readValue(bytes.get(), new TypeReference<List<String>>() {});
            entityIds.addAll(ids);
        } catch (IOException e) {
            log.warn(
                    "lookupEntitiesByDia sampleId={} diaId={} failed: {}",
                    sampleId,
                    diaId,
                    e.getMessage());
        }
        return entityIds;
    }

    @Override
    public Optional<Entity> findEntityById(String entityId) {
        if (entityId == null) return Optional.empty();
        try {
            return loadEntity(entityId);
        } catch (IOException e) {
            log.warn("findEntityById entityId={} failed: {}", entityId, e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<Entity> loadEntity(String entityId) throws IOException {
        Optional<byte[]> bytes = knowledgeStore.get(GraphStoreSchema.nodeKey(entityId));
        if (bytes.isEmpty()) return Optional.empty();
        Map<String, Object> node =
                MAPPER.readValue(bytes.get(), new TypeReference<Map<String, Object>>() {});
        EntityType type =
                node.get("type") == null
                        ? EntityType.OTHER
                        : EntityType.valueOf(node.get("type").toString());
        String name = (String) node.getOrDefault("name", "");
        @SuppressWarnings("unchecked")
        Map<String, String> attrs = (Map<String, String>) node.getOrDefault("attrs", Map.of());
        double confidence =
                node.get("confidence") == null
                        ? 0.0
                        : ((Number) node.get("confidence")).doubleValue();
        return Optional.of(new Entity(entityId, type, name, null, attrs, confidence));
    }

    private List<GraphEdge> readOutEdges(String srcId) {
        List<GraphEdge> edges = new ArrayList<>();
        try {
            List<KeyValue> kvs =
                    dataAccessor.scanWithFallback(GraphStoreSchema.outEdgePrefix(srcId));
            for (KeyValue kv : kvs) {
                try {
                    Map<String, Object> payload =
                            MAPPER.readValue(
                                    kv.value(), new TypeReference<Map<String, Object>>() {});
                    String relType = (String) payload.getOrDefault("relType", "");
                    String dstId = (String) payload.getOrDefault("dstId", "");
                    String temporal = (String) payload.get("temporal");
                    double confidence =
                            payload.get("confidence") == null
                                    ? 0.0
                                    : ((Number) payload.get("confidence")).doubleValue();
                    double weight =
                            payload.get("weight") == null
                                    ? confidence
                                    : ((Number) payload.get("weight")).doubleValue();
                    edges.add(new GraphEdge(srcId, relType, dstId, temporal, confidence, weight));
                } catch (IOException e) {
                    log.warn("readOutEdges decode failure srcId={}: {}", srcId, e.getMessage());
                }
            }
        } catch (IOException e) {
            log.warn("readOutEdges scan failure srcId={}: {}", srcId, e.getMessage());
        }
        return edges;
    }
}
