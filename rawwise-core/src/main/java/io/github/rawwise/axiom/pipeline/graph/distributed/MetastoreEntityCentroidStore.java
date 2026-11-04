package io.github.rawwise.axiom.pipeline.graph.distributed;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.pipeline.entity.EntityType;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetastoreEntityCentroidStore implements EntityCentroidStore {

    private static final String KEY_PREFIX = "entity_centroid:";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Metastore metastore;

    public MetastoreEntityCentroidStore(Metastore metastore) {
        this.metastore = metastore;
    }

    @Override
    public Optional<EntityCentroid> lookup(String entityId) {
        try {
            Optional<byte[]> bytes = metastore.get(keyOf(entityId));
            if (bytes.isEmpty()) return Optional.empty();
            return Optional.of(decode(bytes.get()));
        } catch (IOException e) {
            log.warn("EntityCentroidStore.lookup failed entityId={}: {}", entityId, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public List<EntityCentroid> lookupByType(EntityType type) {
        List<EntityCentroid> all = scanAll();
        List<EntityCentroid> filtered = new ArrayList<>();
        for (EntityCentroid c : all) {
            if (c.entityType() == type) filtered.add(c);
        }
        return filtered;
    }

    @Override
    public void upsert(EntityCentroid centroid) {
        try {
            byte[] payload = MAPPER.writeValueAsBytes(toMap(centroid));
            metastore.put(keyOf(centroid.entityId()), payload);
        } catch (IOException e) {
            log.warn(
                    "EntityCentroidStore.upsert failed entityId={}: {}",
                    centroid.entityId(),
                    e.getMessage());
        }
    }

    @Override
    public void delete(String entityId) {
        try {
            metastore.delete(keyOf(entityId));
        } catch (IOException e) {
            log.warn("EntityCentroidStore.delete failed entityId={}: {}", entityId, e.getMessage());
        }
    }

    @Override
    public List<EntityCentroid> scanAll() {
        List<EntityCentroid> result = new ArrayList<>();
        try (CloseableIterator<KeyValue> it =
                metastore.scan(KEY_PREFIX.getBytes(StandardCharsets.UTF_8))) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                try {
                    result.add(decode(kv.value()));
                } catch (IOException e) {
                    log.warn(
                            "EntityCentroidStore.scanAll: skipping malformed centroid entry: {}",
                            e.getMessage());
                }
            }
        } catch (IOException e) {
            log.warn("EntityCentroidStore.scanAll failed: {}", e.getMessage());
        }
        return result;
    }

    @Override
    public long lastVersionMs() {
        long max = 0L;
        for (EntityCentroid c : scanAll()) {
            if (c.lastUpdatedEpochMs() > max) max = c.lastUpdatedEpochMs();
        }
        return max;
    }

    private static byte[] keyOf(String entityId) {
        return (KEY_PREFIX + entityId).getBytes(StandardCharsets.UTF_8);
    }

    private static Map<String, Object> toMap(EntityCentroid c) {
        Map<String, Object> m = new HashMap<>();
        m.put("entityId", c.entityId());
        m.put("entityType", c.entityType() == null ? null : c.entityType().name());
        m.put("sampleId", c.sampleId());
        m.put("shardId", c.shardId() == null ? null : c.shardId().value());
        m.put("lastUpdatedEpochMs", c.lastUpdatedEpochMs());
        return m;
    }

    private static EntityCentroid decode(byte[] bytes) throws IOException {
        Map<String, Object> m =
                MAPPER.readValue(bytes, new TypeReference<Map<String, Object>>() {});
        Object type = m.get("entityType");
        Object shard = m.get("shardId");
        Object ts = m.get("lastUpdatedEpochMs");
        return EntityCentroid.builder()
                .entityId((String) m.get("entityId"))
                .entityType(type == null ? null : EntityType.valueOf(type.toString()))
                .sampleId((String) m.get("sampleId"))
                .shardId(shard == null ? null : new ShardId(((Number) shard).intValue()))
                .lastUpdatedEpochMs(ts == null ? 0L : ((Number) ts).longValue())
                .build();
    }
}
