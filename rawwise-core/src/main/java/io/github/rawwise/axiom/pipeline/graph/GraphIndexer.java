package io.github.rawwise.axiom.pipeline.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.pipeline.entity.Entity;
import io.github.rawwise.axiom.pipeline.entity.EntityView;
import io.github.rawwise.axiom.pipeline.entity.Relation;
import io.github.rawwise.engine.storage.KnowledgeStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * NOTE (PHC-008 / NI-TXN-001): This component writes to Knowledge Store directly, consistent with
 * the existing pattern in Default Derivation Engine. Full Txn Store Bridge / Txn Orchestrator
 * integration is tracked as project-wide debt and not regressed.
 */
@Slf4j
public final class GraphIndexer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KnowledgeStore knowledgeStore;

    public GraphIndexer(KnowledgeStore knowledgeStore) {
        this.knowledgeStore = knowledgeStore;
    }

    public void indexEntities(EntityView view) {
        indexEntities(view, null);
    }

    public void indexEntities(EntityView view, List<String> chunkIds) {
        if (view == null) return;
        String diaId = view.diaId();
        String sampleId = view.sampleId();
        try {
            List<String> entityIds = new ArrayList<>();
            for (Entity entity : view.entities()) {
                writeNode(entity);
                writeNameIndex(sampleId, entity);
                writeTypeIndex(entity);
                writeEntityToDia(entity.id(), diaId, sampleId, 0);
                entityIds.add(entity.id());
                if (chunkIds != null) {
                    for (String chunkId : chunkIds) {
                        writeChunkToEntity(chunkId, entity.id());
                    }
                }
            }
            if (!entityIds.isEmpty() && sampleId != null && diaId != null) {
                writeDiaToEntity(sampleId, diaId, entityIds);
            }
            for (Relation relation : view.relations()) {
                writeEdge(relation);
            }
        } catch (IOException e) {
            log.error(
                    "GraphIndexer.indexEntities failed diaId={} sampleId={}: {}",
                    diaId,
                    sampleId,
                    e.getMessage(),
                    e);
        }
    }

    private void writeChunkToEntity(String chunkId, String entityId) throws IOException {
        byte[] key =
                ("chunk_to_entity:" + chunkId + ":" + entityId).getBytes(StandardCharsets.UTF_8);
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("chunkId", chunkId);
        meta.put("entityId", entityId);
        knowledgeStore.put(key, MAPPER.writeValueAsBytes(meta));
    }

    private void writeNode(Entity entity) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", entity.id());
        payload.put("type", entity.type().name());
        payload.put("name", entity.name());
        payload.put("canonicalName", Entity.canonicalize(entity.name()));
        payload.put("attrs", entity.attrs());
        payload.put("confidence", entity.confidence());
        knowledgeStore.put(
                GraphStoreSchema.nodeKey(entity.id()), MAPPER.writeValueAsBytes(payload));
    }

    private void writeNameIndex(String sampleId, Entity entity) throws IOException {
        if (sampleId == null) return;
        byte[] idxKey =
                GraphStoreSchema.nameToEntityKey(sampleId, Entity.canonicalize(entity.name()));
        knowledgeStore.put(idxKey, entity.id().getBytes(StandardCharsets.UTF_8));
    }

    private void writeTypeIndex(Entity entity) throws IOException {
        byte[] idxKey = GraphStoreSchema.typeToEntityKey(entity.type().name(), entity.id());
        knowledgeStore.put(idxKey, entity.id().getBytes(StandardCharsets.UTF_8));
    }

    private void writeEntityToDia(String entityId, String diaId, String sampleId, int turnIdx)
            throws IOException {
        if (diaId == null) return;
        byte[] key = GraphStoreSchema.entityToDiaKey(entityId, diaId, turnIdx);
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("diaId", diaId);
        meta.put("sampleId", sampleId);
        meta.put("turnIdx", turnIdx);
        knowledgeStore.put(key, MAPPER.writeValueAsBytes(meta));
    }

    private void writeDiaToEntity(String sampleId, String diaId, List<String> entityIds)
            throws IOException {
        byte[] key = GraphStoreSchema.diaToEntityKey(sampleId, diaId);
        knowledgeStore.put(key, MAPPER.writeValueAsBytes(entityIds));
    }

    private void writeEdge(Relation relation) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("srcId", relation.head());
        payload.put("relType", relation.relType());
        payload.put("dstId", relation.tail());
        payload.put("temporal", relation.temporal());
        payload.put("confidence", relation.confidence());
        payload.put("weight", relation.confidence());
        byte[] payloadBytes = MAPPER.writeValueAsBytes(payload);

        knowledgeStore.put(
                GraphStoreSchema.outEdgeKey(relation.head(), relation.relType(), relation.tail()),
                payloadBytes);
        knowledgeStore.put(
                GraphStoreSchema.inEdgeKey(relation.tail(), relation.relType(), relation.head()),
                payloadBytes);
    }
}
