package io.github.rawwise.axiom.pipeline.ontology;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.pipeline.graph.GraphIndexer;
import io.github.rawwise.model.record.ForeignKeyMeta;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.SemanticRecord;
import io.github.rawwise.model.record.TableMeta;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OntologyMappingStage {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final GraphIndexer graphIndexer;

    public OntologyMappingStage(GraphIndexer graphIndexer) {
        this.graphIndexer = graphIndexer;
    }

    public OntologyMapping getOntologyMapping(TableMeta meta) {
        return OntologyMapping.from(meta);
    }

    public String getIndividualId(SemanticRecord record, String tableIdentifier, TableMeta meta) {
        String pkValue = extractPrimaryKeyValue(record, meta);
        if (pkValue == null) return null;
        return tableIdentifier + ":" + pkValue;
    }

    public void onWrite(SemanticRecord record, String tableIdentifier, TableMeta meta) {
        if (meta == null) return;

        OntologyMapping mapping = OntologyMapping.from(meta);
        String pkValue = extractPrimaryKeyValue(record, meta);
        if (pkValue == null) return;
        String entityId = tableIdentifier + ":" + pkValue;
        String entityType = mapping.ontologyClass().className();

        // graphIndexer.indexIndividual(entityId, entityType, pkValue, tableIdentifier); // TODO:
        // add method to GraphIndexer

        for (OntologyObjectProperty prop : mapping.objectProperties()) {
            ForeignKeyMeta fk = prop.foreignKeyMeta();
            String fkValue = extractFieldValue(record, fk.columns().get(0), meta);
            if (fkValue == null) continue;

            String dstEntityId =
                    normalizeEntityId(fk.parentTable(), tableIdentifier) + ":" + fkValue;
            String relType = prop.propertyName();

            // graphIndexer.indexObjectProperty(entityId, relType, dstEntityId, 1.0); // TODO: add
            // method to GraphIndexer

            String relLabel = prop.propertyLabel();
            String relationText =
                    renderRelationSemantic(
                            entityType, pkValue, relType, relLabel, dstEntityId, record);
            if (relationText != null) {
                String chunkId = entityId + "#rel:" + relType + ":" + dstEntityId;
                // graphIndexer.indexRelationChunk(chunkId, relationText, entityId, relType,
                // dstEntityId); // TODO: add method to GraphIndexer
            }
        }
    }

    private String extractPrimaryKeyValue(SemanticRecord record, TableMeta meta) {
        if (meta.primaryKey() == null || meta.primaryKey().fields().isEmpty()) return null;
        String pkCol = meta.primaryKey().fields().get(0);
        return extractFieldValueFromRecord(record, pkCol);
    }

    private String extractFieldValue(SemanticRecord record, String fieldName, TableMeta meta) {
        return extractFieldValueFromRecord(record, fieldName);
    }

    private String extractFieldValueFromRecord(SemanticRecord record, String fieldName) {
        if (!(record instanceof Record r)) return null;
        try {
            Map<String, Object> bodyMap = r.body().toMap();
            Object bodyContent = bodyMap.get("body");
            if (bodyContent instanceof byte[] bytes) {
                Map<String, Object> parsed = MAPPER.readValue(bytes, MAP_TYPE);
                Object val = parsed.get(fieldName);
                return val != null ? String.valueOf(val) : null;
            } else if (bodyContent instanceof String s) {
                Map<String, Object> parsed = MAPPER.readValue(s, MAP_TYPE);
                Object val = parsed.get(fieldName);
                return val != null ? String.valueOf(val) : null;
            }
        } catch (Exception e) {
            log.debug("Failed to extract field={}: {}", fieldName, e.getMessage());
        }
        return null;
    }

    private String normalizeEntityId(String parentTable, String tableIdentifier) {
        if (parentTable == null || parentTable.isEmpty()) return parentTable;
        if (parentTable.contains(".")) return parentTable;
        int dotIdx = tableIdentifier.indexOf('.');
        if (dotIdx > 0) {
            String dbPrefix = tableIdentifier.substring(0, dotIdx);
            return dbPrefix + "." + parentTable;
        }
        return parentTable;
    }

    private String renderRelationSemantic(
            String entityType,
            String pkValue,
            String relType,
            String relLabel,
            String dstEntityId,
            SemanticRecord record) {
        String entityName = extractFieldValueFromRecord(record, "name");
        if (entityName == null) entityName = pkValue;

        if (relLabel != null && !relLabel.isBlank()) {
            return entityName + " " + relLabel + " " + dstEntityId;
        }
        return entityType + ":" + pkValue + " " + relType + " " + dstEntityId;
    }
}
