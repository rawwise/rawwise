package io.github.rawwise.axiom.pipeline.ontology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.model.record.ForeignKeyMeta;
import io.github.rawwise.model.record.TableMeta;
import java.util.ArrayList;
import java.util.List;

public interface OntologyMapping {

    OntologyClass ontologyClass();

    List<OntologyDataProperty> dataProperties();

    List<OntologyObjectProperty> objectProperties();

    List<OntologyAxiom> axioms();

    static OntologyMapping from(TableMeta meta) {
        String className =
                meta.properties()
                        .getOrDefault("entity.type", inferTypeFromTableName(meta.fullName()));
        String superClass = meta.properties().get("entity.subtype");
        OntologyClass cls = new OntologyClass(className, superClass, meta.fullName());

        // Build data properties from schema columns
        List<OntologyDataProperty> dataProps = new ArrayList<>();
        if (meta.schema() != null && meta.schema().fields() != null) {
            for (var field : meta.schema().fields()) {
                dataProps.add(
                        new OntologyDataProperty(
                                field.name(),
                                field.dataType() != null ? field.dataType().name() : "UNKNOWN",
                                field.name()));
            }
        }

        // Build object properties from foreign.keys TBLPROPERTIES
        List<OntologyObjectProperty> objProps = new ArrayList<>();
        String fkJson = meta.properties().get("foreign.keys");
        if (fkJson != null && !fkJson.isBlank()) {
            try {
                var fkArray = MapperHolder.MAPPER.readTree(fkJson);
                if (fkArray.isArray()) {
                    for (var fkNode : fkArray) {
                        List<String> columns = parseStringList(fkNode.get("columns"));
                        String parentTable =
                                fkNode.has("parentTable")
                                        ? fkNode.get("parentTable").asText()
                                        : null;
                        List<String> parentColumns = parseStringList(fkNode.get("parentColumns"));
                        String declaredType =
                                fkNode.has("relationType")
                                        ? fkNode.get("relationType").asText()
                                        : null;
                        String declaredLabel =
                                fkNode.has("relationLabel")
                                        ? fkNode.get("relationLabel").asText()
                                        : null;

                        if (columns != null && !columns.isEmpty() && parentTable != null) {
                            ForeignKeyMeta fkMeta =
                                    new ForeignKeyMeta(
                                            columns,
                                            parentTable,
                                            parentColumns != null ? parentColumns : columns);
                            String propertyName =
                                    (declaredType != null && !declaredType.isBlank())
                                            ? declaredType
                                            : inferRelationType(fkMeta);
                            String rangeClass = inferTypeFromTableName(parentTable);
                            objProps.add(
                                    new OntologyObjectProperty(
                                            propertyName,
                                            declaredLabel,
                                            className,
                                            rangeClass,
                                            fkMeta));
                        }
                    }
                }
            } catch (Exception e) {
                // Fallback to empty list on JSON parse failure
            }
        }

        return new DefaultOntologyMapping(cls, dataProps, objProps, List.of());
    }

    static List<String> parseStringList(JsonNode node) {
        if (node == null || !node.isArray()) return null;
        List<String> result = new ArrayList<>();
        for (var item : node) {
            result.add(item.asText());
        }
        return result;
    }

    static String inferTypeFromTableName(String fullName) {
        if (fullName == null || fullName.isBlank()) return "Unknown";
        String[] parts = fullName.split("\\.");
        String name = parts[parts.length - 1];
        if (name.isEmpty()) return "Unknown";
        if (name.endsWith("s") && name.length() > 1) {
            name = name.substring(0, name.length() - 1);
        }
        return Character.toUpperCase(name.charAt(0)) + (name.length() > 1 ? name.substring(1) : "");
    }

    static String inferRelationType(ForeignKeyMeta fk) {
        if (fk.columns() == null || fk.columns().isEmpty()) return "references_unknown";
        String col = fk.columns().get(0);
        if (col.endsWith("_id")) {
            return "belongs_to_" + col.substring(0, col.length() - 3);
        }
        return "references_" + col;
    }

    static class MapperHolder {
        static final ObjectMapper MAPPER = new ObjectMapper();
    }
}
