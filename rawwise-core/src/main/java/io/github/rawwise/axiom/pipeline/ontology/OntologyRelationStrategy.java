package io.github.rawwise.axiom.pipeline.ontology;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.cognition.SemanticGenerationStrategy;
import io.github.rawwise.axiom.pipeline.graph.GraphEdge;
import io.github.rawwise.axiom.pipeline.graph.GraphEdgeScanner;
import io.github.rawwise.model.record.Semantic;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OntologyRelationStrategy implements SemanticGenerationStrategy {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_PATHS =
            Integer.parseInt(
                    System.getProperty(
                            "rawwise.axiom.ontology.relation-strategy.max-paths", "100"));

    @Override
    public String name() {
        return "ontology-relation";
    }

    @Override
    public Semantic.SemanticType supportedType() {
        return Semantic.SemanticType.RELATION;
    }

    @Override
    public boolean appliesTo(List<String> viewIds, String tableId) {
        return true;
    }

    @Override
    public byte[] buildContent(
            List<String> viewIds, Map<String, Object> params, KnowledgeDataAccessor accessor) {
        String entityId = (String) params.get("entityId");
        if (entityId == null) return null;

        try {
            List<GraphEdge> hop1Edges = GraphEdgeScanner.scanSchemaOutEdges(accessor, entityId);
            List<Map<String, Object>> relations = new ArrayList<>();

            for (GraphEdge edge1 : hop1Edges) {
                if (relations.size() >= MAX_PATHS) break;
                relations.add(
                        Map.of(
                                "subject",
                                entityId,
                                "relation",
                                GraphEdgeScanner.stripSchemaPrefix(edge1.relType()),
                                "object",
                                edge1.dstId(),
                                "path",
                                entityId + " -> " + edge1.dstId(),
                                "source",
                                "ontology"));
                List<GraphEdge> hop2Edges =
                        GraphEdgeScanner.scanSchemaOutEdges(accessor, edge1.dstId());
                for (GraphEdge edge2 : hop2Edges) {
                    if (relations.size() >= MAX_PATHS) break;
                    if (edge2.dstId().equals(entityId)) continue;
                    relations.add(
                            Map.of(
                                    "subject",
                                    entityId,
                                    "relation",
                                    List.of(
                                            GraphEdgeScanner.stripSchemaPrefix(edge1.relType()),
                                            GraphEdgeScanner.stripSchemaPrefix(edge2.relType())),
                                    "object",
                                    edge2.dstId(),
                                    "path",
                                    entityId + " -> " + edge1.dstId() + " -> " + edge2.dstId(),
                                    "source",
                                    "ontology"));
                }
            }
            List<GraphEdge> inEdges = GraphEdgeScanner.scanSchemaInEdges(accessor, entityId);
            for (GraphEdge edge1 : inEdges) {
                if (relations.size() >= MAX_PATHS) break;
                relations.add(
                        Map.of(
                                "subject",
                                edge1.srcId(),
                                "relation",
                                GraphEdgeScanner.stripSchemaPrefix(edge1.relType()),
                                "object",
                                entityId,
                                "path",
                                edge1.srcId() + " -> " + entityId,
                                "source",
                                "ontology",
                                "direction",
                                "incoming"));
                List<GraphEdge> hop2OutEdges =
                        GraphEdgeScanner.scanSchemaOutEdges(accessor, edge1.srcId());
                for (GraphEdge edge2 : hop2OutEdges) {
                    if (relations.size() >= MAX_PATHS) break;
                    if (edge2.dstId().equals(entityId)) continue;
                    relations.add(
                            Map.of(
                                    "subject",
                                    entityId,
                                    "relation",
                                    List.of(
                                            GraphEdgeScanner.stripSchemaPrefix(edge1.relType()),
                                            GraphEdgeScanner.stripSchemaPrefix(edge2.relType())),
                                    "object",
                                    edge2.dstId(),
                                    "path",
                                    entityId + " <- " + edge1.srcId() + " -> " + edge2.dstId(),
                                    "source",
                                    "ontology",
                                    "direction",
                                    "incoming-then-outgoing"));
                }
            }
            return MAPPER.writeValueAsBytes(relations);
        } catch (IOException e) {
            log.warn(
                    "OntologyRelationStrategy failed for entityId={}: {}",
                    entityId,
                    e.getMessage(),
                    e);
            return null;
        }
    }

    @Override
    public double selfConfidence(byte[] content, List<String> viewIds) {
        return content != null ? 0.9 : 0.0;
    }
}
