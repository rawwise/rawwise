package io.github.rawwise.axiom.pipeline.ontology;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.derivation.ViewGenerationStrategy;
import io.github.rawwise.axiom.pipeline.graph.GraphEdge;
import io.github.rawwise.axiom.pipeline.graph.GraphEdgeScanner;
import io.github.rawwise.model.record.View;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OntologyAwareViewStrategy implements ViewGenerationStrategy {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String name() {
        return "ontology-aware";
    }

    @Override
    public View.ViewType supportedViewType() {
        return View.ViewType.SUMMARY;
    }

    @Override
    public boolean appliesTo(List<String> chunkIds, String tableId) {
        return true;
    }

    @Override
    public byte[] buildContent(
            List<String> chunkIds, Map<String, Object> params, KnowledgeDataAccessor accessor) {
        String entityId = (String) params.get("entityId");
        if (entityId == null) return null;

        try {
            List<GraphEdge> schemaEdges = GraphEdgeScanner.scanSchemaOutEdges(accessor, entityId);

            StringBuilder sb = new StringBuilder();
            for (GraphEdge edge : schemaEdges) {
                String dstSummary = null;
                Optional<byte[]> dstViewBytes = accessor.readView(edge.dstId());
                if (dstViewBytes.isPresent()) {
                    dstSummary = extractSummary(dstViewBytes.get());
                } else {
                    String rawKey = "rec:" + edge.dstId().replace(":", "#");
                    Optional<byte[]> dstRawBytes =
                            accessor.getWithFallback(rawKey.getBytes(StandardCharsets.UTF_8));
                    if (dstRawBytes.isPresent()) {
                        dstSummary = extractRawText(dstRawBytes.get());
                    }
                }
                if (dstSummary != null) {
                    String displayLabel = GraphEdgeScanner.stripSchemaPrefix(edge.relType());
                    sb.append(displayLabel).append(": ").append(dstSummary).append("\n");
                }
            }
            return sb.length() > 0 ? sb.toString().getBytes(StandardCharsets.UTF_8) : null;
        } catch (IOException e) {
            log.warn(
                    "OntologyAwareViewStrategy failed for entityId={}: {}",
                    entityId,
                    e.getMessage(),
                    e);
            return null;
        }
    }

    private String extractSummary(byte[] viewBytes) {
        try {
            var node = MAPPER.readTree(viewBytes);
            if (node.has("content")) {
                return node.get("content").asText();
            }
            return new String(viewBytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return new String(viewBytes, StandardCharsets.UTF_8);
        }
    }

    private String extractRawText(byte[] rawBytes) {
        try {
            var node = MAPPER.readTree(rawBytes);
            if (node.has("body")) {
                return node.get("body").asText();
            }
            return new String(rawBytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return new String(rawBytes, StandardCharsets.UTF_8);
        }
    }

    @Override
    public double selfConfidence(byte[] content, List<String> chunkIds) {
        return content != null ? 0.8 : 0.0;
    }
}
