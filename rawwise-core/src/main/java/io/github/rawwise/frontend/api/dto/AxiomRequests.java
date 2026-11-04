package io.github.rawwise.frontend.api.dto;

import java.util.Map;

public class AxiomRequests {
    public record IngestRequest(
            String tableId,
            String key,
            String body,
            boolean immutable,
            Map<String, String> properties) {}

    public record SearchRequest(String tableId, String query, int topK, long snapshotId) {}

    public record AnnotateRequest(
            String viewId, String type, String value, String annotatorId, double confidence) {}
}
