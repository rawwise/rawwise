package io.github.rawwise.axiom.pipeline.entity;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record EntityView(
        String diaId,
        String sampleId,
        List<Entity> entities,
        List<Relation> relations,
        String source,
        Instant extractedAt,
        ExtractionStatus status,
        String failureReason) {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new JavaTimeModule());

    public EntityView {
        entities = entities == null ? List.of() : List.copyOf(entities);
        relations = relations == null ? List.of() : List.copyOf(relations);
    }

    public static EntityView pending(String diaId, String sampleId) {
        return new EntityView(
                diaId,
                sampleId,
                List.of(),
                List.of(),
                "pending",
                Instant.now(),
                ExtractionStatus.PENDING,
                null);
    }

    public static EntityView failed(String diaId, String sampleId, String reason) {
        return new EntityView(
                diaId,
                sampleId,
                List.of(),
                List.of(),
                "failed",
                Instant.now(),
                ExtractionStatus.FAILED,
                reason);
    }

    public byte[] encode() {
        try {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("diaId", diaId);
            root.put("sampleId", sampleId);
            root.put("entities", entities);
            root.put("relations", relations);
            root.put("source", source);
            root.put("extractedAt", extractedAt == null ? null : extractedAt.toString());
            root.put("status", status == null ? null : status.name());
            root.put("failureReason", failureReason);
            return MAPPER.writeValueAsBytes(root);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static EntityView decode(byte[] bytes) {
        try {
            Map<String, Object> root =
                    MAPPER.readValue(bytes, new TypeReference<Map<String, Object>>() {});
            String diaId = (String) root.get("diaId");
            String sampleId = (String) root.get("sampleId");
            List<Entity> entities =
                    MAPPER.convertValue(
                            root.getOrDefault("entities", List.of()),
                            new TypeReference<List<Entity>>() {});
            List<Relation> relations =
                    MAPPER.convertValue(
                            root.getOrDefault("relations", List.of()),
                            new TypeReference<List<Relation>>() {});
            String source = (String) root.get("source");
            Object extractedAtRaw = root.get("extractedAt");
            Instant extractedAt =
                    extractedAtRaw == null ? null : Instant.parse(extractedAtRaw.toString());
            String statusStr = (String) root.get("status");
            ExtractionStatus status =
                    statusStr == null ? null : ExtractionStatus.valueOf(statusStr);
            String failureReason = (String) root.get("failureReason");
            return new EntityView(
                    diaId,
                    sampleId,
                    entities,
                    relations,
                    source,
                    extractedAt,
                    status,
                    failureReason);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
