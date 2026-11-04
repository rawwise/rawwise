package io.github.rawwise.axiom.pipeline.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.View;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultImplicitViewGenerator implements ImplicitViewGenerator {

    private static final String KEY_PREFIX = "implicit_view:";
    private static final String GENERATOR_VERSION = "1.0";
    private static final String SOURCE_IMPLICIT = "implicit";

    private final Metastore metastore;
    private final KnowledgeStore knowledgeStore;
    private final KnowledgeDataAccessor dataAccessor;
    private final ObjectMapper objectMapper;

    public DefaultImplicitViewGenerator(Metastore metastore, KnowledgeStore knowledgeStore) {
        this.metastore = metastore;
        this.knowledgeStore = knowledgeStore;
        this.dataAccessor = new KnowledgeDataAccessor(metastore, knowledgeStore);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void generateImplicitView(Record.Key rawKey, long tableId, long snapshotId) {
        try {
            byte[] metastoreKey = buildMetastoreKey(rawKey, tableId);

            if (knowledgeStore.exists(metastoreKey)) {
                log.debug("Implicit view already exists for tableId={}, key={}", tableId, rawKey);
                return;
            }

            String viewId = buildViewId(rawKey, tableId, snapshotId);
            byte[] contentBytes = rawKey.encoded();

            View view =
                    new View(
                            viewId,
                            List.of(),
                            View.ViewType.RAW_IDENTITY,
                            0,
                            contentBytes,
                            Optional.empty(),
                            Optional.of(0.0),
                            List.of());

            byte[] viewJson = serializeView(view);
            knowledgeStore.put(metastoreKey, viewJson);

            log.debug("Generated implicit view {} for tableId={}, key={}", viewId, tableId, rawKey);
        } catch (Exception e) {
            log.warn(
                    "Failed to generate implicit view for tableId={}, key={}: {}",
                    tableId,
                    rawKey,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    public boolean isViewGenerated(Record.Key rawKey, long tableId, long snapshotId) {
        try {
            byte[] metastoreKey = buildMetastoreKey(rawKey, tableId);
            return knowledgeStore.exists(metastoreKey);
        } catch (IOException e) {
            log.warn(
                    "Failed to check implicit view existence for tableId={}, key={}: {}",
                    tableId,
                    rawKey,
                    e.getMessage());
            return false;
        }
    }

    @Override
    public Optional<View> getImplicitView(Record.Key rawKey, long tableId, long snapshotId) {
        try {
            byte[] metastoreKey = buildMetastoreKey(rawKey, tableId);
            Optional<byte[]> stored = dataAccessor.getWithFallback(metastoreKey);
            if (stored.isEmpty()) {
                return Optional.empty();
            }
            View view = deserializeView(stored.get());
            return Optional.of(view);
        } catch (IOException e) {
            log.warn(
                    "Failed to get implicit view for tableId={}, key={}: {}",
                    tableId,
                    rawKey,
                    e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public Metastore metastore() {
        return metastore;
    }

    private byte[] buildMetastoreKey(Record.Key rawKey, long tableId) {
        String encodedKey = Base64.getEncoder().encodeToString(rawKey.encoded());
        String keyStr = KEY_PREFIX + tableId + ":" + encodedKey;
        return keyStr.getBytes(StandardCharsets.UTF_8);
    }

    private String buildViewId(Record.Key rawKey, long tableId, long snapshotId) {
        String encodedKey = Base64.getEncoder().encodeToString(rawKey.encoded());
        return "v0:" + tableId + ":" + encodedKey + ":" + snapshotId;
    }

    private byte[] serializeView(View view) throws JsonProcessingException {
        Map<String, Object> viewMap =
                Map.of(
                        "viewId", view.viewId(),
                        "sourceChunkIds", view.sourceChunkIds(),
                        "viewType", view.viewType().name(),
                        "version", view.version(),
                        "content", Base64.getEncoder().encodeToString(view.content()),
                        "qualityScore", view.qualityScore().orElse(0.0),
                        "source", SOURCE_IMPLICIT,
                        "generatorVersion", GENERATOR_VERSION);
        return objectMapper.writeValueAsBytes(viewMap);
    }

    private View deserializeView(byte[] jsonBytes) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = objectMapper.readValue(jsonBytes, Map.class);

        String viewId = (String) map.get("viewId");
        View.ViewType viewType = View.ViewType.valueOf((String) map.get("viewType"));
        int version = ((Number) map.get("version")).intValue();
        String contentBase64 = (String) map.get("content");
        byte[] content = Base64.getDecoder().decode(contentBase64);
        double qualityScore = ((Number) map.getOrDefault("qualityScore", 0.0)).doubleValue();

        @SuppressWarnings("unchecked")
        List<String> sourceChunkIds = (List<String>) map.getOrDefault("sourceChunkIds", List.of());

        return new View(
                viewId,
                sourceChunkIds,
                viewType,
                version,
                content,
                Optional.empty(),
                Optional.of(qualityScore),
                List.of());
    }
}
