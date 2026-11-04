package io.github.rawwise.engine.vector.onnx;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmbeddingProviderRegistry {

    public static final String CURRENT_MODEL_KEY = "embedding:model:current";
    private static final String HISTORY_KEY = "embedding:model:history";

    private static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new JavaTimeModule());
    private static final TypeReference<List<RegisteredModel>> LIST_TYPE = new TypeReference<>() {};

    private final Metastore metastore;

    public EmbeddingProviderRegistry(Metastore metastore) {
        this.metastore = metastore;
    }

    public void registerCurrentModel(String modelId, OnnxEmbeddingConfig config) {
        RegisteredModel model = new RegisteredModel(modelId, config, System.currentTimeMillis());
        try {
            metastore.put(
                    CURRENT_MODEL_KEY.getBytes(StandardCharsets.UTF_8),
                    MAPPER.writeValueAsBytes(model));
            appendToHistory(model);
            log.info(
                    "EmbeddingProviderRegistry.registerCurrentModel: modelId={} dimension={}",
                    modelId,
                    config.dimension());
        } catch (IOException e) {
            log.warn(
                    "EmbeddingProviderRegistry.registerCurrentModel failed for modelId={}: {}",
                    modelId,
                    e.getMessage());
        }
    }

    public Optional<RegisteredModel> getCurrentModel() {
        try {
            Optional<byte[]> raw =
                    metastore.get(CURRENT_MODEL_KEY.getBytes(StandardCharsets.UTF_8));
            if (raw.isEmpty()) return Optional.empty();
            return Optional.of(MAPPER.readValue(raw.get(), RegisteredModel.class));
        } catch (IOException e) {
            log.warn("EmbeddingProviderRegistry.getCurrentModel failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public List<RegisteredModel> listVersions() {
        try {
            Optional<byte[]> raw = metastore.get(HISTORY_KEY.getBytes(StandardCharsets.UTF_8));
            if (raw.isEmpty()) return Collections.emptyList();
            List<RegisteredModel> versions = MAPPER.readValue(raw.get(), LIST_TYPE);
            return Collections.unmodifiableList(versions);
        } catch (IOException e) {
            log.warn("EmbeddingProviderRegistry.listVersions failed: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    public Optional<RegisteredModel> getVersion(String modelId) {
        return listVersions().stream().filter(m -> modelId.equals(m.modelId())).findFirst();
    }

    public int versionCount() {
        return listVersions().size();
    }

    private void appendToHistory(RegisteredModel model) throws IOException {
        List<RegisteredModel> history = new ArrayList<>(listVersions());
        history.add(model);
        metastore.put(
                HISTORY_KEY.getBytes(StandardCharsets.UTF_8), MAPPER.writeValueAsBytes(history));
    }

    public record RegisteredModel(
            String modelId, OnnxEmbeddingConfig config, long registeredAtMs) {}
}
