package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.axiom.pipeline.graph.GraphStoreSchema;
import io.github.rawwise.engine.storage.KnowledgeStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KnowledgeStoreNameIndexResolver implements NameIndexResolver {

    private final KnowledgeStore knowledgeStore;

    public KnowledgeStoreNameIndexResolver(KnowledgeStore knowledgeStore) {
        this.knowledgeStore = knowledgeStore;
    }

    @Override
    public Optional<String> resolveEntityId(String sampleId, String canonicalName) {
        if (sampleId == null || canonicalName == null || knowledgeStore == null) {
            return Optional.empty();
        }
        byte[] key = GraphStoreSchema.nameToEntityKey(sampleId, canonicalName);
        try {
            Optional<byte[]> value = knowledgeStore.get(key);
            if (value.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new String(value.get(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.warn(
                    "KnowledgeStoreNameIndexResolver lookup failed sampleId={} name={}: {}",
                    sampleId,
                    canonicalName,
                    e.getMessage());
            return Optional.empty();
        }
    }
}
