package io.github.rawwise.axiom.pipeline;

import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KnowledgeDataAccessor {
    private final Metastore metastore;
    private final KnowledgeStore knowledgeStore;

    public KnowledgeDataAccessor(Metastore metastore, KnowledgeStore knowledgeStore) {
        this.metastore = metastore;
        this.knowledgeStore = knowledgeStore;
    }

    public Optional<byte[]> getWithFallback(byte[] key) throws IOException {
        Optional<byte[]> result = knowledgeStore.get(key);
        if (result.isPresent()) return result;
        log.debug(
                "KnowledgeStore miss for key={}, falling back to Metastore",
                new String(key, StandardCharsets.UTF_8));
        return metastore.get(key);
    }

    public Optional<byte[]> readView(String viewId) throws IOException {
        byte[] latestKey = key("view:latest:" + viewId);
        Optional<byte[]> latest = metastore.get(latestKey);
        if (latest.isPresent()) {
            try {
                int version =
                        Integer.parseInt(new String(latest.get(), StandardCharsets.UTF_8).trim());
                if (version >= 0) {
                    byte[] versionedKey = key("view:" + viewId + ":v" + version);
                    Optional<byte[]> content = knowledgeStore.get(versionedKey);
                    if (content.isPresent()) return content;
                    content = metastore.get(versionedKey);
                    if (content.isPresent()) return content;
                }
            } catch (NumberFormatException e) {
                log.warn(
                        "Invalid view:latest pointer for viewId={}, falling back to legacy key",
                        viewId);
            }
        }
        byte[] legacyKey = key("view:" + viewId);
        Optional<byte[]> legacy = knowledgeStore.get(legacyKey);
        if (legacy.isPresent()) return legacy;
        return metastore.get(legacyKey);
    }

    public List<KeyValue> scanWithFallback(byte[] prefix) throws IOException {
        List<KeyValue> ksResults = knowledgeStore.scanAll(prefix);
        List<KeyValue> msResults = metastore.scanAll(prefix);
        if (msResults.isEmpty()) return ksResults;
        if (ksResults.isEmpty()) return msResults;
        java.util.Map<String, KeyValue> merged = new java.util.LinkedHashMap<>();
        for (KeyValue kv : msResults) {
            merged.put(new String(kv.key(), StandardCharsets.UTF_8), kv);
        }
        for (KeyValue kv : ksResults) {
            merged.put(new String(kv.key(), StandardCharsets.UTF_8), kv);
        }
        return new ArrayList<>(merged.values());
    }

    private byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
