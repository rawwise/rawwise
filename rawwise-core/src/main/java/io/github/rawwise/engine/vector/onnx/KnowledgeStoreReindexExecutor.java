package io.github.rawwise.engine.vector.onnx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.ShardInternalIndex;
import io.github.rawwise.engine.vector.VectorIndexManager;
import io.github.rawwise.metastore.Metastore;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KnowledgeStoreReindexExecutor implements ReindexCoordinator.ReindexExecutor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENTRY_KEY_TO_VIEW_PREFIX = "entrykey2view:";
    private static final String VIEW_LATEST_PREFIX = "view:latest:";
    private static final String VIEW_VERSIONED_PREFIX = "view:";

    private final VectorIndexManager indexManager;
    private final Supplier<EmbeddingProvider> newProviderSupplier;
    private final KnowledgeStore knowledgeStore;
    private final Metastore metastore;
    private final String columnName;

    public KnowledgeStoreReindexExecutor(
            VectorIndexManager indexManager,
            Supplier<EmbeddingProvider> newProviderSupplier,
            KnowledgeStore knowledgeStore,
            Metastore metastore,
            String columnName) {
        this.indexManager = indexManager;
        this.newProviderSupplier = newProviderSupplier;
        this.knowledgeStore = knowledgeStore;
        this.metastore = metastore;
        this.columnName = columnName;
    }

    @Override
    public void execute(ReindexCoordinator.ReindexContext context) throws Exception {
        ShardInternalIndex oldIndex = indexManager.getIndex(context.srcTableId(), columnName);
        if (oldIndex == null) {
            log.warn(
                    "KnowledgeStoreReindexExecutor: no old index for table={} column={}, nothing to reindex",
                    context.srcTableId(),
                    columnName);
            context.setTotalRows(0);
            return;
        }

        Set<String> entryKeys = oldIndex.entryKeys();
        context.setTotalRows(entryKeys.size());
        log.info(
                "KnowledgeStoreReindexExecutor: starting reindex table={} entries={} dim {}->{}",
                context.srcTableId(),
                entryKeys.size(),
                context.srcDimension(),
                context.destDimension());

        List<float[]> newVectors = new ArrayList<>(entryKeys.size());
        List<String> retainedKeys = new ArrayList<>(entryKeys.size());
        int skipped = 0;

        EmbeddingProvider provider = newProviderSupplier.get();
        if (provider == null) {
            throw new IllegalStateException(
                    "KnowledgeStoreReindexExecutor: newProviderSupplier returned null");
        }
        log.info(
                "KnowledgeStoreReindexExecutor: using provider={} for re-embedding",
                provider.modelIdentifier());

        for (String entryKey : entryKeys) {
            String text = resolveText(entryKey);
            if (text == null || text.isBlank()) {
                skipped++;
                context.incrementProcessed(1);
                continue;
            }
            try {
                float[] vec = provider.embed(text);
                if (vec != null && vec.length == context.destDimension()) {
                    newVectors.add(vec);
                    retainedKeys.add(entryKey);
                } else {
                    skipped++;
                    log.warn(
                            "KnowledgeStoreReindexExecutor: embedding dimension mismatch for entryKey={} "
                                    + "expected={} actual={}",
                            entryKey,
                            context.destDimension(),
                            vec == null ? 0 : vec.length);
                }
            } catch (RuntimeException e) {
                skipped++;
                log.warn(
                        "KnowledgeStoreReindexExecutor: embed failed for entryKey={}: {}",
                        entryKey,
                        e.getMessage());
            }
            context.incrementProcessed(1);
        }

        log.info(
                "KnowledgeStoreReindexExecutor: re-embed complete entries={} retained={} skipped={}",
                entryKeys.size(),
                retainedKeys.size(),
                skipped);

        if (retainedKeys.isEmpty()) {
            log.warn(
                    "KnowledgeStoreReindexExecutor: no vectors retained, creating empty index for table={}",
                    context.destTableId());
        }

        indexManager.rebuildIndexWithData(
                context.destTableId(),
                columnName,
                context.destDimension(),
                newVectors,
                retainedKeys);

        log.info(
                "KnowledgeStoreReindexExecutor: reindex complete table={} newIndexSize={}",
                context.destTableId(),
                retainedKeys.size());
    }

    private String resolveText(String entryKey) {
        String viewId = lookupViewId(entryKey);
        if (viewId == null) {
            log.debug("KnowledgeStoreReindexExecutor: no viewId for entryKey={}", entryKey);
            return null;
        }
        return lookupViewText(viewId);
    }

    private String lookupViewId(String entryKey) {
        if (entryKey != null && entryKey.startsWith("view:")) {
            return entryKey;
        }
        try {
            byte[] mapKey = (ENTRY_KEY_TO_VIEW_PREFIX + entryKey).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> raw = metastore.get(mapKey);
            if (raw.isPresent()) {
                return new String(raw.get(), StandardCharsets.UTF_8).trim();
            }
        } catch (Exception e) {
            log.debug(
                    "KnowledgeStoreReindexExecutor: metastore lookup failed for entryKey={}: {}",
                    entryKey,
                    e.getMessage());
        }
        return null;
    }

    private String lookupViewText(String viewId) {
        Integer version = lookupLatestVersion(viewId);
        if (version == null || version <= 0) {
            log.debug("KnowledgeStoreReindexExecutor: no version for viewId={}", viewId);
            return null;
        }
        byte[] versionedKey =
                (VIEW_VERSIONED_PREFIX + viewId + ":v" + version).getBytes(StandardCharsets.UTF_8);
        try {
            Optional<byte[]> raw = knowledgeStore.get(versionedKey);
            if (raw.isEmpty()) {
                return null;
            }
            return extractTextFromViewJson(raw.get());
        } catch (Exception e) {
            log.debug(
                    "KnowledgeStoreReindexExecutor: view lookup failed for viewId={} v={}: {}",
                    viewId,
                    version,
                    e.getMessage());
            return null;
        }
    }

    private Integer lookupLatestVersion(String viewId) {
        try {
            byte[] latestKey = (VIEW_LATEST_PREFIX + viewId).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> raw = knowledgeStore.get(latestKey);
            if (raw.isPresent()) {
                return Integer.parseInt(new String(raw.get(), StandardCharsets.UTF_8).trim());
            }
        } catch (Exception e) {
            log.debug(
                    "KnowledgeStoreReindexExecutor: version lookup failed for viewId={}: {}",
                    viewId,
                    e.getMessage());
        }
        return null;
    }

    private String extractTextFromViewJson(byte[] viewBytes) {
        try {
            JsonNode viewNode = MAPPER.readTree(viewBytes);
            JsonNode contentNode = viewNode.get("content");
            if (contentNode == null || contentNode.isNull()) {
                return null;
            }
            if (contentNode.isTextual()) {
                return contentNode.asText();
            }
            if (contentNode.isBinary()) {
                byte[] contentBytes = contentNode.binaryValue();
                return extractTextFromContentJson(contentBytes);
            }
            return contentNode.toString();
        } catch (Exception e) {
            log.debug("KnowledgeStoreReindexExecutor: view JSON parse failed: {}", e.getMessage());
            return null;
        }
    }

    private String extractTextFromContentJson(byte[] contentBytes) {
        try {
            JsonNode contentNode = MAPPER.readTree(contentBytes);
            JsonNode textNode = contentNode.get("content");
            if (textNode != null && textNode.isTextual()) {
                return textNode.asText();
            }
            return null;
        } catch (Exception e) {
            return new String(contentBytes, StandardCharsets.UTF_8);
        }
    }
}
