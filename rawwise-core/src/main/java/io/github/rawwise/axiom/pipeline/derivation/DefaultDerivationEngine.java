package io.github.rawwise.axiom.pipeline.derivation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.axiom.pipeline.CasRetryHelper;
import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.StorageWriteFacade;
import io.github.rawwise.axiom.pipeline.cognition.DerivationFeedbackCallback;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.record.Chunk;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.View;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultDerivationEngine implements DerivationEngine {

    private static final String CHAIN_KEY_PREFIX = "deriv_chain:";
    private static final String SRC_KEY_PREFIX = "deriv_src:";
    private static final String DERIVED_KEY_PREFIX = "deriv_from:";
    private static final String CHUNK_KEY_PREFIX = "chunk:";
    private static final String DERIVE_INTENT_PREFIX = "derive_intent:";
    private static final int CAS_MAX_RETRIES = 8;
    private static final long CAS_BASE_DELAY_MS = 10;
    private static final long CAS_MAX_DELAY_MS = 1000;
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new Jdk8Module())
                    .enable(SerializationFeature.INDENT_OUTPUT);

    private final StorageWriteFacade store;
    private final Metastore metastore;
    private final KnowledgeStore knowledgeStore;
    private final KnowledgeDataAccessor dataAccessor;
    private final Map<Chunk.ChunkType, ChunkingStrategy> strategies;
    private final Map<String, ChunkingStrategy> strategiesByName;
    private final Map<String, String> tableStrategyBindings = new ConcurrentHashMap<>();

    @Setter private DerivationFeedbackCallback derivationFeedbackCallback;

    public DefaultDerivationEngine(
            StorageWriteFacade store, Metastore metastore, KnowledgeStore knowledgeStore) {
        this.store = store;
        this.metastore = metastore;
        this.knowledgeStore = knowledgeStore;
        this.dataAccessor = new KnowledgeDataAccessor(metastore, knowledgeStore);
        this.strategies = new ConcurrentHashMap<>();
        this.strategiesByName = new ConcurrentHashMap<>();
        ParagraphChunkingStrategy paragraph = new ParagraphChunkingStrategy();
        strategies.put(Chunk.ChunkType.TEXT_SEGMENT, paragraph);
        strategiesByName.put(paragraph.name(), paragraph);
        CodeBlockChunkingStrategy codeBlock = new CodeBlockChunkingStrategy();
        strategies.put(Chunk.ChunkType.CODE_BLOCK, codeBlock);
        strategiesByName.put(codeBlock.name(), codeBlock);
        SentenceChunkingStrategy sentence = new SentenceChunkingStrategy();
        strategiesByName.put(sentence.name(), sentence);
        PatternBasedChunkingStrategy pattern = new PatternBasedChunkingStrategy();
        strategiesByName.put(pattern.name(), pattern);
        TableRowChunkingStrategy tableRow = new TableRowChunkingStrategy();
        strategies.put(Chunk.ChunkType.TABLE_ROW, tableRow);
        strategiesByName.put(tableRow.name(), tableRow);
        SlidingWindowChunkingStrategy slidingWindow = new SlidingWindowChunkingStrategy();
        strategiesByName.put(slidingWindow.name(), slidingWindow);
        loadStrategiesViaSpi();
    }

    private void loadStrategiesViaSpi() {
        java.util.ServiceLoader<ChunkingStrategy> loader =
                java.util.ServiceLoader.load(ChunkingStrategy.class);
        for (ChunkingStrategy s : loader) {
            try {
                registerStrategy(s);
                log.debug("Loaded ChunkingStrategy via SPI: {}", s.name());
            } catch (Exception e) {
                log.warn("Failed to load ChunkingStrategy via SPI: {}", e.getMessage(), e);
            }
        }
    }

    public void registerStrategy(ChunkingStrategy strategy) {
        strategies.put(strategy.supportedType(), strategy);
        strategiesByName.put(strategy.name(), strategy);
    }

    public ChunkingStrategy selectStrategy(String name) {
        if (name == null || name.isBlank()) return null;
        return strategiesByName.get(name);
    }

    public void bindTableStrategy(String tableId, String strategyName) {
        if (strategyName == null || strategyName.isBlank()) {
            tableStrategyBindings.remove(tableId);
        } else {
            tableStrategyBindings.put(tableId, strategyName);
        }
    }

    public ChunkingStrategy selectStrategy(Record record, String tableId, Chunk.ChunkType type) {
        if (tableId != null) {
            String boundName = tableStrategyBindings.get(tableId);
            if (boundName != null) {
                ChunkingStrategy s = strategiesByName.get(boundName);
                if (s != null && s.appliesTo(record, tableId)) {
                    return s;
                }
            }
        }
        ChunkingStrategy defaultStrategy = getDefaultStrategy(type);
        if (defaultStrategy != null && defaultStrategy.appliesTo(record, tableId)) {
            return defaultStrategy;
        }
        return new ParagraphChunkingStrategy();
    }

    private ChunkingStrategy getDefaultStrategy(Chunk.ChunkType type) {
        String defaultName =
                switch (type) {
                    case TEXT_SEGMENT -> "paragraph";
                    case CODE_BLOCK -> "code-block";
                    case TABLE_ROW -> "table-row";
                    case IMAGE_REGION -> null;
                };
        if (defaultName == null) return null;
        return strategiesByName.get(defaultName);
    }

    public void clearDeriveIntent(Record.Key rawKey) {
        String rawId = rawKeyId(rawKey);
        byte[] intentKey =
                (DERIVE_INTENT_PREFIX + "CHUNK:" + rawId).getBytes(StandardCharsets.UTF_8);
        cleanupIntentKey(intentKey);
    }

    @Override
    public void clearAllDeriveIntents(Record.Key rawKey) {
        String rawId = rawKeyId(rawKey);
        cleanupIntentKey(
                (DERIVE_INTENT_PREFIX + "CHUNK:" + rawId).getBytes(StandardCharsets.UTF_8));
        cleanupIntentKey((DERIVE_INTENT_PREFIX + "VIEW:" + rawId).getBytes(StandardCharsets.UTF_8));
        cleanupIntentKey(
                (DERIVE_INTENT_PREFIX + "EMBEDDING:" + rawId).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean isChunkDerived(Record.Key rawKey) {
        String rawId = rawKeyId(rawKey);
        byte[] intentKey =
                (DERIVE_INTENT_PREFIX + "CHUNK:" + rawId).getBytes(StandardCharsets.UTF_8);
        try {
            return metastore.get(intentKey).isPresent();
        } catch (IOException e) {
            log.warn(
                    "Failed to check chunk derive intent for rawKey={}: {}",
                    rawId,
                    e.getMessage(),
                    e);
            return false;
        }
    }

    @Override
    public int deleteChunks(Record.Key rawKey) {
        String rawId = rawKeyId(rawKey);
        int deleted = 0;
        try {
            List<DerivationChain> chains = getChain(rawId, Layer.RAW);
            for (DerivationChain chain : chains) {
                if (chain.derivedLayer() == Layer.CHUNK) {
                    byte[] chunkKey =
                            (CHUNK_KEY_PREFIX + chain.derivedId()).getBytes(StandardCharsets.UTF_8);
                    knowledgeStore.delete(chunkKey);
                    deleted++;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to delete chunks for rawKey={}: {}", rawId, e.getMessage(), e);
        }
        return deleted;
    }

    @Override
    public Chunk.ChunkType resolveChunkType(String strategyName, Chunk.ChunkType fallback) {
        ChunkingStrategy s = selectStrategy(strategyName);
        return s != null ? s.supportedType() : fallback;
    }

    @Override
    public List<Chunk> deriveChunks(
            Record.Key rawKey, Chunk.ChunkType type, Map<String, Object> params) {
        return deriveChunks(rawKey, type, null, params);
    }

    @Override
    public List<Chunk> deriveChunks(
            Record.Key rawKey,
            Chunk.ChunkType type,
            String strategyName,
            Map<String, Object> params) {
        String rawId = rawKeyId(rawKey);

        byte[] intentKey =
                (DERIVE_INTENT_PREFIX + "CHUNK:" + rawId).getBytes(StandardCharsets.UTF_8);
        boolean claimed =
                CasRetryHelper.casWithRetry(
                        metastore,
                        List.of(new Precondition.NotExists(intentKey)),
                        List.of(
                                new Mutation.Put(
                                        intentKey,
                                        ("agent:" + Thread.currentThread().getName())
                                                .getBytes(StandardCharsets.UTF_8))),
                        "derive_intent:CHUNK:" + rawId,
                        CAS_MAX_RETRIES,
                        CAS_BASE_DELAY_MS,
                        CAS_MAX_DELAY_MS);
        if (!claimed) {
            log.debug("Chunks already derived or in-progress for rawKey={}", rawId);
            return Collections.emptyList();
        }

        boolean success = false;
        try {
            byte[] content =
                    params.get("renderedContent") instanceof byte[] rendered
                            ? rendered
                            : readRawContent(rawKey, params);
            if (content == null) {
                log.debug("No content found for rawKey={}", rawId);
                return Collections.emptyList();
            }

            List<byte[]> segments = splitContent(content, type, strategyName, params);
            if (segments.isEmpty()) {
                return Collections.emptyList();
            }

            List<Chunk> chunks = new ArrayList<>();
            List<String> chunkIds = new ArrayList<>();
            AtomicInteger seq = new AtomicInteger(0);
            long now = System.currentTimeMillis();

            for (byte[] segment : segments) {
                String chunkId = "chunk:" + rawId + ":" + seq.getAndIncrement();
                chunkIds.add(chunkId);
                Chunk chunk = new Chunk(chunkId, rawKey, type, segment);
                chunks.add(chunk);

                persistChunk(chunk);

                DerivationChain chain =
                        new DerivationChain(
                                rawId,
                                Layer.RAW,
                                chunkId,
                                Layer.CHUNK,
                                "CHUNK_" + type.name(),
                                now);
                appendChain(chain);
            }

            log.debug(
                    "Derived {} chunks from rawKey={} strategy={}",
                    chunks.size(),
                    rawId,
                    strategyName != null ? strategyName : "default");

            if (derivationFeedbackCallback != null && !chunks.isEmpty()) {
                derivationFeedbackCallback.onDeriveBatch(chunkIds, Layer.CHUNK, 1.0);
                double overallScore = computeChunkQuality(chunks);
                derivationFeedbackCallback.onDerive(rawKey.toString(), Layer.CHUNK, overallScore);
            }

            success = true;
            return chunks;
        } finally {
            if (!success) {
                cleanupIntentKey(intentKey);
            }
        }
    }

    @Override
    public View deriveView(
            List<String> chunkIds,
            View.ViewType type,
            String modelVersion,
            Map<String, Object> params) {
        String viewId = "view:" + UUID.randomUUID().toString().substring(0, 8);

        byte[] content = buildViewContent(chunkIds, type, params);

        int newVersion = getCurrentViewVersion(viewId) + 1;

        View view =
                new View(
                        viewId,
                        chunkIds,
                        type,
                        newVersion,
                        content,
                        Optional.empty(),
                        Optional.empty(),
                        List.of());

        try {
            byte[] versionedKey =
                    ("view:" + viewId + ":v" + newVersion).getBytes(StandardCharsets.UTF_8);
            knowledgeStore.put(versionedKey, MAPPER.writeValueAsBytes(view));

            byte[] latestKey = ("view:latest:" + viewId).getBytes(StandardCharsets.UTF_8);
            casUpdateViewLatest(latestKey, newVersion);
        } catch (IOException e) {
            log.error("Failed to persist versioned view {}: {}", viewId, e.getMessage(), e);
        }

        long now = System.currentTimeMillis();
        for (String chunkId : chunkIds) {
            DerivationChain chain =
                    new DerivationChain(
                            chunkId, Layer.CHUNK, viewId, Layer.VIEW, "DERIVE_" + type.name(), now);
            appendChain(chain);
        }

        log.debug("Derived view {} v{} from {} chunks", viewId, newVersion, chunkIds.size());
        return view;
    }

    @Override
    public List<DerivationChain> getChain(String entityId) {
        try {
            byte[] key = chainKey(entityId);
            Optional<byte[]> raw = metastore.get(key);
            if (raw.isEmpty()) {
                return List.of();
            }
            return MAPPER.readValue(raw.get(), new TypeReference<List<DerivationChain>>() {});
        } catch (IOException e) {
            log.debug(
                    "Failed to read derivation chain for entityId={}: {}",
                    entityId,
                    e.getMessage());
            return List.of();
        }
    }

    @Override
    public List<DerivationChain> getChain(String sourceId, Layer sourceLayer) {
        try {
            byte[] key = srcKey(sourceId, sourceLayer);
            Optional<byte[]> raw = metastore.get(key);
            if (raw.isEmpty()) {
                return List.of();
            }
            return MAPPER.readValue(raw.get(), new TypeReference<List<DerivationChain>>() {});
        } catch (IOException e) {
            log.warn(
                    "Failed to read derivation chain for sourceId={}, layer={}: {}",
                    sourceId,
                    sourceLayer,
                    e.getMessage(),
                    e);
            return List.of();
        }
    }

    @Override
    public List<DerivationChain> getDerivedFrom(String derivedId, Layer derivedLayer) {
        try {
            byte[] key = derivedKey(derivedId, derivedLayer);
            Optional<byte[]> raw = metastore.get(key);
            if (raw.isEmpty()) {
                return List.of();
            }
            return MAPPER.readValue(raw.get(), new TypeReference<List<DerivationChain>>() {});
        } catch (IOException e) {
            log.warn(
                    "Failed to read derivation chain for derivedId={}, layer={}: {}",
                    derivedId,
                    derivedLayer,
                    e.getMessage(),
                    e);
            return List.of();
        }
    }

    private void appendChain(DerivationChain chain) {
        try {
            appendToEntityChain(chain);
            appendToSourceIndex(chain);
            appendToDerivedIndex(chain);
        } catch (IOException e) {
            log.warn(
                    "Failed to append derivation chain for {} -> {}: {}",
                    chain.sourceId(),
                    chain.derivedId(),
                    e.getMessage(),
                    e);
        }
    }

    private void appendToEntityChain(DerivationChain chain) throws IOException {
        byte[] key = chainKey(chain.derivedId());
        casAppendChain(key, chain);
    }

    private void appendToSourceIndex(DerivationChain chain) throws IOException {
        byte[] key = srcKey(chain.sourceId(), chain.sourceLayer());
        casAppendChain(key, chain);
    }

    private void appendToDerivedIndex(DerivationChain chain) throws IOException {
        byte[] key = derivedKey(chain.derivedId(), chain.derivedLayer());
        casAppendChain(key, chain);
    }

    private void casAppendChain(byte[] key, DerivationChain chain) throws IOException {
        for (int attempt = 0; attempt < CAS_MAX_RETRIES; attempt++) {
            Optional<byte[]> current = metastore.get(key);
            List<DerivationChain> list =
                    current.isEmpty()
                            ? new ArrayList<>()
                            : new ArrayList<>(
                                    MAPPER.readValue(
                                            current.get(),
                                            new TypeReference<List<DerivationChain>>() {}));
            list.add(chain);
            byte[] newValue = MAPPER.writeValueAsBytes(list);

            List<Precondition> preconditions =
                    current.isPresent()
                            ? List.of(new Precondition.ValueEquals(key, current.get()))
                            : List.of(new Precondition.NotExists(key));
            boolean success =
                    metastore.compareAndSwap(
                            preconditions, List.of(new Mutation.Put(key, newValue)));
            if (success) return;
            log.debug(
                    "CAS append retry {}/{} for key={}",
                    attempt + 1,
                    CAS_MAX_RETRIES,
                    new String(key, StandardCharsets.UTF_8));
            casRetrySleep(attempt);
        }
        log.warn(
                "Derivation chain append CAS retries exhausted for key={}",
                new String(key, StandardCharsets.UTF_8));
        throw new IOException(
                "Derivation chain append CAS retries exhausted for key="
                        + new String(key, StandardCharsets.UTF_8));
    }

    private void casRetrySleep(int attempt) {
        try {
            long expDelay =
                    Math.min(CAS_BASE_DELAY_MS * (1L << Math.min(attempt, 30)), CAS_MAX_DELAY_MS);
            Thread.sleep(expDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void casUpdateViewLatest(byte[] latestKey, int newVersion) throws IOException {
        for (int attempt = 0; attempt < CAS_MAX_RETRIES; attempt++) {
            Optional<byte[]> current = metastore.get(latestKey);
            int currentVersion =
                    current.isEmpty()
                            ? 0
                            : Integer.parseInt(
                                    new String(current.get(), StandardCharsets.UTF_8).trim());
            if (newVersion <= currentVersion) break;

            List<Precondition> preconditions =
                    current.isPresent()
                            ? List.of(new Precondition.ValueEquals(latestKey, current.get()))
                            : List.of(new Precondition.NotExists(latestKey));
            boolean success =
                    metastore.compareAndSwap(
                            preconditions,
                            List.of(
                                    new Mutation.Put(
                                            latestKey,
                                            String.valueOf(newVersion)
                                                    .getBytes(StandardCharsets.UTF_8))));
            if (success) return;
            log.debug(
                    "CAS view:latest retry {}/{} for key={}",
                    attempt + 1,
                    CAS_MAX_RETRIES,
                    new String(latestKey, StandardCharsets.UTF_8));
            casRetrySleep(attempt);
        }
        log.warn(
                "View latest CAS retries exhausted for key={}",
                new String(latestKey, StandardCharsets.UTF_8));
    }

    private void cleanupIntentKey(byte[] intentKey) {
        try {
            metastore.delete(intentKey);
        } catch (IOException e) {
            log.warn("Failed to cleanup intent key: {}", e.getMessage(), e);
        }
    }

    private byte[] readRawContent(Record.Key rawKey, Map<String, Object> params) {
        String tableIdentifier = null;
        if (params != null) {
            Object tid = params.get("tableIdentifier");
            if (tid instanceof String s && !s.isBlank()) {
                tableIdentifier = s;
            }
        }
        if (tableIdentifier == null) {
            log.warn(
                    "readRawContent: no tableIdentifier in params, falling back to numericId=0 lookup (may return wrong table if multiple tables exist)");
            tableIdentifier = store.findTableIdentifierByNumericId(0);
        }
        if (tableIdentifier == null) {
            log.debug("No tables registered, trying all table stores");
            return readRawFromAllTables(rawKey);
        }
        return readRawFromTable(rawKey, tableIdentifier);
    }

    private byte[] readRawFromTable(Record.Key rawKey, String tableIdentifier) {
        try {
            TableStore ts = store.tableStore(tableIdentifier);
            byte[] encodedKey = rawKey.encoded();
            byte[] rawValue = ts.getRawValue(encodedKey);
            if (rawValue == null || NullableValueCodec.isTombstone(rawValue)) {
                return null;
            }
            byte[] decoded = NullableValueCodec.decode(rawValue);
            if (decoded == null) {
                return null;
            }
            var recordCodec = ts.getRecordCodec();
            if (recordCodec != null && recordCodec.isRecordCodecFormat(decoded)) {
                var schemaResolver = ts.getSchemaResolver();
                Record record = recordCodec.decode(decoded, schemaResolver);
                return recordToContent(record);
            }
            return decoded;
        } catch (Exception e) {
            log.debug(
                    "Failed to read raw content from table {}: {}",
                    tableIdentifier,
                    e.getMessage());
            return null;
        }
    }

    private byte[] readRawFromAllTables(Record.Key rawKey) {
        for (String tableIdentifier : store.getAllTableIdentifiers()) {
            byte[] content = readRawFromTable(rawKey, tableIdentifier);
            if (content != null) {
                return content;
            }
        }
        return null;
    }

    private byte[] recordToContent(Record record) {
        try {
            Map<String, Object> map = record.body().toMap();
            Object body = map.get("body");
            if (body instanceof String s) {
                try {
                    return Base64.getDecoder().decode(s);
                } catch (IllegalArgumentException e) {
                    return s.getBytes(StandardCharsets.UTF_8);
                }
            }
            return MAPPER.writeValueAsBytes(map);
        } catch (Exception e) {
            log.debug("Failed to serialize record body: {}", e.getMessage());
            return null;
        }
    }

    private List<byte[]> splitContent(
            byte[] content, Chunk.ChunkType type, String strategyName, Map<String, Object> params) {
        ChunkingStrategy named = selectStrategy(strategyName);
        if (named != null) {
            return named.split(content, params);
        }
        if (strategyName != null && !strategyName.isBlank()) {
            log.warn(
                    "Strategy '{}' not registered, falling back to type-based selection",
                    strategyName);
        }
        ChunkingStrategy byType = getDefaultStrategy(type);
        if (byType != null) {
            return byType.split(content, params);
        }
        return switch (type) {
            case TEXT_SEGMENT -> splitText(content, params);
            case TABLE_ROW -> splitByLines(content);
            case CODE_BLOCK, IMAGE_REGION -> List.of(content);
        };
    }

    private List<byte[]> splitText(byte[] content, Map<String, Object> params) {
        String text = new String(content, StandardCharsets.UTF_8);
        int maxChunkSize = getIntParam(params, "maxChunkSize", 1024);

        List<byte[]> chunks = new ArrayList<>();
        String[] paragraphs = text.split("\n\n");
        StringBuilder current = new StringBuilder();

        for (String para : paragraphs) {
            if (current.length() + para.length() > maxChunkSize && current.length() > 0) {
                chunks.add(current.toString().getBytes(StandardCharsets.UTF_8));
                current = new StringBuilder();
            }
            if (current.length() > 0) {
                current.append("\n\n");
            }
            current.append(para);
        }
        if (current.length() > 0) {
            chunks.add(current.toString().getBytes(StandardCharsets.UTF_8));
        }
        return chunks;
    }

    private List<byte[]> splitByLines(byte[] content) {
        String text = new String(content, StandardCharsets.UTF_8);
        String[] lines = text.split("\n");
        List<byte[]> chunks = new ArrayList<>();
        for (String line : lines) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                chunks.add(trimmed.getBytes(StandardCharsets.UTF_8));
            }
        }
        return chunks;
    }

    private void persistChunk(Chunk chunk) {
        try {
            byte[] key = (CHUNK_KEY_PREFIX + chunk.chunkId()).getBytes(StandardCharsets.UTF_8);
            knowledgeStore.put(key, chunk.content());
        } catch (IOException e) {
            log.error("Failed to persist chunk {}: {}", chunk.chunkId(), e.getMessage(), e);
        }
    }

    private byte[] buildViewContent(
            List<String> chunkIds, View.ViewType type, Map<String, Object> params) {
        try {
            Map<String, Object> meta = new java.util.LinkedHashMap<>();
            meta.put("sourceChunkIds", chunkIds);
            meta.put("viewType", type.name());
            meta.put("chunkCount", chunkIds.size());

            StringBuilder contentText = new StringBuilder();
            for (String chunkId : chunkIds) {
                byte[] chunkKey = (CHUNK_KEY_PREFIX + chunkId).getBytes(StandardCharsets.UTF_8);
                var raw = dataAccessor.getWithFallback(chunkKey);

                if (raw.isPresent()) {
                    contentText.append(new String(raw.get(), StandardCharsets.UTF_8));
                    contentText.append("\n");
                }
            }

            if (contentText.length() > 0) {
                meta.put("content", contentText.toString());
                meta.put("contentLength", contentText.length());
            }

            if (params != null) {
                meta.put("params", params);
            }
            return MAPPER.writeValueAsBytes(meta);
        } catch (Exception e) {
            log.warn("Failed to build view content: {}", e.getMessage(), e);
            return "{}".getBytes(StandardCharsets.UTF_8);
        }
    }

    private int getCurrentViewVersion(String viewId) {
        try {
            Optional<byte[]> latestBytes =
                    metastore.get(("view:latest:" + viewId).getBytes(StandardCharsets.UTF_8));
            if (latestBytes.isPresent()) {
                return Integer.parseInt(new String(latestBytes.get(), StandardCharsets.UTF_8));
            }
            Optional<byte[]> legacyBytes =
                    dataAccessor.getWithFallback(
                            ("view:" + viewId).getBytes(StandardCharsets.UTF_8));
            return legacyBytes.isPresent() ? 1 : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    private static String rawKeyId(Record.Key key) {
        return "raw:" + Base64.getEncoder().encodeToString(key.encoded());
    }

    private static byte[] chainKey(String entityId) {
        return (CHAIN_KEY_PREFIX + entityId).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] srcKey(String sourceId, Layer sourceLayer) {
        return (SRC_KEY_PREFIX + sourceLayer.name() + ":" + sourceId)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] derivedKey(String derivedId, Layer derivedLayer) {
        return (DERIVED_KEY_PREFIX + derivedLayer.name() + ":" + derivedId)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static int getIntParam(Map<String, Object> params, String key, int defaultValue) {
        Object val = params != null ? params.get(key) : null;
        if (val instanceof Number n) {
            return n.intValue();
        }
        return defaultValue;
    }

    private double computeChunkQuality(List<Chunk> chunks) {
        if (chunks == null || chunks.isEmpty()) return 0.0;
        int nonEmpty = 0;
        for (Chunk c : chunks) {
            if (c.content() != null && c.content().length > 0) nonEmpty++;
        }
        return (double) nonEmpty / chunks.size();
    }
}
