package io.github.rawwise.axiom.pipeline.derivation;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.memory.SummarizationService;
import io.github.rawwise.axiom.memory.ViewMetadata;
import io.github.rawwise.axiom.memory.ViewMetadataStore;
import io.github.rawwise.axiom.pipeline.StorageWriteFacade;
import io.github.rawwise.axiom.pipeline.feedback.FeedbackLoopCloser;
import io.github.rawwise.backend.shared.BackendSharedRegistry;
import io.github.rawwise.engine.storage.PostWriteHook;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.model.record.Chunk;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.SemanticRecord;
import io.github.rawwise.model.record.View;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AutoDerivationHook implements PostWriteHook {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern PURE_NUMERIC = Pattern.compile("^[\\d.\\-+eE,\\s]+$");
    private static final double BINARY_SUSPECT_RATIO = 0.3;
    private static final String DERIV_INCOMPLETE_PREFIX = "deriv_incomplete:";

    private static final boolean L0_SUMMARIZATION_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.l0summarization.enabled", "false"));
    private static final long L0_SUMMARIZATION_TIMEOUT_MS =
            Long.parseLong(System.getProperty("rawwise.axiom.l0summarization.timeout-ms", "30000"));

    // Strategy D: Mem0-style fact-level extraction
    // each dia uses LLM to extract N independent facts,each has independent embedding coexisting
    // with original text embedding
    // entry key: rec:<raw Key>#fact:<idx> (extract Raw Key strips #fact: suffix)
    private static final boolean FACT_EXTRACTION_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.fact.extraction.enabled", "false"));
    private static final int FACT_MAX_PER_RECORD =
            Integer.parseInt(System.getProperty("rawwise.axiom.fact.max-per-record", "5"));
    private static final long FACT_EXTRACTION_TIMEOUT_MS =
            Long.parseLong(System.getProperty("rawwise.axiom.fact.extraction.timeout-ms", "30000"));
    private static final int FACT_MIN_LEN =
            Integer.parseInt(System.getProperty("rawwise.axiom.fact.min-len", "8"));

    /**
     * Strategy H: only for source text length ≥ MIN_SOURCE_LEN dia extract fact. short dia(single
     * utterance)does not split,avoid breaking original text recall.
     */
    private static final int FACT_MIN_SOURCE_LEN =
            Integer.parseInt(System.getProperty("rawwise.axiom.fact.min-source-len", "0"));

    // async fact extraction executor(avoid blocking the ingest main path)
    // concurrency 2 prevents LLM API rate limiting (429 Too Many Requests)
    private static final java.util.concurrent.ExecutorService FACT_EXECUTOR =
            FACT_EXTRACTION_ENABLED
                    ? java.util.concurrent.Executors.newFixedThreadPool(
                            Integer.parseInt(
                                    System.getProperty("rawwise.axiom.fact.concurrency", "2")),
                            r -> {
                                Thread t = new Thread(r, "fact-extractor");
                                t.setDaemon(true);
                                return t;
                            })
                    : null;

    // ===== Session-Level Semantic Derivation(general hierarchical memory capability)=====
    // when a group of related records(same session/conversation)after writing,auto-derive SEMANTIC
    // layer summary.
    // summary as independent entry index,serves as boost signal during searchenhances recall of
    // same-group original entries.
    // trigger condition:record properties contains session_id(or sample_id + session_num
    // combination).
    // debounce:same session record write interval < DELAY_MS resets timer,avoid frequent LLM calls.
    private static final boolean SEMANTIC_SESSION_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.semantic.session.enabled", "false"));
    private static final boolean SEMANTIC_SESSION_FORCE_REGEN =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.semantic.session.force-regen", "false"));
    private static final long SEMANTIC_SESSION_DELAY_MS =
            Long.parseLong(System.getProperty("rawwise.axiom.semantic.session.delay-ms", "15000"));
    private static final long SEMANTIC_SESSION_TIMEOUT_MS =
            Long.parseLong(
                    System.getProperty("rawwise.axiom.semantic.session.timeout-ms", "60000"));
    private static final int SEMANTIC_SESSION_MIN_RECORDS =
            Integer.parseInt(System.getProperty("rawwise.axiom.semantic.session.min-records", "2"));
    private static final java.util.concurrent.ScheduledExecutorService SEMANTIC_SCHEDULER =
            SEMANTIC_SESSION_ENABLED
                    ? java.util.concurrent.Executors.newScheduledThreadPool(
                            2,
                            r -> {
                                Thread t = new Thread(r, "semantic-session-deriver");
                                t.setDaemon(true);
                                return t;
                            })
                    : null;

    // ===== Cognitive Stage (View → Semantic → Action Plan) =====
    // async trigger, never blocks ingest main path.after View generation, derive Semantic and CHAIN
    // Action Plan,
    // and persist Action Plan to Knowledge Store (key=action:plan:{plan Id}).
    // cognition Engine when null or derivation fails, only warn and continue.
    private static final boolean COGNITION_STAGE_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.cognition.stage.enabled", "true"));
    private static final java.util.concurrent.ExecutorService COGNITION_EXECUTOR =
            COGNITION_STAGE_ENABLED
                    ? java.util.concurrent.Executors.newThreadPerTaskExecutor(
                            Thread.ofVirtual().name("cognitive-stage-", 0).factory())
                    : null;

    // ===== Async Derivation Queue (Chunk→View→Embedding→JVector→Keyword off the ingest path) =====
    // Raw→LMDB write stays synchronous (strong consistency); the heavy derivation chain is
    // submitted to Async Derivation Queue and runs on virtual threads. Backpressure via bounded
    // semaphore (Caller Runs Policy when full). Watermark tracks derivation progress per table.
    // Set rawwise.axiom.derivation.async=false to revert to fully synchronous derivation.
    private static final boolean ASYNC_DERIVATION_ENABLED =
            Boolean.parseBoolean(System.getProperty("rawwise.axiom.derivation.async", "true"));
    private static final int ASYNC_DERIVATION_QUEUE_SIZE =
            Integer.parseInt(
                    System.getProperty("rawwise.axiom.derivation.async.queue-size", "10000"));

    private static final boolean PER_CHUNK_EMBEDDING_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.per-chunk-embedding.enabled", "false"));

    private final DerivationEngine derivationEngine;
    private final VectorIndex vectorIndex;
    private final EmbeddingProvider embeddingProvider;
    private final KeywordIndex keywordIndex;
    private final FeedbackLoopCloser feedbackLoopCloser;
    private final StorageWriteFacade store;
    private final io.github.rawwise.axiom.pipeline.entity.EntityDerivationStage entityStage;
    private volatile SummarizationService summarizationService;
    private volatile ViewMetadataStore viewMetadataStore;
    private volatile io.github.rawwise.axiom.pipeline.cognition.CognitionEngine cognitionEngine;
    private volatile io.github.rawwise.engine.storage.KnowledgeStore cognitiveKnowledgeStore;
    // session semantic derivation debounce: session Id → pending Scheduled Future
    private final Map<String, java.util.concurrent.ScheduledFuture<?>> pendingSemanticTasks =
            new java.util.concurrent.ConcurrentHashMap<>();

    public AutoDerivationHook(
            DerivationEngine derivationEngine,
            VectorIndex vectorIndex,
            EmbeddingProvider embeddingProvider,
            KeywordIndex keywordIndex,
            FeedbackLoopCloser feedbackLoopCloser,
            StorageWriteFacade store) {
        this(
                derivationEngine,
                vectorIndex,
                embeddingProvider,
                keywordIndex,
                feedbackLoopCloser,
                store,
                null);
    }

    public AutoDerivationHook(
            DerivationEngine derivationEngine,
            VectorIndex vectorIndex,
            EmbeddingProvider embeddingProvider,
            KeywordIndex keywordIndex,
            FeedbackLoopCloser feedbackLoopCloser,
            StorageWriteFacade store,
            io.github.rawwise.axiom.pipeline.entity.EntityDerivationStage entityStage) {
        this.derivationEngine = derivationEngine;
        this.vectorIndex = vectorIndex;
        this.embeddingProvider = embeddingProvider;
        this.keywordIndex = keywordIndex;
        this.feedbackLoopCloser = feedbackLoopCloser;
        this.store = store;
        this.entityStage = entityStage;
    }

    public void setSummarizationService(SummarizationService svc) {
        this.summarizationService = svc;
        log.info(
                "AutoDerivationHook: SummarizationService injected (L0 summarization enabled={})",
                L0_SUMMARIZATION_ENABLED);
    }

    public void setViewMetadataStore(ViewMetadataStore viewMetadataStore) {
        this.viewMetadataStore = viewMetadataStore;
        log.info("AutoDerivationHook: ViewMetadataStore injected");
    }

    public void setCognitionEngine(
            io.github.rawwise.axiom.pipeline.cognition.CognitionEngine engine) {
        this.cognitionEngine = engine;
        log.info(
                "AutoDerivationHook: CognitionEngine injected (cognition.stage.enabled={})",
                COGNITION_STAGE_ENABLED);
    }

    public void setKnowledgeStore(io.github.rawwise.engine.storage.KnowledgeStore knowledgeStore) {
        this.cognitiveKnowledgeStore = knowledgeStore;
        log.info("AutoDerivationHook: KnowledgeStore injected for CognitiveStage persistence");
    }

    private volatile AsyncDerivationQueue asyncDerivationQueue;
    private volatile SemanticPatternRenderer patternRenderer = new DefaultSemanticPatternRenderer();

    public void setAsyncDerivationQueue(AsyncDerivationQueue queue) {
        this.asyncDerivationQueue = queue;
        log.info(
                "AutoDerivationHook: AsyncDerivationQueue injected (async={}, queueSize={})",
                ASYNC_DERIVATION_ENABLED,
                ASYNC_DERIVATION_QUEUE_SIZE);
    }

    public void setPatternRenderer(SemanticPatternRenderer renderer) {
        if (renderer != null) {
            this.patternRenderer = renderer;
            log.info("AutoDerivationHook: SemanticPatternRenderer injected");
        }
    }

    private SummarizationService resolveSummarizationService() {
        if (this.summarizationService != null) {
            return this.summarizationService;
        }
        return BackendSharedRegistry.getSummarizationService();
    }

    @Override
    public void afterWrite(SemanticRecord record, String tableIdentifier) {
        if (record == null || record.key() == null) return;
        try {
            log.info(
                    "afterWrite called: table={} SEMANTIC_SESSION_ENABLED={} recordKey={}",
                    tableIdentifier,
                    SEMANTIC_SESSION_ENABLED,
                    record.key());
            // SEMANTIC session derivation does not depend on table ai-native flag,
            // only needs Summarization Service + embedding.
            // SEMANTIC_SESSION_ENABLED but table is not ai-native,skip regular derivation but still
            // execute SEMANTIC session.
            boolean derivationEnabled = shouldTriggerDerivation(tableIdentifier);
            if (!derivationEnabled && !SEMANTIC_SESSION_ENABLED) {
                return;
            }

            String summaryText = extractDerivableText(record);
            if (summaryText == null) {
                if (derivationEnabled) {
                    log.info(
                            "Auto-derivation skipped: no derivable text for table={} recordKey={}",
                            tableIdentifier,
                            record.key());
                }
                return;
            }
            // save original text(untokenized),for extract Session Id regex matching
            final String rawText = summaryText;

            // SEMANTIC session tracking: execute before any embedding call,avoid embedding failure
            // causing SEMANTIC session skip
            if (SEMANTIC_SESSION_ENABLED && SEMANTIC_SCHEDULER != null) {
                final String sessionId = extractSessionId(record, rawText);
                log.info("SEMANTIC session: sessionId={} for table={}", sessionId, tableIdentifier);
                if (sessionId != null) {
                    final String fEntryKey = String.valueOf(record.key());
                    final String fText = summaryText;
                    final String fTableId = tableIdentifier;
                    SEMANTIC_SCHEDULER.submit(
                            () -> {
                                try {
                                    appendToSessionBuffer(fTableId, sessionId, fEntryKey, fText);
                                    scheduleSessionSemanticDerivation(fTableId, sessionId);
                                } catch (Exception ex) {
                                    log.error(
                                            "SEMANTIC session buffer/schedule FAILED sessionId={}: {}",
                                            sessionId,
                                            ex.getMessage(),
                                            ex);
                                }
                            });
                }
            }

            if (!derivationEnabled) {
                // table ai-native:SEMANTIC session tracking already completed above,skip other
                // derivations
                return;
            }

            log.info(
                    "afterWrite: derivationEnabled={} ASYNC_DERIVATION_ENABLED={} asyncDerivationQueue={}",
                    derivationEnabled,
                    ASYNC_DERIVATION_ENABLED,
                    asyncDerivationQueue != null ? "present" : "null");
            if (ASYNC_DERIVATION_ENABLED && asyncDerivationQueue != null) {
                asyncDerivationQueue.submit(
                        () -> {
                            try {
                                log.info(
                                        "runDerivationChain STARTED (async) table={} recordKey={}",
                                        tableIdentifier,
                                        record.key());
                                runDerivationChain(record, tableIdentifier, rawText, true);
                                String recordKeyStr =
                                        new String(record.key().encoded(), StandardCharsets.UTF_8);
                                asyncDerivationQueue.updateWatermark(tableIdentifier, recordKeyStr);
                            } catch (Throwable ex) {
                                log.error(
                                        "Async derivation chain FAILED (caught Throwable) table={} recordKey={} type={}: {}",
                                        tableIdentifier,
                                        record.key(),
                                        ex.getClass().getName(),
                                        ex.getMessage(),
                                        ex);
                                markDerivIncomplete(
                                        tableIdentifier,
                                        record.key(),
                                        "async:" + ex.getClass().getSimpleName());
                            }
                        });
            } else {
                log.info(
                        "runDerivationChain STARTED (sync) table={} recordKey={}",
                        tableIdentifier,
                        record.key());
                runDerivationChain(record, tableIdentifier, rawText, false);
            }

        } catch (Exception e) {
            log.error(
                    "Auto-derivation FAILED (uncaught) for table={} recordKey={}: {}",
                    tableIdentifier,
                    record.key(),
                    e.getMessage(),
                    e);
            markDerivIncomplete(
                    tableIdentifier, record.key(), "uncaught:" + e.getClass().getSimpleName());
        }
    }

    /**
     * Executes the heavy derivation chain: segmentation → L0 summarization → Chunk → entity → View
     * → Cognitive Stage → Embedding → JVector → Keyword → fact extraction.
     *
     * <p>When {@code async=true} the chain runs on an {@link Async Derivation Queue} virtual thread
     * (the Raw→LMDB write has already completed synchronously); the Cognitive Stage is executed
     * inline to avoid an extra hop. When {@code async=false} the behavior is identical to the
     * legacy synchronous path (Cognitive Stage dispatched to its own executor).
     */
    private void runDerivationChain(
            SemanticRecord record, String tableIdentifier, String rawText, boolean async) {
        Map<String, Object> deriveParams = buildDeriveParams(tableIdentifier, record);
        String baseText = rawText;
        Object rendered = deriveParams.get("renderedContent");
        if (rendered instanceof byte[] renderedBytes) {
            String renderedText = new String(renderedBytes, StandardCharsets.UTF_8);
            if (!renderedText.isBlank()) {
                baseText = renderedText;
            }
        }

        String summaryText = baseText;
        if (ChineseSegmenter.hasChinese(summaryText)) {
            summaryText = ChineseSegmenter.getInstance().segment(summaryText);
        }
        final String finalSummaryText = summaryText;
        String entryKey = "rec:" + new String(record.key().encoded(), StandardCharsets.UTF_8);

        String embeddingText = summaryText;
        SummarizationService svc = resolveSummarizationService();
        if (L0_SUMMARIZATION_ENABLED && svc != null && svc.isAvailable()) {
            try {
                SummarizationService.SummarizationRequest req =
                        new SummarizationService.SummarizationRequest(
                                "default", 100, 2000, "text/plain", "");
                SummarizationService.SummarizationResult result;
                try {
                    result =
                            java.util.concurrent.CompletableFuture.supplyAsync(
                                            () -> {
                                                try {
                                                    return svc.summarize(finalSummaryText, req);
                                                } catch (java.io.IOException e) {
                                                    throw new RuntimeException(e);
                                                }
                                            })
                                    .get(
                                            L0_SUMMARIZATION_TIMEOUT_MS,
                                            java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (java.util.concurrent.TimeoutException timeout) {
                    log.warn(
                            "L0 summarization timed out after {}ms, falling back to original text table={} recordKey={}",
                            L0_SUMMARIZATION_TIMEOUT_MS,
                            tableIdentifier,
                            record.key());
                    result = null;
                }
                if (result != null
                        && result.l0Abstract() != null
                        && !result.l0Abstract().isBlank()) {
                    embeddingText = summaryText + "\n\n[Summary] " + result.l0Abstract();
                    log.info(
                            "L0 summarization: augmented embedding with L0 abstract table={} recordKey={} origLen={} l0Len={} combinedLen={}",
                            tableIdentifier,
                            record.key(),
                            summaryText.length(),
                            result.l0Abstract().length(),
                            embeddingText.length());
                } else {
                    log.info(
                            "L0 summarization: L0 abstract empty, falling back to original text table={} recordKey={}",
                            tableIdentifier,
                            record.key());
                }
            } catch (Exception sumEx) {
                log.warn(
                        "L0 summarization FAILED, falling back to original text table={} recordKey={}: {}",
                        tableIdentifier,
                        record.key(),
                        sumEx.getMessage(),
                        sumEx);
                embeddingText = summaryText;
            }
        } else {
            log.debug(
                    "L0 summarization disabled or service unavailable, using original text for embedding");
        }

        String configuredStrategy = resolveChunkingStrategy(tableIdentifier);
        if (configuredStrategy != null && !configuredStrategy.isBlank()) {
            derivationEngine.bindTableStrategy(tableIdentifier, configuredStrategy);
        }
        ChunkingStrategy selectedStrategy =
                (record instanceof Record r)
                        ? derivationEngine.selectStrategy(
                                r, tableIdentifier, Chunk.ChunkType.TEXT_SEGMENT)
                        : null;
        String strategyName =
                selectedStrategy != null ? selectedStrategy.name() : configuredStrategy;
        Chunk.ChunkType chunkType =
                derivationEngine.resolveChunkType(strategyName, Chunk.ChunkType.TEXT_SEGMENT);
        List<Chunk> chunks =
                derivationEngine.deriveChunks(record.key(), chunkType, strategyName, deriveParams);

        // late-binding: get entity Stage from store,
        // avoid Auto Derivation Hook being registered before Entity Derivation Stage injection
        // causing entity Stage to always be null timing bug.
        io.github.rawwise.axiom.pipeline.entity.EntityDerivationStage effectiveEntityStage =
                this.entityStage != null
                        ? this.entityStage
                        : (store != null ? store.getEntityDerivationStage() : null);
        if (effectiveEntityStage != null) {
            try {
                long snapshotId = resolveSnapshotId(tableIdentifier);
                List<String> entityChunkIds =
                        chunks.stream().map(Chunk::chunkId).collect(Collectors.toList());
                effectiveEntityStage.onWrite(record, tableIdentifier, snapshotId, entityChunkIds);
            } catch (java.util.concurrent.RejectedExecutionException reject) {
                log.warn(
                        "Entity derivation REJECTED (queue full) for table={} recordKey={}: {}",
                        tableIdentifier,
                        record.key(),
                        reject.getMessage());
            } catch (RuntimeException e) {
                log.error(
                        "Entity derivation submission FAILED for table={} recordKey={}: {}",
                        tableIdentifier,
                        record.key(),
                        e.getMessage(),
                        e);
            }
        }

        if (!chunks.isEmpty()) {
            List<String> chunkIds =
                    chunks.stream().map(Chunk::chunkId).collect(Collectors.toList());
            View view =
                    derivationEngine.deriveView(
                            chunkIds, View.ViewType.SUMMARY, "default", Map.of());
            log.info(
                    "Auto-derivation: chunks+view OK table={} recordKey={} chunks={} viewId={} async={}",
                    tableIdentifier,
                    record.key(),
                    chunks.size(),
                    view.viewId(),
                    async);
            if (feedbackLoopCloser != null) {
                feedbackLoopCloser.onWrite(view.viewId());
            }
            // fix wiring: entry Key → view Id mapping,allows feedback mechanism to reverse-lookup
            // real view Id via entry Key
            // entry Key = "rec:<record Key>" returned by search;real view Id = "view:<uuid>"
            // generated by derive View
            String entryKeyForMap =
                    "rec:" + new String(record.key().encoded(), StandardCharsets.UTF_8);
            writeEntryKeyToViewIdMapping(entryKeyForMap, view.viewId());

            // persist ViewMetadata sidecar (lifecycle metadata only: decay/access). Summaries live
            // in the View layer (deriveView SUMMARY), not in the sidecar.
            if (viewMetadataStore != null) {
                try {
                    ViewMetadata vm =
                            new ViewMetadata(
                                    entryKeyForMap,
                                    tableIdentifier,
                                    1.0f,
                                    System.currentTimeMillis(),
                                    1);
                    viewMetadataStore.update(tableIdentifier, entryKeyForMap, vm);
                } catch (Exception e) {
                    log.warn(
                            "Failed to write ViewMetadata for table={} recordKey={}: {}",
                            tableIdentifier,
                            record.key(),
                            e.getMessage(),
                            e);
                }
            }

            if (COGNITION_STAGE_ENABLED) {
                if (async) {
                    // already on an async derivation thread - run Cognitive Stage inline to avoid
                    // an extra executor hop and keep it within the same watermark boundary
                    triggerCognitiveStage(tableIdentifier, view.viewId());
                } else if (COGNITION_EXECUTOR != null) {
                    final String fViewId = view.viewId();
                    final String fTableId = tableIdentifier;
                    COGNITION_EXECUTOR.submit(() -> triggerCognitiveStage(fTableId, fViewId));
                }
            }
        } else {
            log.info(
                    "Auto-derivation: chunks empty (already derived or no segments) table={} recordKey={} async={}",
                    tableIdentifier,
                    record.key(),
                    async);
        }

        float[] embedding;
        try {
            embedding = embeddingProvider.embed(embeddingText);
        } catch (Exception embedEx) {
            log.error(
                    "Auto-derivation FAILED at embed step for table={} recordKey={} textLen={} async={}: {}",
                    tableIdentifier,
                    record.key(),
                    embeddingText.length(),
                    async,
                    embedEx.getMessage(),
                    embedEx);
            markDerivIncomplete(
                    tableIdentifier, record.key(), "embed:" + embedEx.getClass().getSimpleName());
            return;
        }
        try {
            vectorIndex.indexWithEmbedding(tableIdentifier, entryKey, embedding, embeddingText);
        } catch (Exception indexEx) {
            log.error(
                    "Auto-derivation FAILED at vectorIndex step for table={} recordKey={} entryKey={} async={}: {}",
                    tableIdentifier,
                    record.key(),
                    entryKey,
                    async,
                    indexEx.getMessage(),
                    indexEx);
            markDerivIncomplete(
                    tableIdentifier,
                    record.key(),
                    "vectorIndex:" + indexEx.getClass().getSimpleName());
            return;
        }

        if (keywordIndex != null) {
            try {
                keywordIndex.index(tableIdentifier, entryKey, summaryText, summaryText);
            } catch (Exception kwEx) {
                log.error(
                        "Auto-derivation partial failure at keywordIndex for table={} recordKey={}: {}",
                        tableIdentifier,
                        record.key(),
                        kwEx.getMessage(),
                        kwEx);
            }
        }

        log.info(
                "Auto-derivation OK end-to-end table={} recordKey={} embedDim={} async={}",
                tableIdentifier,
                record.key(),
                embedding.length,
                async);
        clearDerivIncomplete(tableIdentifier, record.key());

        if (PER_CHUNK_EMBEDDING_ENABLED && !chunks.isEmpty()) {
            for (Chunk chunk : chunks) {
                try {
                    String chunkText = new String(chunk.content(), StandardCharsets.UTF_8);
                    float[] chunkEmbed = embeddingProvider.embed(chunkText);
                    String chunkEntryKey = "chunk:" + chunk.chunkId();
                    vectorIndex.indexWithEmbedding(
                            tableIdentifier, chunkEntryKey, chunkEmbed, chunkText);
                    if (keywordIndex != null) {
                        try {
                            keywordIndex.index(
                                    tableIdentifier, chunkEntryKey, chunkText, chunkText);
                        } catch (Exception kwEx) {
                            log.debug(
                                    "Per-chunk keyword index failed chunk={}: {}",
                                    chunk.chunkId(),
                                    kwEx.getMessage());
                        }
                    }
                } catch (Exception e) {
                    log.warn(
                            "Per-chunk embedding failed chunk={} table={}: {}",
                            chunk.chunkId(),
                            tableIdentifier,
                            e.getMessage());
                }
            }
            log.info(
                    "Per-chunk embedding OK table={} recordKey={} chunks={}",
                    tableIdentifier,
                    record.key(),
                    chunks.size());
        }

        // Strategy D: async extract fact-level entries and index independently
        if (FACT_EXTRACTION_ENABLED && FACT_EXECUTOR != null) {
            final String factText = summaryText;
            final String factTableId = tableIdentifier;
            final String factEntryKeyBase = entryKey;
            FACT_EXECUTOR.submit(
                    () -> {
                        try {
                            extractAndIndexFacts(factTableId, factEntryKeyBase, factText);
                        } catch (Exception ex) {
                            log.warn(
                                    "Fact extraction failed for table={} entryKey={}: {}",
                                    factTableId,
                                    factEntryKeyBase,
                                    ex.getMessage());
                        }
                    });
        }
    }

    private void extractAndIndexFacts(
            String tableIdentifier, String entryKeyBase, String sourceText) {
        if (sourceText == null || sourceText.isBlank()) {
            return;
        }
        // Strategy H: short text skips fact extraction(keeping original text embedding is
        // sufficient)
        if (FACT_MIN_SOURCE_LEN > 0 && sourceText.length() < FACT_MIN_SOURCE_LEN) {
            log.debug(
                    "Fact extraction skipped (text too short): len={} < min={} table={} entryKey={}",
                    sourceText.length(),
                    FACT_MIN_SOURCE_LEN,
                    tableIdentifier,
                    entryKeyBase);
            return;
        }
        SummarizationService svc = resolveSummarizationService();
        if (svc == null || !svc.isAvailable()) {
            log.debug(
                    "Fact extraction skipped: SummarizationService unavailable table={} entryKey={}",
                    tableIdentifier,
                    entryKeyBase);
            return;
        }

        String factPrompt =
                "Extract up to "
                        + FACT_MAX_PER_RECORD
                        + " independent, atomic factual statements from the following text. "
                        + "Return ONLY a JSON array of strings, no explanation. Each fact must be self-contained and >= "
                        + FACT_MIN_LEN
                        + " chars.";

        SummarizationService.SummarizationResult result;
        int maxRetries = 3;
        long backoffMs = 2000;
        result = null;
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                // reuse Summarization Service.summarize;prompt placed in system Prompt field,source
                // Text as content
                SummarizationService.SummarizationRequest req =
                        new SummarizationService.SummarizationRequest(
                                "default", 50, 1000, "application/json", factPrompt);
                result =
                        java.util.concurrent.CompletableFuture.supplyAsync(
                                        () -> {
                                            try {
                                                return svc.summarize(sourceText, req);
                                            } catch (java.io.IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        })
                                .get(
                                        FACT_EXTRACTION_TIMEOUT_MS,
                                        java.util.concurrent.TimeUnit.MILLISECONDS);
                break;
            } catch (java.util.concurrent.TimeoutException timeout) {
                log.warn(
                        "Fact extraction timed out after {}ms (attempt {}/{}) table={} entryKey={}",
                        FACT_EXTRACTION_TIMEOUT_MS,
                        attempt + 1,
                        maxRetries,
                        tableIdentifier,
                        entryKeyBase);
                if (attempt == maxRetries - 1) return;
            } catch (Exception e) {
                String msg = e.getMessage() == null ? "" : e.getMessage();
                boolean rateLimited = msg.contains("429") || msg.contains("TooManyRequests");
                if (rateLimited && attempt < maxRetries - 1) {
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    backoffMs *= 2;
                    log.debug(
                            "Fact extraction rate limited, retry {} after {}ms",
                            attempt + 1,
                            backoffMs);
                    continue;
                }
                log.warn(
                        "Fact extraction LLM call failed table={} entryKey={}: {}",
                        tableIdentifier,
                        entryKeyBase,
                        msg);
                return;
            }
        }

        if (result == null || result.l0Abstract() == null || result.l0Abstract().isBlank()) {
            log.debug(
                    "Fact extraction: empty result table={} entryKey={}",
                    tableIdentifier,
                    entryKeyBase);
            return;
        }

        List<String> facts;
        try {
            String raw = result.l0Abstract().trim();
            int start = raw.indexOf('[');
            int end = raw.lastIndexOf(']');
            if (start < 0 || end <= start) {
                log.debug(
                        "Fact extraction: no JSON array found in LLM output table={} entryKey={}",
                        tableIdentifier,
                        entryKeyBase);
                return;
            }
            String jsonArr = raw.substring(start, end + 1);
            facts =
                    MAPPER.readValue(
                            jsonArr,
                            MAPPER.getTypeFactory()
                                    .constructCollectionType(List.class, String.class));
        } catch (Exception e) {
            log.warn(
                    "Fact extraction JSON parse failed table={} entryKey={}: {}",
                    tableIdentifier,
                    entryKeyBase,
                    e.getMessage());
            return;
        }

        int indexed = 0;
        for (int i = 0; i < facts.size() && indexed < FACT_MAX_PER_RECORD; i++) {
            String fact = facts.get(i);
            if (fact == null || fact.trim().length() < FACT_MIN_LEN) {
                continue;
            }
            String factEntryKey = entryKeyBase + "#fact:" + indexed;
            try {
                float[] factEmbed = embeddingProvider.embed(fact);
                vectorIndex.indexWithEmbedding(tableIdentifier, factEntryKey, factEmbed, fact);
                if (keywordIndex != null) {
                    try {
                        keywordIndex.index(tableIdentifier, factEntryKey, fact, fact);
                    } catch (Exception kwEx) {
                        log.debug(
                                "Fact keyword index failed entryKey={}: {}",
                                factEntryKey,
                                kwEx.getMessage());
                    }
                }
                indexed++;
            } catch (Exception e) {
                log.warn(
                        "Fact embed/index failed entryKey={}: {}", factEntryKey, e.getMessage(), e);
            }
        }
        log.info(
                "Fact extraction OK table={} entryKey={} extracted={} indexed={}",
                tableIdentifier,
                entryKeyBase,
                facts.size(),
                indexed);
    }

    // ===== Session-Level Semantic Derivation implementation =====

    /**
     * extract generic session identifier from record properties. priority:session_id (properties) >
     * sample_id:session_num (properties) > content text [dia:xxx:D<n>:m] parsing
     */
    @SuppressWarnings("unchecked")
    private String extractSessionId(SemanticRecord record, String contentText) {
        // Path 1: extract from record body properties
        if (record instanceof Record r) {
            try {
                Map<String, Object> map = r.body().toMap();
                Object propsObj = map.get("properties");
                if (propsObj instanceof Map) {
                    Map<String, Object> props = (Map<String, Object>) propsObj;
                    String sessionId = asString(props.get("session_id"));
                    if (sessionId != null) return sessionId;
                    String sampleId = asString(props.get("sample_id"));
                    String sessionNum = asString(props.get("session_num"));
                    if (sampleId != null && sessionNum != null) {
                        return sampleId + ":s" + sessionNum;
                    }
                }
            } catch (Exception e) {
                log.debug("extractSessionId from properties failed: {}", e.getMessage());
            }
        } else {
            log.info(
                    "extractSessionId: record is NOT instanceof Record (class={})",
                    record.getClass().getName());
        }
        // path 2: content text [dia:<sample_id>:s<n>:<m>] parsing
        if (contentText != null) {
            try {
                java.util.regex.Matcher m = DIA_SESSION_PATTERN.matcher(contentText);
                if (m.find()) {
                    String prefix = m.group(1); // sample_id or null
                    String sessionNum = m.group(2);
                    return prefix != null ? prefix + ":s" + sessionNum : "D" + sessionNum;
                }
                log.info(
                        "extractSessionId: DIA_SESSION_PATTERN no match in contentText (first 100 chars='{}')",
                        contentText.length() > 100 ? contentText.substring(0, 100) : contentText);
            } catch (Exception e) {
                log.debug("extractSessionId from content text failed: {}", e.getMessage());
            }
        }
        return null;
    }

    // match [dia:D<session_num>:<turn_num>] [dia:<sample_id>:s<session_num>:<turn_num>]
    private static final java.util.regex.Pattern DIA_SESSION_PATTERN =
            java.util.regex.Pattern.compile("\\[dia:(?:([^:\\]]+):)?[a-zA-Z]+(\\d+):\\d+\\]");

    private static String asString(Object o) {
        if (o == null) return null;
        String s = o.toString().trim();
        return s.isEmpty() ? null : s;
    }

    /**
     * append entry to session buffer(stored in metastore,key = session_entries:<session Id>).
     * buffer is JSON array of {entry Key, text}. use synchronized to prevent data loss from
     * concurrent writes in same session.
     */
    private void appendToSessionBuffer(
            String tableId, String sessionId, String entryKey, String text) {
        log.info(
                "appendToSessionBuffer START: sessionId={} entryKey={} store={}",
                sessionId,
                entryKey,
                store != null ? "non-null" : "null");
        if (store == null) return;
        try {
            io.github.rawwise.metastore.Metastore ms = store.getMetastore();
            if (ms == null) return;
            byte[] key = ("session_entries:" + sessionId).getBytes(StandardCharsets.UTF_8);
            synchronized (sessionId.intern()) {
                List<Map<String, String>> entries;
                byte[] existing = ms.get(key).orElse(null);
                if (existing != null && existing.length > 0) {
                    entries =
                            MAPPER.readValue(
                                    existing,
                                    MAPPER.getTypeFactory()
                                            .constructCollectionType(List.class, Map.class));
                } else {
                    entries = new java.util.ArrayList<>();
                }
                Map<String, String> entry = new java.util.LinkedHashMap<>();
                entry.put("entryKey", entryKey);
                entry.put("text", text.length() > 500 ? text.substring(0, 500) : text);
                entries.add(entry);
                ms.put(key, MAPPER.writeValueAsBytes(entries));
            }
        } catch (Exception e) {
            log.warn("appendToSessionBuffer failed sessionId={}: {}", sessionId, e.getMessage(), e);
        }
    }

    /**
     * Debounce scheduling:same session record write interval < DELAY_MS cancel previous task and
     * re-delay.
     */
    private void scheduleSessionSemanticDerivation(String tableId, String sessionId) {
        log.info(
                "scheduleSessionSemanticDerivation: sessionId={} delayMs={}",
                sessionId,
                SEMANTIC_SESSION_DELAY_MS);
        java.util.concurrent.ScheduledFuture<?> existing = pendingSemanticTasks.get(sessionId);
        if (existing != null) {
            existing.cancel(false);
        }
        java.util.concurrent.ScheduledFuture<?> future =
                SEMANTIC_SCHEDULER.schedule(
                        () -> deriveSessionSemantic(tableId, sessionId),
                        SEMANTIC_SESSION_DELAY_MS,
                        java.util.concurrent.TimeUnit.MILLISECONDS);
        pendingSemanticTasks.put(sessionId, future);
    }

    /**
     * core: generate LLM summary for all records in same session,as independent SEMANTIC entry
     * index. idempotent: if SEMANTIC entry already exists(metastore key semantic:session:<session
     * Id>:done exists),skip.
     */
    private void deriveSessionSemantic(String tableId, String sessionId) {
        log.info("deriveSessionSemantic START: sessionId={} tableId={}", sessionId, tableId);
        pendingSemanticTasks.remove(sessionId);
        try {
            io.github.rawwise.metastore.Metastore ms = store.getMetastore();
            if (ms == null) {
                log.warn(
                        "deriveSessionSemantic: metastore is null, skipping sessionId={}",
                        sessionId);
                return;
            }

            // idempotency check(force-regen skip)
            byte[] doneKey =
                    ("semantic:session:" + sessionId + ":done").getBytes(StandardCharsets.UTF_8);
            if (!SEMANTIC_SESSION_FORCE_REGEN && ms.get(doneKey).isPresent()) {
                log.info("Session semantic already derived: {}", sessionId);
                return;
            }

            // read session buffer
            byte[] bufferKey = ("session_entries:" + sessionId).getBytes(StandardCharsets.UTF_8);
            byte[] bufferData = ms.get(bufferKey).orElse(null);
            if (bufferData == null || bufferData.length == 0) {
                log.info("deriveSessionSemantic: no buffer for sessionId={}", sessionId);
                return;
            }
            List<Map<String, String>> entries =
                    MAPPER.readValue(
                            bufferData,
                            MAPPER.getTypeFactory().constructCollectionType(List.class, Map.class));
            log.info("deriveSessionSemantic: sessionId={} entries={}", sessionId, entries.size());
            if (entries.size() < SEMANTIC_SESSION_MIN_RECORDS) {
                log.info(
                        "Session {} has only {} entries, need >= {}",
                        sessionId,
                        entries.size(),
                        SEMANTIC_SESSION_MIN_RECORDS);
                return;
            }

            // concatenate all text as LLM input
            StringBuilder sb = new StringBuilder();
            List<String> sourceEntryKeys = new java.util.ArrayList<>();
            for (Map<String, String> e : entries) {
                String text = e.get("text");
                if (text != null && !text.isBlank()) {
                    sb.append(text).append("\n");
                }
                String ek = e.get("entryKey");
                if (ek != null) sourceEntryKeys.add(ek);
            }
            String combinedText = sb.toString().trim();
            if (combinedText.isBlank()) return;

            // LLM generates session summary
            SummarizationService svc = resolveSummarizationService();
            if (svc == null || !svc.isAvailable()) {
                log.debug(
                        "Session semantic skipped: SummarizationService unavailable sessionId={}",
                        sessionId);
                return;
            }
            String summaryPrompt =
                    "Summarize the key facts, events, and topics discussed in this conversation session. "
                            + "Preserve specific names, dates, and details. Output a concise paragraph (max 200 words).";
            SummarizationService.SummarizationRequest req =
                    new SummarizationService.SummarizationRequest(
                            "default", 100, 2000, "text/plain", summaryPrompt);
            SummarizationService.SummarizationResult result;
            try {
                result =
                        java.util.concurrent.CompletableFuture.supplyAsync(
                                        () -> {
                                            try {
                                                return svc.summarize(combinedText, req);
                                            } catch (java.io.IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        })
                                .get(
                                        SEMANTIC_SESSION_TIMEOUT_MS,
                                        java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                log.warn(
                        "Session semantic LLM failed sessionId={}: {}",
                        sessionId,
                        e.getMessage(),
                        e);
                return;
            }
            if (result == null || result.l0Abstract() == null || result.l0Abstract().isBlank()) {
                log.debug("Session semantic empty result sessionId={}", sessionId);
                return;
            }

            // generate SEMANTIC entry
            String semanticEntryKey = "semantic:session:" + sessionId;
            String semanticText = "[session:" + sessionId + "] " + result.l0Abstract();
            // embedding uses only summary text,not polluted by source_entries
            float[] embedding;
            try {
                embedding = embeddingProvider.embed(semanticText);
            } catch (Exception e) {
                log.warn(
                        "Session semantic embed failed sessionId={}: {}",
                        sessionId,
                        e.getMessage(),
                        e);
                return;
            }
            // indexed text carries source_entries marker,search parsing boost session EPISODIC
            // entries
            String indexedText =
                    semanticText + "\n[source_entries:" + String.join(",", sourceEntryKeys) + "]";
            vectorIndex.indexWithEmbedding(tableId, semanticEntryKey, embedding, indexedText);
            if (keywordIndex != null) {
                try {
                    keywordIndex.index(tableId, semanticEntryKey, semanticText, semanticText);
                } catch (Exception kwEx) {
                    log.debug("Session semantic keyword index failed: {}", kwEx.getMessage());
                }
            }

            // store source_entry_keys mapping(used for boost during search)
            byte[] sourcesKey =
                    ("semantic_sources:" + semanticEntryKey).getBytes(StandardCharsets.UTF_8);
            ms.put(sourcesKey, MAPPER.writeValueAsBytes(sourceEntryKeys));

            // mark done
            ms.put(
                    doneKey,
                    String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));

            log.info(
                    "Session semantic derived OK sessionId={} entries={} summaryLen={} entryKey={}",
                    sessionId,
                    entries.size(),
                    result.l0Abstract().length(),
                    semanticEntryKey);
        } catch (Exception e) {
            log.error(
                    "Session semantic derivation FAILED sessionId={}: {}",
                    sessionId,
                    e.getMessage(),
                    e);
        }
    }

    /**
     * fix wiring: entry Key → view Id mapping metastore. Key: "entrykey2view:<entry Key>" Value:
     * view Id string annotate API via search return entry Key real view Id trigger cascade
     * Rederive.
     */
    public static final String ENTRY_KEY_TO_VIEW_PREFIX = "entrykey2view:";

    private void writeEntryKeyToViewIdMapping(String entryKey, String viewId) {
        if (store == null || entryKey == null || viewId == null) return;
        try {
            io.github.rawwise.metastore.Metastore ms = store.getMetastore();
            if (ms == null) return;
            byte[] key = (ENTRY_KEY_TO_VIEW_PREFIX + entryKey).getBytes(StandardCharsets.UTF_8);
            byte[] value = viewId.getBytes(StandardCharsets.UTF_8);
            ms.put(key, value);
        } catch (Exception e) {
            log.warn(
                    "Failed to write entryKey→viewId mapping entryKey={} viewId={}: {}",
                    entryKey,
                    viewId,
                    e.getMessage());
        }
    }

    private void triggerCognitiveStage(String tableIdentifier, String viewId) {
        io.github.rawwise.axiom.pipeline.cognition.CognitionEngine engine = this.cognitionEngine;
        if (engine == null) {
            log.warn(
                    "CognitiveStage skipped: CognitionEngine not wired table={} viewId={}",
                    tableIdentifier,
                    viewId);
            return;
        }
        io.github.rawwise.engine.storage.KnowledgeStore ks = this.cognitiveKnowledgeStore;
        if (ks == null) {
            log.warn(
                    "CognitiveStage skipped: KnowledgeStore not wired table={} viewId={}",
                    tableIdentifier,
                    viewId);
            return;
        }
        try {
            Map<String, Object> params = Map.of("tableId", tableIdentifier, "sourceViewId", viewId);
            io.github.rawwise.model.record.Semantic semantic =
                    engine.deriveSemantic(
                            List.of(viewId),
                            io.github.rawwise.model.record.Semantic.SemanticType.CONCEPT,
                            params);
            io.github.rawwise.axiom.pipeline.cognition.ActionPlan plan =
                    engine.deriveActionPlan(
                            List.of(semantic.semanticId()),
                            io.github.rawwise.axiom.pipeline.cognition.CognitionEngine.PlanType
                                    .CHAIN,
                            params);
            persistActionPlan(ks, plan);
            log.info(
                    "CognitiveStage OK table={} viewId={} semanticId={} planId={} actions={}",
                    tableIdentifier,
                    viewId,
                    semantic.semanticId(),
                    plan.planId(),
                    plan.actions().size());
        } catch (Exception e) {
            log.warn(
                    "CognitiveStage FAILED table={} viewId={}: {}",
                    tableIdentifier,
                    viewId,
                    e.getMessage(),
                    e);
        }
    }

    private void persistActionPlan(
            io.github.rawwise.engine.storage.KnowledgeStore ks,
            io.github.rawwise.axiom.pipeline.cognition.ActionPlan plan) {
        try {
            byte[] key = ("action:plan:" + plan.planId()).getBytes(StandardCharsets.UTF_8);
            byte[] value = MAPPER.writeValueAsBytes(plan);
            ks.put(key, value);
        } catch (Exception e) {
            log.warn(
                    "CognitiveStage: failed to persist ActionPlan planId={}: {}",
                    plan.planId(),
                    e.getMessage());
        }
    }

    private long resolveSnapshotId(String tableIdentifier) {
        try {
            io.github.rawwise.engine.storage.manifest.SnapshotManager sm =
                    store.getSnapshotManager();
            if (sm == null) {
                return 0L;
            }
            return sm.latestSnapshot(tableIdentifier)
                    .map(io.github.rawwise.model.record.Snapshot::snapshotId)
                    .orElse(0L);
        } catch (Exception e) {
            log.warn(
                    "resolveSnapshotId FAILED for table={}: {}",
                    tableIdentifier,
                    e.getMessage(),
                    e);
            return 0L;
        }
    }

    private boolean shouldTriggerDerivation(String tableIdentifier) {
        try {
            TableStore ts = store.tableStore(tableIdentifier);
            if (!ts.isAiNative()) {
                log.info(
                        "Auto-derivation skipped: table={} is NOT ai-native (call setTableAiNative(true) to enable)",
                        tableIdentifier);
                return false;
            }
            TableStore.DerivationStrategy strategy = ts.getDerivationStrategy();
            if (strategy == TableStore.DerivationStrategy.NONE) {
                log.info("Auto-derivation skipped: table={} strategy=NONE", tableIdentifier);
                return false;
            }
            if (strategy == TableStore.DerivationStrategy.MANUAL) {
                log.info("Auto-derivation skipped: table={} strategy=MANUAL", tableIdentifier);
                return false;
            }
            return true;
        } catch (Exception e) {
            log.error(
                    "Auto-derivation gate check FAILED for table={}: {}",
                    tableIdentifier,
                    e.getMessage(),
                    e);
            return false;
        }
    }

    private String resolveChunkingStrategy(String tableIdentifier) {
        try {
            TableStore ts = store.tableStore(tableIdentifier);
            return ts.getChunkingStrategy();
        } catch (Exception e) {
            log.debug(
                    "Failed to resolve chunking strategy for table={}: {}",
                    tableIdentifier,
                    e.getMessage(),
                    e);
            return null;
        }
    }

    private Map<String, Object> buildDeriveParams(String tableIdentifier, SemanticRecord record) {
        Map<String, Object> params = new HashMap<>();
        params.put("tableIdentifier", tableIdentifier);
        try {
            TableStore ts = store.tableStore(tableIdentifier);
            String pattern = ts.getSemanticPattern();

            Map<String, Object> fields = null;
            if (record instanceof Record r) {
                try {
                    Map<String, Object> bodyMap = r.body().toMap();
                    Object bodyContent = bodyMap.get("body");
                    byte[] bodyBytes = null;
                    if (bodyContent instanceof byte[] bytes) {
                        bodyBytes = bytes;
                    } else if (bodyContent instanceof String s) {
                        bodyBytes = s.getBytes(StandardCharsets.UTF_8);
                    }
                    if (bodyBytes != null) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> parsed =
                                MAPPER.readValue(
                                        bodyBytes,
                                        MAPPER.getTypeFactory()
                                                .constructMapType(
                                                        Map.class, String.class, Object.class));
                        fields = parsed;
                    }
                } catch (Exception e) {
                    log.debug(
                            "Failed to extract fields for pattern substitution table={}: {}",
                            tableIdentifier,
                            e.getMessage(),
                            e);
                }
            }

            if (pattern == null || pattern.isBlank()) {
                if (fields != null && !fields.isEmpty()) {
                    pattern =
                            patternRenderer.inferDefaultPattern(
                                    tableIdentifier, new java.util.ArrayList<>(fields.keySet()));
                }
            }

            if (pattern == null || pattern.isBlank()) {
                return params;
            }

            params.put("semantic.pattern", pattern);
            if (fields != null && !fields.isEmpty()) {
                params.put("record.fields", fields);
                String rendered = patternRenderer.render(pattern, fields);
                if (rendered != null && !rendered.isBlank()) {
                    params.put("renderedContent", rendered.getBytes(StandardCharsets.UTF_8));
                }
            }
        } catch (Exception e) {
            log.warn(
                    "Failed to build derive params for table={}: {}",
                    tableIdentifier,
                    e.getMessage(),
                    e);
        }
        return params;
    }

    private String applySemanticPattern(String pattern, Map<String, Object> fields) {
        String result = pattern;
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String placeholder = "${" + entry.getKey() + "}";
            String value = entry.getValue() != null ? String.valueOf(entry.getValue()) : "";
            result = result.replace(placeholder, value);
        }
        return result;
    }

    private String extractDerivableText(SemanticRecord record) {
        if (!(record instanceof Record r)) {
            return null;
        }
        try {
            Map<String, Object> map = r.body().toMap();
            Object body = map.get("body");
            if (body instanceof byte[] bytes) {
                String decoded = decodeUtf8IfPossible(bytes);
                if (decoded == null) {
                    log.info(
                            "extractDerivableText: byte[] body is not valid UTF-8, skipping ({} bytes)",
                            bytes.length);
                    return null;
                }
                return isDerivableText(decoded) ? decoded : null;
            }
            if (body instanceof String s) {
                return isDerivableText(s) ? s : null;
            }
            String json = MAPPER.writeValueAsString(map);
            return isDerivableText(json) ? json : null;
        } catch (Exception e) {
            log.error("extractDerivableText FAILED for record: {}", e.getMessage(), e);
            return null;
        }
    }

    private String decodeUtf8IfPossible(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        if (isLikelyBinary(bytes)) {
            return null;
        }
        try {
            java.nio.charset.CharsetDecoder decoder =
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                            .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
            return decoder.decode(java.nio.ByteBuffer.wrap(bytes)).toString();
        } catch (java.nio.charset.CharacterCodingException e) {
            return null;
        }
    }

    private boolean isDerivableText(String text) {
        if (text == null || text.isBlank()) {
            return false;
        }
        if (PURE_NUMERIC.matcher(text.trim()).matches()) {
            log.debug("Skipping pure numeric content: {} chars", text.length());
            return false;
        }
        return true;
    }

    private boolean isLikelyBinary(byte[] bytes) {
        if (bytes.length == 0) {
            return false;
        }
        int checkLen = Math.min(bytes.length, 512);
        int controlCount = 0;
        for (int i = 0; i < checkLen; i++) {
            int b = bytes[i] & 0xFF;
            if (b < 0x20 && b != 0x09 && b != 0x0A && b != 0x0D) {
                controlCount++;
            }
        }
        return (double) controlCount / checkLen > BINARY_SUSPECT_RATIO;
    }

    private void markDerivIncomplete(String tableId, Record.Key key, String reason) {
        if (store == null) return;
        try {
            io.github.rawwise.metastore.Metastore ms = store.getMetastore();
            if (ms == null) return;
            String rawId = "raw:" + java.util.Base64.getEncoder().encodeToString(key.encoded());
            byte[] markerKey =
                    (DERIV_INCOMPLETE_PREFIX + tableId + ":" + rawId)
                            .getBytes(StandardCharsets.UTF_8);
            String value = System.currentTimeMillis() + ":" + reason;
            ms.put(markerKey, value.getBytes(StandardCharsets.UTF_8));
            log.debug("Marked deriv_incomplete: table={} key={} reason={}", tableId, rawId, reason);
        } catch (Exception e) {
            log.warn(
                    "Failed to mark deriv_incomplete: table={} reason={}: {}",
                    tableId,
                    reason,
                    e.getMessage(),
                    e);
        }
    }

    private void clearDerivIncomplete(String tableId, Record.Key key) {
        if (store == null) return;
        try {
            io.github.rawwise.metastore.Metastore ms = store.getMetastore();
            if (ms == null) return;
            String rawId = "raw:" + java.util.Base64.getEncoder().encodeToString(key.encoded());
            byte[] markerKey =
                    (DERIV_INCOMPLETE_PREFIX + tableId + ":" + rawId)
                            .getBytes(StandardCharsets.UTF_8);
            ms.delete(markerKey);
        } catch (Exception e) {
            log.debug("Failed to clear deriv_incomplete: table={}: {}", tableId, e.getMessage());
        }
    }
}
