package io.github.rawwise.axiom.pipeline.entity;

import io.github.rawwise.axiom.pipeline.graph.GraphIndexer;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.SemanticRecord;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * NOTE (PHC-008 / NI-TXN-001): This component writes to Knowledge Store directly, consistent with
 * the existing pattern in Default Derivation Engine (e.g. view/chunk writes). Full Txn Store Bridge
 * / Txn Orchestrator integration is tracked as project-wide debt and not regressed.
 *
 * <p>NOTE (AI-H3 / Backpressure Manager): Backpressure Manager integration is tracked separately.
 * Queue-full currently surfaces as Rejected Execution Exception → mark Failed(dia Id, sample Id,
 * "QUEUE_FULL").
 */
@Slf4j
public final class EntityDerivationStage implements EntityDerivationStageSpi {

    private static final String ENTITY_VIEW_KEY_PREFIX = "entity_view:";

    private final HybridEntityExtractor extractor;
    private final GraphIndexer indexer;
    private final KnowledgeStore knowledgeStore;
    private final ThreadPoolExecutor asyncDerivationPool;
    private final EntityExtractionConfig config;

    public EntityDerivationStage(
            HybridEntityExtractor extractor,
            GraphIndexer indexer,
            KnowledgeStore knowledgeStore,
            ThreadPoolExecutor asyncDerivationPool,
            EntityExtractionConfig config) {
        this.extractor = extractor;
        this.indexer = indexer;
        this.knowledgeStore = knowledgeStore;
        this.asyncDerivationPool = asyncDerivationPool;
        this.config = config == null ? EntityExtractionConfig.defaults() : config;
    }

    public static ThreadPoolExecutor newDefaultPool(EntityExtractionConfig config) {
        EntityExtractionConfig cfg = config == null ? EntityExtractionConfig.defaults() : config;
        int threads = cfg.asyncThreads() > 0 ? cfg.asyncThreads() : 8;
        int queueSize = cfg.asyncQueueSize() > 0 ? cfg.asyncQueueSize() : 10000;
        ThreadPoolExecutor pool =
                new ThreadPoolExecutor(
                        threads,
                        threads,
                        60L,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(queueSize),
                        r -> {
                            Thread t = new Thread(r, "entity-derivation");
                            t.setDaemon(true);
                            return t;
                        },
                        new ThreadPoolExecutor.AbortPolicy());
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    @Override
    public void onWrite(SemanticRecord record, String tableId, long snapshotId) {
        onWrite(record, tableId, snapshotId, java.util.List.of());
    }

    @Override
    public void onWrite(
            SemanticRecord record,
            String tableId,
            long snapshotId,
            java.util.List<String> chunkIds) {
        if (record == null) return;
        String diaId;
        try {
            diaId = encodeDiaId(record);
        } catch (ExtractionException e) {
            log.warn("Entity derivation skipped: invalid record key: {}", e.getMessage());
            return;
        }
        String sampleId = extractSampleId(record);
        try {
            EntityView placeholder = EntityView.pending(diaId, sampleId);
            persistView(diaId, placeholder);
        } catch (IOException e) {
            log.error(
                    "Failed to write placeholder EntityView diaId={}: {}",
                    diaId,
                    e.getMessage(),
                    e);
            return;
        }

        try {
            asyncDerivationPool.submit(
                    () -> runExtraction(record, tableId, snapshotId, diaId, sampleId, chunkIds));
            log.info(
                    "Entity derivation submitted diaId={} sampleId={} table={} chunkIds={}",
                    diaId,
                    sampleId,
                    tableId,
                    chunkIds != null ? chunkIds.size() : 0);
        } catch (RejectedExecutionException e) {
            log.error("Entity derivation queue full diaId={}, marking FAILED", diaId);
            markFailed(diaId, sampleId, "QUEUE_FULL");
        }
    }

    private void runExtraction(
            SemanticRecord record,
            String tableId,
            long snapshotId,
            String diaId,
            String sampleId,
            java.util.List<String> chunkIds) {
        try {
            String text = extractDerivableText(record);
            if (text == null) {
                markFailed(diaId, sampleId, "NO_DERIVABLE_TEXT");
                return;
            }
            EntityContext context = new EntityContext(sampleId, tableId, snapshotId, Map.of());
            EntityView view = extractWithTimeout(text, context);
            EntityView finalView =
                    new EntityView(
                            diaId,
                            sampleId,
                            view.entities(),
                            view.relations(),
                            view.source(),
                            view.extractedAt(),
                            ExtractionStatus.SUCCESS,
                            null);
            try {
                persistView(diaId, finalView);
            } catch (IOException e) {
                log.error("Failed to persist EntityView diaId={}: {}", diaId, e.getMessage(), e);
                markFailed(diaId, sampleId, "PERSIST_FAILED:" + e.getMessage());
                return;
            }
            if (indexer != null) {
                if (chunkIds != null && !chunkIds.isEmpty()) {
                    indexer.indexEntities(finalView, chunkIds);
                } else {
                    indexer.indexEntities(finalView);
                }
            }
            log.info(
                    "Entity derivation OK diaId={} entities={} relations={} source={} chunkIds={}",
                    diaId,
                    finalView.entities().size(),
                    finalView.relations().size(),
                    finalView.source(),
                    chunkIds != null ? chunkIds.size() : 0);
        } catch (TimeoutException e) {
            log.warn("Entity extraction timeout diaId={}", diaId);
            markFailed(diaId, sampleId, "TIMEOUT");
        } catch (ExtractionException e) {
            log.warn("Entity extraction failed diaId={}: {}", diaId, e.getMessage());
            markFailed(diaId, sampleId, e.getMessage());
        } catch (RuntimeException e) {
            log.error("Entity extraction unexpected failure diaId={}", diaId, e);
            markFailed(diaId, sampleId, "UNEXPECTED:" + e.getMessage());
        }
    }

    private EntityView extractWithTimeout(String text, EntityContext context)
            throws ExtractionException, TimeoutException {
        long timeoutMs = config.timeout() != null ? config.timeout().toMillis() : 0;
        if (timeoutMs <= 0) {
            return extractor.extract(text, context);
        }
        java.util.concurrent.CompletableFuture<EntityView> future =
                java.util.concurrent.CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return extractor.extract(text, context);
                            } catch (ExtractionException e) {
                                throw new java.util.concurrent.CompletionException(e);
                            }
                        },
                        java.util.concurrent.ForkJoinPool.commonPool());
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            future.cancel(true);
            throw te;
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof ExtractionException ex) throw ex;
            if (cause instanceof RuntimeException re) throw re;
            throw new ExtractionException(
                    "extractor failed: " + (cause == null ? ee.getMessage() : cause.getMessage()));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ExtractionException("extractor interrupted");
        }
    }

    private void persistView(String diaId, EntityView view) throws IOException {
        byte[] key = (ENTITY_VIEW_KEY_PREFIX + diaId).getBytes(StandardCharsets.UTF_8);
        knowledgeStore.put(key, view.encode());
    }

    private void markFailed(String diaId, String sampleId, String reason) {
        try {
            persistView(diaId, EntityView.failed(diaId, sampleId, reason));
        } catch (IOException e) {
            log.error(
                    "Failed to write failed-marker EntityView diaId={}: {}",
                    diaId,
                    e.getMessage(),
                    e);
        }
    }

    private String encodeDiaId(SemanticRecord record) throws ExtractionException {
        try {
            byte[] encoded = record.key().encoded();
            return Base64.getUrlEncoder().withoutPadding().encodeToString(encoded);
        } catch (RuntimeException e) {
            throw new ExtractionException(
                    "invalid record key for entity derivation: " + e.getMessage());
        }
    }

    private String extractSampleId(SemanticRecord record) {
        if (!(record instanceof Record r)) return "";
        try {
            Map<String, Object> map = r.body().toMap();
            Object sampleId = map.get("sample_id");
            if (sampleId == null) sampleId = map.get("sampleId");
            if (sampleId == null) sampleId = map.get("session_id");
            if (sampleId == null) sampleId = map.get("agent_id");
            return sampleId == null ? "" : sampleId.toString();
        } catch (RuntimeException e) {
            return "";
        }
    }

    private String extractDerivableText(SemanticRecord record) {
        if (!(record instanceof Record r)) return null;
        try {
            Map<String, Object> map = r.body().toMap();
            Object body = map.get("body");
            if (body instanceof byte[] bytes) {
                String s = new String(bytes, StandardCharsets.UTF_8);
                return s.isBlank() ? null : s;
            }
            if (body instanceof String s) {
                return s.isBlank() ? null : s;
            }
            Object text = map.get("text");
            if (text instanceof String ts) {
                return ts.isBlank() ? null : ts;
            }
            return null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    public void shutdown() {
        asyncDerivationPool.shutdown();
        try {
            if (!asyncDerivationPool.awaitTermination(30, TimeUnit.SECONDS)) {
                asyncDerivationPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            asyncDerivationPool.shutdownNow();
        }
    }

    public EntityExtractionConfig config() {
        return config;
    }
}
