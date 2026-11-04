package io.github.rawwise.axiom.pipeline.feedback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.cognition.CognitiveAssetMetric;
import io.github.rawwise.axiom.pipeline.cognition.MembershipFunctions;
import io.github.rawwise.axiom.pipeline.derivation.Layer;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationCriteria;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.axiom.pipeline.ingestion.IngestionEngine;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.record.FeedbackAnnotation;
import io.github.rawwise.model.record.Semantic;
import io.github.rawwise.model.record.View;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FeedbackLoopCloser {

    private static final int MAX_CASCADE_DEPTH = 3;
    private static final int MAX_LOOP_DEPTH = 3;
    private static final double CASCADE_THRESHOLD =
            Double.parseDouble(System.getProperty("rawwise.feedback.cascade-threshold", "0.05"));
    private static final int CAS_MAX_RETRIES = 8;
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

    private static final boolean CASCADE_LLM_ENABLED =
            Boolean.parseBoolean(System.getProperty("rawwise.axiom.cascade.llm.enabled", "false"));
    private static final String CASCADE_LLM_ENDPOINT =
            System.getProperty("rawwise.axiom.cascade.llm.endpoint", "");
    private static final String CASCADE_LLM_API_KEY =
            System.getProperty("rawwise.axiom.cascade.llm.api-key", "");
    private static final String CASCADE_LLM_MODEL =
            System.getProperty("rawwise.axiom.cascade.llm.model", "gpt-4o-mini");
    private static final java.net.http.HttpClient CASCADE_HTTP_CLIENT =
            java.net.http.HttpClient.newBuilder()
                    .connectTimeout(java.time.Duration.ofSeconds(5))
                    .build();

    private static final int MIN_FEEDBACK_FOR_LEARNING =
            Integer.parseInt(System.getProperty("rawwise.strategy.min-feedback", "2"));

    private final EvaluationEngine evaluationEngine;
    private CognitionEngine cognitionEngine;
    private final Metastore metastore;
    private final KnowledgeStore knowledgeStore;
    private final KnowledgeDataAccessor dataAccessor;
    private final ExecutorService cascadeExecutor;
    private final IngestionEngine ingestionEngine;

    // T function Update Models --strategy learning(optional,injected via setter)
    private volatile io.github.rawwise.axiom.pipeline.cognition.StrategyLearningService
            strategyLearningService;
    private volatile FeedbackCollector feedbackCollector;
    private volatile io.github.rawwise.axiom.pipeline.cognition.CognitionDriver cognitionDriver;
    // strategy learninguses separate thread pool,does not compete with cascade Rederive Fixed
    // Thread Pool(2)
    private final ExecutorService strategyLearningExecutor =
            Executors.newVirtualThreadPerTaskExecutor();
    private final java.util.concurrent.Semaphore strategyLearningSemaphore =
            new java.util.concurrent.Semaphore(4);

    public FeedbackLoopCloser(
            EvaluationEngine evaluationEngine,
            CognitionEngine cognitionEngine,
            Metastore metastore,
            KnowledgeStore knowledgeStore) {
        this(
                evaluationEngine,
                cognitionEngine,
                metastore,
                knowledgeStore,
                Executors.newVirtualThreadPerTaskExecutor());
    }

    public FeedbackLoopCloser(
            EvaluationEngine evaluationEngine,
            CognitionEngine cognitionEngine,
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            ExecutorService cascadeExecutor) {
        this(evaluationEngine, cognitionEngine, metastore, knowledgeStore, cascadeExecutor, null);
    }

    public FeedbackLoopCloser(
            EvaluationEngine evaluationEngine,
            CognitionEngine cognitionEngine,
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            ExecutorService cascadeExecutor,
            IngestionEngine ingestionEngine) {
        this.evaluationEngine = evaluationEngine;
        this.cognitionEngine = cognitionEngine;
        this.metastore = metastore;
        this.knowledgeStore = knowledgeStore;
        this.dataAccessor = new KnowledgeDataAccessor(metastore, knowledgeStore);
        this.cascadeExecutor = cascadeExecutor;
        this.ingestionEngine = ingestionEngine;
    }

    public void setCognitionEngine(CognitionEngine cognitionEngine) {
        this.cognitionEngine = cognitionEngine;
    }

    /**
     * strategy learning(T function's Update Models component). on Annotation async trigger strategy
     * learning.
     */
    public void setStrategyLearningService(
            io.github.rawwise.axiom.pipeline.cognition.StrategyLearningService
                    strategyLearningService) {
        this.strategyLearningService = strategyLearningService;
    }

    /** inject feedback collector(strategy learning). */
    public void setFeedbackCollector(FeedbackCollector feedbackCollector) {
        this.feedbackCollector = feedbackCollector;
    }

    /**
     * inject cognition driver(optional,injected via setter). after injection, on Annotation will
     * async trigger Cognition Driver.next Step() at end of feedback processing.
     */
    public void setCognitionDriver(
            io.github.rawwise.axiom.pipeline.cognition.CognitionDriver cognitionDriver) {
        this.cognitionDriver = cognitionDriver;
    }

    public LoopRecord onAnnotation(String viewId, FeedbackAnnotation annotation) {
        return onAnnotation(viewId, annotation, 0);
    }

    public LoopRecord onAnnotation(String viewId, FeedbackAnnotation annotation, int depth) {
        if (viewId == null || annotation == null) {
            log.warn("onAnnotation called with null viewId or annotation");
            return new LoopRecord(
                    viewId,
                    "null",
                    0.0,
                    0.0,
                    false,
                    LoopStatus.FAILED,
                    0.0,
                    System.currentTimeMillis());
        }

        if (depth >= MAX_LOOP_DEPTH) {
            log.info("Loop depth limit reached for viewId={}, stopping cascade", viewId);
            return new LoopRecord(
                    viewId,
                    annotation.annotationId(),
                    0.0,
                    0.0,
                    false,
                    LoopStatus.OK,
                    0.0,
                    System.currentTimeMillis());
        }

        View view = loadView(viewId);
        if (view == null) {
            log.warn("View not found for viewId={}", viewId);
            return new LoopRecord(
                    viewId,
                    annotation.annotationId(),
                    0.0,
                    0.0,
                    false,
                    LoopStatus.VIEW_NOT_FOUND,
                    0.0,
                    System.currentTimeMillis());
        }

        int viewVersion = view.version();
        double previousQualityScore = view.qualityScore().orElse(0.0);

        try {
            // must first write Versioned Annotation to persist current annotation,
            // then evaluate can read it(including PREFERENCE value counted into annotation Bonus).
            writeVersionedAnnotation(viewId, viewVersion, annotation);

            EvaluationCriteria criteria = evaluationEngine.getDefaultCriteria(view.viewType());
            var result = evaluationEngine.evaluate(view, criteria);
            double newQualityScore = result.qualityScore();

            updateViewQualityScore(viewId, newQualityScore);

            CognitiveAssetMetric metric = cognitionEngine.incrementalAssetUpdate(viewId);
            double assetValueSnapshot = metric.assetValue();

            boolean shouldCascade =
                    shouldCascade(newQualityScore - previousQualityScore, annotation, view);

            LoopRecord record =
                    new LoopRecord(
                            viewId,
                            annotation.annotationId(),
                            previousQualityScore,
                            newQualityScore,
                            shouldCascade,
                            LoopStatus.OK,
                            assetValueSnapshot,
                            System.currentTimeMillis());

            persistLoopRecord(
                    viewId,
                    annotation.annotationId(),
                    previousQualityScore,
                    newQualityScore,
                    shouldCascade,
                    LoopStatus.OK,
                    assetValueSnapshot);

            var signal =
                    new DefaultFeedbackSignal(
                            annotation.annotationId(),
                            annotation.annotatorId().startsWith("agent:")
                                    ? FeedbackSignal.FeedbackSource.ENV
                                    : FeedbackSignal.FeedbackSource.HUMAN,
                            FeedbackSignal.FeedbackPath.EVAL_LAYER,
                            FeedbackSignal.SignalType.QUALITY_SCORE,
                            viewId,
                            Layer.VIEW,
                            annotation.confidence(),
                            Map.of("annotationType", annotation.type().name()),
                            annotation.timestamp());
            emitFeedback(signal);

            // write Versioned Annotation already called before evaluate(ensure evaluate can read
            // current annotation)

            if (shouldCascade) {
                cascadeExecutor.submit(
                        () -> {
                            try {
                                cascadeRederive(viewId, newQualityScore, depth + 1);
                            } catch (Exception e) {
                                log.warn(
                                        "Cascade rederive failed for viewId={}: {}",
                                        viewId,
                                        e.getMessage());
                            }
                        });
            }

            // T function Update Models:asyncstrategy learning(separate thread pool,does not compete
            // with cascade Rederive)
            if (strategyLearningService != null && feedbackCollector != null) {
                final double fnewScore = newQualityScore;
                strategyLearningExecutor.submit(
                        () -> {
                            try {
                                if (!strategyLearningSemaphore.tryAcquire(
                                        30, java.util.concurrent.TimeUnit.SECONDS)) {
                                    log.warn(
                                            "Strategy learning semaphore timeout for viewId={}",
                                            viewId);
                                    return;
                                }
                                try {
                                    triggerStrategyLearning(
                                            viewId, String.valueOf(viewVersion), fnewScore);
                                } finally {
                                    strategyLearningSemaphore.release();
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } catch (Exception e) {
                                log.warn(
                                        "Strategy learning failed for viewId={}: {}",
                                        viewId,
                                        e.getMessage());
                            }
                        });
            }

            // Cognition Driver:produce cognitive decisions when feedback arrives(async, does not
            // block on Annotation)
            if (cognitionDriver != null && cognitionDriver.isAvailable()) {
                final double fnewScore = newQualityScore;
                strategyLearningExecutor.submit(
                        () -> {
                            try {
                                triggerCognitionDriver(viewId, fnewScore);
                            } catch (Exception e) {
                                log.warn(
                                        "CognitionDriver failed for viewId={}: {}",
                                        viewId,
                                        e.getMessage());
                            }
                        });
            }

            return record;
        } catch (Exception e) {
            log.warn("onAnnotation failed for viewId={}: {}", viewId, e.getMessage());
            return new LoopRecord(
                    viewId,
                    annotation.annotationId(),
                    previousQualityScore,
                    previousQualityScore,
                    false,
                    LoopStatus.FAILED,
                    0.0,
                    System.currentTimeMillis());
        }
    }

    /**
     * T function Update Models:trigger strategy learning. async call, does not block on Annotation
     * main path.
     */
    private void triggerStrategyLearning(String viewId, String viewVersion, double currentScore) {
        List<CollectedFeedback> collected =
                feedbackCollector.collectByScene(viewId, viewVersion, "default");
        if (collected == null || collected.size() < MIN_FEEDBACK_FOR_LEARNING) {
            return;
        }
        log.info(
                "Triggering strategy learning for viewId={}, feedbackCount={}",
                viewId,
                collected.size());

        io.github.rawwise.axiom.pipeline.cognition.StrategyLearningContext context =
                new io.github.rawwise.axiom.pipeline.cognition.StrategyLearningContext(
                        collected, "score=" + currentScore, java.util.List.of(), "default");
        strategyLearningService
                .learn(context)
                .ifPresent(
                        strategy -> {
                            persistStrategyAction(strategy, viewId);
                            log.info(
                                    "Learned strategy id={} scene={} confidence={}",
                                    strategy.strategyId(),
                                    strategy.sceneCondition(),
                                    strategy.confidence());
                        });
    }

    private void triggerCognitionDriver(String viewId, double currentScore) {
        String sceneHash = viewId;
        var state = getOrCreateCognitionState(sceneHash, java.util.Map.of("score", currentScore));
        var decision = cognitionDriver.nextStep(state);
        if (decision == null) {
            return;
        }
        try {
            byte[] decisionJson = MAPPER.writeValueAsBytes(decision);
            knowledgeStore.put(
                    ("cognitive_decision:" + decision.decisionId())
                            .getBytes(StandardCharsets.UTF_8),
                    decisionJson);
            log.info(
                    "CognitionDriver decision persisted: id={} type={} scene={} viewId={}",
                    decision.decisionId(),
                    decision.type(),
                    decision.sceneHash(),
                    viewId);
        } catch (Exception e) {
            log.warn(
                    "Failed to persist cognitive decision for viewId={}: {}",
                    viewId,
                    e.getMessage());
        }
    }

    private io.github.rawwise.axiom.pipeline.cognition.CognitiveState getOrCreateCognitionState(
            String sceneHash, java.util.Map<String, Double> metrics) {
        if (cognitionDriver
                instanceof io.github.rawwise.axiom.pipeline.cognition.DefaultCognitionDriver dcd) {
            return dcd.getOrCreateState(sceneHash, metrics);
        }
        return cognitionDriver.currentState();
    }

    /** persist Strategy Action to Knowledge Store(write index first then strategy). */
    private void persistStrategyAction(
            io.github.rawwise.model.record.StrategyAction strategy, String viewId) {
        try {
            byte[] strategyJson = MAPPER.writeValueAsBytes(strategy);
            String sceneHash = Integer.toHexString(strategy.sceneCondition().hashCode());
            // 1. write strategy JSON first(ensure even if subsequent index write fails,strategy can
            // still be found via full scan)
            knowledgeStore.put(
                    strategy.strategyId().getBytes(StandardCharsets.UTF_8), strategyJson);
            // 2. write scenario index
            knowledgeStore.put(
                    ("strategy:scene:" + sceneHash + ":" + strategy.strategyId())
                            .getBytes(StandardCharsets.UTF_8),
                    strategy.strategyId().getBytes(StandardCharsets.UTF_8));
            // 3. write lineage chain
            knowledgeStore.put(
                    ("strategy:lineage:VIEW:" + viewId).getBytes(StandardCharsets.UTF_8),
                    ("STRATEGY:"
                                    + strategy.strategyId()
                                    + ":strategy_learn:"
                                    + System.currentTimeMillis())
                            .getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.warn("Failed to persist strategy {}: {}", strategy.strategyId(), e.getMessage());
        }
    }

    public LoopRecord onWrite(String viewId) {
        if (viewId == null) {
            log.warn("onWrite called with null viewId");
            return new LoopRecord(
                    viewId,
                    "self",
                    0.0,
                    0.0,
                    false,
                    LoopStatus.FAILED,
                    0.0,
                    System.currentTimeMillis());
        }

        View view = loadView(viewId);
        if (view == null) {
            log.warn("View not found for viewId={}", viewId);
            return new LoopRecord(
                    viewId,
                    "self",
                    0.0,
                    0.0,
                    false,
                    LoopStatus.VIEW_NOT_FOUND,
                    0.0,
                    System.currentTimeMillis());
        }

        double previousQualityScore = view.qualityScore().orElse(0.0);

        try {
            EvaluationCriteria criteria = evaluationEngine.getDefaultCriteria(view.viewType());
            var result = evaluationEngine.evaluate(view, criteria);
            double newQualityScore = result.qualityScore();

            updateViewQualityScore(viewId, newQualityScore);

            CognitiveAssetMetric metric = cognitionEngine.incrementalAssetUpdate(viewId);
            double assetValueSnapshot = metric.assetValue();

            boolean shouldCascade = shouldCascade(newQualityScore - previousQualityScore);

            LoopRecord record =
                    new LoopRecord(
                            viewId,
                            "self",
                            previousQualityScore,
                            newQualityScore,
                            shouldCascade,
                            LoopStatus.OK,
                            assetValueSnapshot,
                            System.currentTimeMillis());

            persistLoopRecord(
                    viewId,
                    "self",
                    previousQualityScore,
                    newQualityScore,
                    shouldCascade,
                    LoopStatus.OK,
                    assetValueSnapshot);

            if (shouldCascade) {
                cascadeExecutor.submit(
                        () -> {
                            try {
                                cascadeRederive(viewId, newQualityScore, 0);
                            } catch (Exception e) {
                                log.warn(
                                        "Cascade rederive failed for viewId={}: {}",
                                        viewId,
                                        e.getMessage());
                            }
                        });
            }

            return record;
        } catch (Exception e) {
            log.warn("onWrite auto-evaluation failed for viewId={}: {}", viewId, e.getMessage());
            return new LoopRecord(
                    viewId,
                    "self",
                    previousQualityScore,
                    previousQualityScore,
                    false,
                    LoopStatus.FAILED,
                    0.0,
                    System.currentTimeMillis());
        }
    }

    public void onDerive(String entityId, Layer layer, double selfScore) {
        var signal =
                new DefaultFeedbackSignal(
                        "self:" + entityId,
                        FeedbackSignal.FeedbackSource.SELF,
                        FeedbackSignal.FeedbackPath.EVAL_LAYER,
                        FeedbackSignal.SignalType.QUALITY_SCORE,
                        entityId,
                        layer,
                        selfScore,
                        Map.of("autoGenerated", true),
                        System.currentTimeMillis());
        emitFeedback(signal);
    }

    public void onDeriveBatch(List<String> entityIds, Layer layer, double defaultScore) {
        if (entityIds == null || entityIds.isEmpty()) return;
        long now = System.currentTimeMillis();
        for (String entityId : entityIds) {
            var signal =
                    new DefaultFeedbackSignal(
                            "self:" + entityId,
                            FeedbackSignal.FeedbackSource.SELF,
                            FeedbackSignal.FeedbackPath.EVAL_LAYER,
                            FeedbackSignal.SignalType.QUALITY_SCORE,
                            entityId,
                            layer,
                            defaultScore,
                            Map.of("autoGenerated", true),
                            now);
            persistEvaluationScore(signal);
            persistFactToMetastore(signal);
        }
    }

    private void emitFeedback(FeedbackSignal signal) {
        persistEvaluationScore(signal);
        ingestAsFact(signal);
    }

    private void ingestAsFact(FeedbackSignal signal) {
        if (ingestionEngine != null) {
            try {
                ingestionEngine.ingestFeedback(signal, 0);
                return;
            } catch (Exception e) {
                log.error(
                        "FACT_LAYER ingest via IngestionEngine failed for signal={}: {}",
                        signal.feedbackId(),
                        e.getMessage());
            }
        }
        persistFactToMetastore(signal);
    }

    private void persistFactToMetastore(FeedbackSignal signal) {
        try {
            String key = "feedback_fact:" + signal.feedbackId();
            var node = MAPPER.createObjectNode();
            node.put("feedbackId", signal.feedbackId());
            node.put("source", signal.source().name());
            node.put("path", signal.path().name());
            node.put("targetEntityId", signal.targetEntityId());
            node.put("targetLayer", signal.targetLayer().name());
            node.put("score", signal.score());
            node.put("timestamp", signal.timestamp());
            knowledgeStore.put(
                    key.getBytes(StandardCharsets.UTF_8), MAPPER.writeValueAsBytes(node));
        } catch (IOException e) {
            log.error(
                    "FACT_LAYER persist failed for signal={}: {}",
                    signal.feedbackId(),
                    e.getMessage());
        }
    }

    private void persistEvaluationScore(FeedbackSignal signal) {
        try {
            String key =
                    switch (signal.targetLayer()) {
                        case VIEW -> "view_score:" + signal.targetEntityId();
                        case SEMANTIC -> "semantic_score:" + signal.targetEntityId();
                        case ACTION -> "action_score:" + signal.targetEntityId();
                        case CHUNK -> "chunk_score:" + signal.targetEntityId();
                        case RAW -> "raw_score:" + signal.targetEntityId();
                        case ENTITY -> "entity_score:" + signal.targetEntityId();
                    };
            var node = MAPPER.createObjectNode();
            node.put("signalId", signal.feedbackId());
            node.put("score", signal.score());
            node.put("source", signal.source().name());
            node.put("timestamp", signal.timestamp());
            knowledgeStore.put(
                    key.getBytes(StandardCharsets.UTF_8), MAPPER.writeValueAsBytes(node));
        } catch (IOException e) {
            log.warn(
                    "EVAL_LAYER persist failed for {}: {}",
                    signal.targetEntityId(),
                    e.getMessage());
        }
    }

    /**
     * Fuzzy cascade membership: how much a quality delta warrants cascading re-derivation. Uses
     * sigmoid centered at CASCADE_THRESHOLD with steepness 100.0.
     *
     * <p>Since CASCADE_THRESHOLD=0.05 is a small value, k must be proportionally large to produce
     * meaningful transition. With k=100, the transition zone is ±0.02:
     *
     * <ul>
     *   <li>delta=0.05 → μ=0.50 (boundary)
     *   <li>delta=0.06 → μ≈0.73 (clearly above)
     *   <li>delta=0.07 → μ≈0.88 (strong cascade signal)
     *   <li>delta=0.04 → μ≈0.27 (clearly below)
     * </ul>
     *
     * <p>μ(|delta|) = 1 / (1 + e^(-100*(|delta| - threshold)))
     */
    public double cascadeMembership(double delta) {
        double absDelta = Math.abs(delta);
        return MembershipFunctions.sigmoid(absDelta, CASCADE_THRESHOLD, 100.0);
    }

    public boolean shouldCascade(double delta) {
        return cascadeMembership(delta) >= 0.5;
    }

    public boolean shouldCascade(double delta, FeedbackAnnotation annotation, View view) {
        if (!CASCADE_LLM_ENABLED
                || CASCADE_LLM_ENDPOINT.isEmpty()
                || annotation == null
                || view == null) {
            return shouldCascade(delta);
        }
        try {
            String prompt =
                    "A memory entry received feedback. Decide if the quality change warrants cascading re-derivation "
                            + "(re-evaluating dependent entries). "
                            + "Delta: "
                            + String.format("%.4f", delta)
                            + " (threshold was "
                            + CASCADE_THRESHOLD
                            + "). "
                            + "Annotation type: "
                            + annotation.type()
                            + ", value: "
                            + annotation.value()
                            + ". "
                            + "View type: "
                            + view.viewType()
                            + ", content length: "
                            + (view.content() == null ? 0 : view.content().length)
                            + ". "
                            + "Respond with ONLY 'yes' or 'no'.";
            com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.node.ObjectNode requestBody = mapper.createObjectNode();
            requestBody.put("model", CASCADE_LLM_MODEL);
            com.fasterxml.jackson.databind.node.ArrayNode messages =
                    requestBody.putArray("messages");
            messages.add(mapper.createObjectNode().put("role", "user").put("content", prompt));
            requestBody.put("max_tokens", 5);
            requestBody.put("temperature", 0);

            java.net.http.HttpRequest httpRequest =
                    java.net.http.HttpRequest.newBuilder()
                            .uri(java.net.URI.create(CASCADE_LLM_ENDPOINT))
                            .header("Content-Type", "application/json")
                            .header("Authorization", "Bearer " + CASCADE_LLM_API_KEY)
                            .POST(
                                    java.net.http.HttpRequest.BodyPublishers.ofString(
                                            mapper.writeValueAsString(requestBody)))
                            .timeout(java.time.Duration.ofSeconds(10))
                            .build();

            java.net.http.HttpResponse<String> response =
                    CASCADE_HTTP_CLIENT.send(
                            httpRequest, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                log.warn(
                        "Cascade LLM API returned status={}: {}",
                        response.statusCode(),
                        response.body());
                return shouldCascade(delta);
            }
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(response.body());
            String content =
                    root.path("choices")
                            .path(0)
                            .path("message")
                            .path("content")
                            .asText("")
                            .trim()
                            .toLowerCase();
            boolean llmDecision = content.startsWith("yes");
            log.info(
                    "Cascade LLM: delta={} annotationType={} viewId={} -> {} (threshold fallback would be {})",
                    String.format("%.4f", delta),
                    annotation.type(),
                    annotation.value(),
                    viewId(annotation),
                    llmDecision ? "cascade" : "skip",
                    shouldCascade(delta) ? "cascade" : "skip");
            return llmDecision;
        } catch (Exception e) {
            log.warn("Cascade LLM failed, falling back to threshold: {}", e.getMessage());
            return shouldCascade(delta);
        }
    }

    private static String viewId(FeedbackAnnotation annotation) {
        return annotation == null ? "null" : annotation.annotationId();
    }

    public View loadView(String viewId) {
        try {
            Optional<byte[]> viewBytes = dataAccessor.readView(viewId);
            if (viewBytes.isPresent()) {
                return MAPPER.readValue(viewBytes.get(), View.class);
            }
            return null;
        } catch (Exception e) {
            log.warn("Failed to load view viewId={}: {}", viewId, e.getMessage());
            return null;
        }
    }

    public void updateViewQualityScore(String viewId, double qualityScore) {
        byte[] versionedKey = null;
        byte[] expectedBytes = null;

        for (int attempt = 0; attempt < CAS_MAX_RETRIES; attempt++) {
            View view = loadView(viewId);
            if (view == null) {
                log.warn("Cannot update quality score: view not found for viewId={}", viewId);
                return;
            }

            View updated =
                    new View(
                            view.viewId(),
                            view.sourceChunkIds(),
                            view.viewType(),
                            view.version(),
                            view.content(),
                            view.embedding(),
                            Optional.of(qualityScore),
                            view.annotations());

            try {
                versionedKey =
                        ("view:" + viewId + ":v" + view.version()).getBytes(StandardCharsets.UTF_8);
                byte[] newValue = MAPPER.writeValueAsBytes(updated);

                if (expectedBytes == null) {
                    Optional<byte[]> current = knowledgeStore.get(versionedKey);
                    if (current.isEmpty()) {
                        knowledgeStore.put(versionedKey, newValue);
                        log.debug("CAS skip: key not yet exists, direct put for viewId={}", viewId);
                        return;
                    }
                    expectedBytes = current.get();
                }

                boolean success =
                        knowledgeStore.compareAndSwap(
                                List.of(new Precondition.ValueEquals(versionedKey, expectedBytes)),
                                List.of(new Mutation.Put(versionedKey, newValue)));

                if (success) {
                    log.debug(
                            "CAS success for viewId={}, qualityScore={}, attempt={}",
                            viewId,
                            qualityScore,
                            attempt + 1);
                    return;
                }

                log.debug(
                        "CAS conflict for viewId={}, retrying (attempt={}/{})",
                        viewId,
                        attempt + 1,
                        CAS_MAX_RETRIES);
                expectedBytes = knowledgeStore.get(versionedKey).orElse(null);
                if (expectedBytes == null) {
                    log.warn("View key disappeared during CAS for viewId={}", viewId);
                    return;
                }
            } catch (IOException e) {
                log.warn(
                        "CAS failed for viewId={}, attempt={}: {}",
                        viewId,
                        attempt + 1,
                        e.getMessage());
                return;
            }
        }

        log.warn(
                "CAS exhausted {} retries for viewId={}, falling back to put",
                CAS_MAX_RETRIES,
                viewId);
        try {
            View view = loadView(viewId);
            if (view == null) return;
            View updated =
                    new View(
                            view.viewId(),
                            view.sourceChunkIds(),
                            view.viewType(),
                            view.version(),
                            view.content(),
                            view.embedding(),
                            Optional.of(qualityScore),
                            view.annotations());
            byte[] key =
                    ("view:" + viewId + ":v" + view.version()).getBytes(StandardCharsets.UTF_8);
            knowledgeStore.put(key, MAPPER.writeValueAsBytes(updated));
        } catch (IOException e) {
            log.warn("Fallback put also failed for viewId={}: {}", viewId, e.getMessage());
        }
    }

    public void cascadeRederive(String viewId, double newQualityScore, int depth) {
        if (depth >= MAX_CASCADE_DEPTH) {
            log.debug("Cascade depth limit reached for viewId={}, depth={}", viewId, depth);
            return;
        }

        try {
            byte[] exactKey = ("deriv_from:VIEW:" + viewId).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> chainData = metastore.get(exactKey);
            if (chainData.isEmpty()) {
                log.debug("No derivation chain found for deriv_from:VIEW:{}", viewId);
                return;
            }

            List<String> semanticIds = parseDerivedSemanticIds(chainData.get());

            for (String semanticId : semanticIds) {
                try {
                    cognitionEngine.deriveSemantic(
                            List.of(viewId),
                            Semantic.SemanticType.PATTERN,
                            Map.of("cascadeDepth", depth, "qualityScore", newQualityScore));
                    log.debug(
                            "Cascade rederived semanticId={} from viewId={}, depth={}",
                            semanticId,
                            viewId,
                            depth);
                    cascadeRederive(semanticId, newQualityScore, depth + 1);
                } catch (Exception e) {
                    log.warn(
                            "Cascade rederive failed for semanticId={}: {}",
                            semanticId,
                            e.getMessage());
                }
            }
        } catch (IOException e) {
            log.warn("Cascade scan failed for viewId={}: {}", viewId, e.getMessage());
        }
    }

    private List<String> parseDerivedSemanticIds(byte[] chainData) {
        List<String> ids = new ArrayList<>();
        try {
            String valueStr = new String(chainData, StandardCharsets.UTF_8);
            for (String entry : valueStr.split(",")) {
                String[] parts = entry.trim().split(":");
                if (parts.length >= 2 && "SEMANTIC".equals(parts[0])) {
                    ids.add(parts[1]);
                }
            }
        } catch (Exception e) {
            log.debug("Failed to parse derivation chain: {}", e.getMessage());
        }
        return ids;
    }

    private void persistLoopRecord(
            String viewId,
            String annotationId,
            double previousQualityScore,
            double newQualityScore,
            boolean cascaded,
            LoopStatus status,
            double assetValueSnapshot) {
        LoopRecord record =
                new LoopRecord(
                        viewId,
                        annotationId,
                        previousQualityScore,
                        newQualityScore,
                        cascaded,
                        status,
                        assetValueSnapshot,
                        System.currentTimeMillis());

        try {
            byte[] key =
                    ("loop_record:" + viewId + ":" + annotationId + ":" + record.timestamp())
                            .getBytes(StandardCharsets.UTF_8);
            knowledgeStore.put(key, MAPPER.writeValueAsBytes(record));
            log.debug("Persisted loop record for viewId={}, annotationId={}", viewId, annotationId);
        } catch (IOException e) {
            log.warn("Failed to persist loop record for viewId={}: {}", viewId, e.getMessage());
        }
    }

    private void writeVersionedAnnotation(
            String viewId, int viewVersion, FeedbackAnnotation annotation) {
        try {
            byte[] annotKey =
                    viewVersion > 0
                            ? ("view_annotations:" + viewId + ":v" + viewVersion)
                                    .getBytes(StandardCharsets.UTF_8)
                            : ("view_annotations:" + viewId).getBytes(StandardCharsets.UTF_8);

            var existingData = knowledgeStore.get(annotKey);
            var annotations = new ArrayList<Map<String, Object>>();
            if (existingData.isPresent()) {
                var existing =
                        MAPPER.readValue(
                                existingData.get(),
                                new com.fasterxml.jackson.core.type.TypeReference<
                                        List<Map<String, Object>>>() {});
                annotations.addAll(existing);
            }

            var annMap = new java.util.LinkedHashMap<String, Object>();
            annMap.put("annotationId", annotation.annotationId());
            annMap.put("viewId", annotation.viewId());
            annMap.put("annotatorId", annotation.annotatorId());
            annMap.put("type", annotation.type().name());
            annMap.put("value", annotation.value());
            annMap.put("confidence", annotation.confidence());
            annMap.put("timestamp", annotation.timestamp());
            annotations.add(annMap);

            knowledgeStore.put(annotKey, MAPPER.writeValueAsBytes(annotations));
            log.info(
                    "[feedback] Wrote annotation key={} count_total={} value={} type={}",
                    new String(annotKey, StandardCharsets.UTF_8),
                    annotations.size(),
                    annotation.value(),
                    annotation.type().name());
        } catch (Exception e) {
            log.warn(
                    "Failed to write versioned annotation for viewId={}: {}",
                    viewId,
                    e.getMessage());
        }
    }

    /** strategy learningthread. */
    public void close() {
        strategyLearningExecutor.shutdownNow();
        try {
            if (!strategyLearningExecutor.awaitTermination(
                    5, java.util.concurrent.TimeUnit.SECONDS)) {
                log.warn("Strategy learning executor did not terminate gracefully");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
