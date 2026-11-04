package io.github.rawwise.axiom.pipeline.cognition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.axiom.pipeline.derivation.DerivationChain;
import io.github.rawwise.axiom.pipeline.derivation.Layer;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Action;
import io.github.rawwise.model.record.Semantic;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultCognitionEngine implements CognitionEngine {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

    private final Metastore metastore;
    private final KnowledgeStore knowledgeStore;
    private final KnowledgeDataAccessor dataAccessor;
    private final EvaluationEngine evaluationEngine;
    private final ActionExecutor actionExecutor;
    private final SimilarityService similarityService;
    private final double timeDecayAlpha;
    private final DerivationFeedbackCallback feedbackCallback;

    public DefaultCognitionEngine(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            EvaluationEngine evaluationEngine,
            ActionExecutor actionExecutor) {
        this(
                metastore,
                knowledgeStore,
                evaluationEngine,
                actionExecutor,
                new DefaultSimilarityService(),
                0.01,
                null);
    }

    public DefaultCognitionEngine(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            EvaluationEngine evaluationEngine,
            ActionExecutor actionExecutor,
            double timeDecayAlpha) {
        this(
                metastore,
                knowledgeStore,
                evaluationEngine,
                actionExecutor,
                new DefaultSimilarityService(),
                timeDecayAlpha,
                null);
    }

    public DefaultCognitionEngine(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            EvaluationEngine evaluationEngine,
            ActionExecutor actionExecutor,
            SimilarityService similarityService,
            double timeDecayAlpha) {
        this(
                metastore,
                knowledgeStore,
                evaluationEngine,
                actionExecutor,
                similarityService,
                timeDecayAlpha,
                null);
    }

    public DefaultCognitionEngine(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            EvaluationEngine evaluationEngine,
            ActionExecutor actionExecutor,
            DerivationFeedbackCallback feedbackCallback) {
        this(
                metastore,
                knowledgeStore,
                evaluationEngine,
                actionExecutor,
                new DefaultSimilarityService(),
                0.01,
                feedbackCallback);
    }

    public DefaultCognitionEngine(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            EvaluationEngine evaluationEngine,
            ActionExecutor actionExecutor,
            SimilarityService similarityService,
            double timeDecayAlpha,
            DerivationFeedbackCallback feedbackCallback) {
        this.metastore = metastore;
        this.knowledgeStore = knowledgeStore;
        this.dataAccessor = new KnowledgeDataAccessor(metastore, knowledgeStore);
        this.evaluationEngine = evaluationEngine;
        this.actionExecutor = actionExecutor;
        this.similarityService = similarityService;
        this.timeDecayAlpha = timeDecayAlpha;
        this.feedbackCallback = feedbackCallback;
    }

    @Override
    public Semantic deriveSemantic(
            List<String> viewIds, Semantic.SemanticType type, Map<String, Object> params) {
        String semanticId = "sem:" + UUID.randomUUID().toString().substring(0, 8);
        byte[] content = buildSemanticContent(viewIds, type, params);

        Semantic semantic = new Semantic(semanticId, List.copyOf(viewIds), type, content);

        try {
            for (String viewId : viewIds) {
                DerivationChain chain =
                        new DerivationChain(
                                viewId,
                                Layer.VIEW,
                                semanticId,
                                Layer.SEMANTIC,
                                "semantic_derive:" + type.name().toLowerCase(),
                                System.currentTimeMillis());
                writeDerivationChain(chain);
            }

            byte[] semKey = semanticKey(semanticId);
            knowledgeStore.put(semKey, content);
        } catch (IOException e) {
            log.warn("Failed to persist semantic {}: {}", semanticId, e.getMessage());
        }

        log.debug("Derived semantic {} from {} views, type={}", semanticId, viewIds.size(), type);

        if (feedbackCallback != null) {
            try {
                double completenessScore = computeSemanticCompleteness(content);
                feedbackCallback.onDerive(semanticId, Layer.SEMANTIC, completenessScore);
            } catch (Exception e) {
                log.debug("F_self callback failed for semantic {}: {}", semanticId, e.getMessage());
            }
        }

        return semantic;
    }

    @Override
    public Action deriveAction(
            List<String> semanticIds, Action.ActionType type, Map<String, Object> params) {
        String actionId = "act:" + UUID.randomUUID().toString().substring(0, 8);
        byte[] content = buildActionContent(semanticIds, type, params);

        Action action = new Action(actionId, List.copyOf(semanticIds), type, content, true);

        try {
            for (String semanticId : semanticIds) {
                DerivationChain chain =
                        new DerivationChain(
                                semanticId,
                                Layer.SEMANTIC,
                                actionId,
                                Layer.ACTION,
                                "action_derive:" + type.name().toLowerCase(),
                                System.currentTimeMillis());
                writeDerivationChain(chain);
            }

            byte[] actKey = actionKey(actionId);
            knowledgeStore.put(actKey, content);
        } catch (IOException e) {
            log.warn("Failed to persist action {}: {}", actionId, e.getMessage());
        }

        log.debug(
                "Derived action {} from {} semantics, type={}", actionId, semanticIds.size(), type);

        if (feedbackCallback != null) {
            try {
                feedbackCallback.onDerive(actionId, Layer.ACTION, action.executable() ? 1.0 : 0.0);
            } catch (Exception e) {
                log.debug("F_self callback failed for action {}: {}", actionId, e.getMessage());
            }
        }

        return action;
    }

    @Override
    public ActionPlan deriveActionPlan(
            List<String> semanticIds, PlanType planType, Map<String, Object> params) {
        String planId = "plan:" + UUID.randomUUID().toString().substring(0, 8);

        List<Action> actions = new ArrayList<>();
        if (planType == PlanType.CHAIN) {
            actions.add(deriveAction(semanticIds, Action.ActionType.RETRIEVE, params));
            actions.add(deriveAction(semanticIds, Action.ActionType.TRANSFORM, params));
        } else {
            Set<String> retrieveSemIds =
                    new HashSet<>(
                            semanticIds.subList(
                                    0, Math.min(semanticIds.size(), semanticIds.size() / 2 + 1)));
            actions.add(
                    deriveAction(
                            new ArrayList<>(retrieveSemIds), Action.ActionType.RETRIEVE, params));
            actions.add(deriveAction(semanticIds, Action.ActionType.NOTIFY, params));
        }

        try {
            persistPlanActions(planId, actions);
        } catch (IOException e) {
            log.warn("Failed to persist plan actions for {}: {}", planId, e.getMessage());
        }

        Map<String, Set<String>> dependencies =
                planType == PlanType.DAG ? buildDagDependencies(actions) : Map.of();

        log.debug(
                "Derived action plan {} with {} actions, type={}",
                planId,
                actions.size(),
                planType);
        return new ActionPlan(
                planId, Collections.unmodifiableList(actions), planType, dependencies);
    }

    @Override
    public ActionResult execute(String actionId, ExecutionContext context) {
        try {
            byte[] actKey = actionKey(actionId);
            var raw = dataAccessor.getWithFallback(actKey);
            if (raw.isEmpty()) {
                log.warn("Action {} not found", actionId);
                return new ActionResult(
                        actionId, false, "action not found", Map.of(), System.currentTimeMillis());
            }

            List<String> semanticIds = extractSemanticIds(actionId);
            Action action =
                    new Action(actionId, semanticIds, Action.ActionType.RETRIEVE, raw.get(), true);
            return actionExecutor.execute(action, context);
        } catch (IOException e) {
            log.warn("Failed to execute action {}: {}", actionId, e.getMessage());
            return new ActionResult(
                    actionId, false, e.getMessage(), Map.of(), System.currentTimeMillis());
        }
    }

    @Override
    public PlanResult executePlan(String planId, ExecutionContext context) {
        try {
            List<Action> actions = loadPlanActions(planId);

            if (actions.isEmpty()) {
                return new PlanResult(
                        planId, false, List.of(), "no actions in plan", System.currentTimeMillis());
            }

            ActionPlan plan = new ActionPlan(planId, actions, PlanType.CHAIN, Map.of());
            return actionExecutor.executePlan(plan, context);
        } catch (Exception e) {
            log.warn("Failed to execute plan {}: {}", planId, e.getMessage());
            return new PlanResult(
                    planId, false, List.of(), e.getMessage(), System.currentTimeMillis());
        }
    }

    @Override
    public CognitiveAssetMetric cognitiveAssetMetric() {
        try {
            byte[] viewPrefix = "view:".getBytes(StandardCharsets.UTF_8);
            List<KeyValue> viewEntries = dataAccessor.scanWithFallback(viewPrefix);
            long totalViews = viewEntries.size();

            byte[] annotPrefix = "view_annotations:".getBytes(StandardCharsets.UTF_8);
            List<KeyValue> annotEntries = dataAccessor.scanWithFallback(annotPrefix);

            double feedbackCoverage =
                    totalViews > 0 ? (double) annotEntries.size() / totalViews : 1.0;

            long totalFeedbackRecords = annotEntries.size();

            double assetValue = computeAssetValue(totalViews, annotEntries, feedbackCoverage);

            return new CognitiveAssetMetric(totalFeedbackRecords, feedbackCoverage, assetValue);
        } catch (IOException e) {
            log.warn("Failed to compute cognitive asset metric: {}", e.getMessage());
            return new CognitiveAssetMetric(0, 0.0, 0.0);
        }
    }

    @Override
    public CognitiveAssetMetric incrementalAssetUpdate(String viewId) {
        try {
            List<KeyValue> annotEntries = loadVersionedAnnotations(viewId);
            if (annotEntries.isEmpty()) {
                return cognitiveAssetMetric();
            }

            byte[] viewPrefix = "view:".getBytes(StandardCharsets.UTF_8);
            long totalViews = Math.max(1, dataAccessor.scanWithFallback(viewPrefix).size());

            double feedbackCoverage = 1.0 / totalViews;

            double assetValue = computeAssetValue(totalViews, annotEntries, feedbackCoverage);

            return new CognitiveAssetMetric(annotEntries.size(), feedbackCoverage, assetValue);
        } catch (IOException e) {
            log.warn(
                    "Failed to compute incremental asset update for viewId={}: {}",
                    viewId,
                    e.getMessage());
            return new CognitiveAssetMetric(0, 0.0, 0.0);
        }
    }

    private List<KeyValue> loadVersionedAnnotations(String viewId) throws IOException {
        byte[] annotPrefix = ("view_annotations:" + viewId + ":v").getBytes(StandardCharsets.UTF_8);
        List<KeyValue> entries = dataAccessor.scanWithFallback(annotPrefix);
        if (!entries.isEmpty()) {
            return entries;
        }
        byte[] legacyKey = ("view_annotations:" + viewId).getBytes(StandardCharsets.UTF_8);
        var legacyData = dataAccessor.getWithFallback(legacyKey);
        if (legacyData.isPresent()) {
            return List.of(new KeyValue(legacyKey, legacyData.get()));
        }
        return List.of();
    }

    private double computeAssetValue(
            long totalViews, List<KeyValue> annotEntries, double coverage) {
        if (totalViews == 0 || annotEntries.isEmpty()) {
            return 0.0;
        }

        long now = System.currentTimeMillis();
        double weightedSum = 0.0;
        int qualityCount = 0;

        for (KeyValue kv : annotEntries) {
            try {
                List<Map<String, Object>> annotations =
                        MAPPER.readValue(
                                kv.value(),
                                new com.fasterxml.jackson.core.type.TypeReference<
                                        List<Map<String, Object>>>() {});
                for (Map<String, Object> ann : annotations) {
                    String type = (String) ann.get("type");
                    if ("QUALITY_SCORE".equals(type)) {
                        Object value = ann.get("value");
                        double q = value instanceof Number n ? n.doubleValue() : 1.0;
                        if (ann.containsKey("confidence")) {
                            double confidence = ((Number) ann.get("confidence")).doubleValue();
                            q = q * confidence;
                        }
                        Long ts =
                                ann.containsKey("timestamp")
                                        ? ((Number) ann.get("timestamp")).longValue()
                                        : now;
                        double w = Math.exp(-timeDecayAlpha * (now - ts) / 1000.0);
                        weightedSum += w * q;
                        qualityCount++;
                    }
                }
            } catch (IOException e) {
                log.debug("Failed to parse annotation entry: {}", e.getMessage());
            }
        }

        if (qualityCount == 0) {
            return annotEntries.size() * coverage;
        }

        return weightedSum * coverage;
    }

    private void writeDerivationChain(DerivationChain chain) throws IOException {
        byte[] key =
                ("deriv_src:" + chain.sourceLayer() + ":" + chain.sourceId())
                        .getBytes(StandardCharsets.UTF_8);
        byte[] derivedEntry =
                (chain.derivedLayer()
                                + ":"
                                + chain.derivedId()
                                + ":"
                                + chain.deriveOp()
                                + ":"
                                + chain.timestamp())
                        .getBytes(StandardCharsets.UTF_8);
        metastore.put(key, derivedEntry);
    }

    private void persistPlanActions(String planId, List<Action> actions) throws IOException {
        for (int i = 0; i < actions.size(); i++) {
            Action action = actions.get(i);
            byte[] key = ("plan_action:" + planId + ":" + i).getBytes(StandardCharsets.UTF_8);
            knowledgeStore.put(key, action.content());
        }
    }

    private byte[] buildSemanticContent(
            List<String> viewIds, Semantic.SemanticType type, Map<String, Object> params) {
        try {
            List<Map<String, Object>> viewContents = readViewContents(viewIds);
            Map<String, Object> content = new LinkedHashMap<>();
            content.put("semanticType", type.name());
            content.put("sourceViewIds", viewIds);
            content.put("derivedAt", System.currentTimeMillis());

            switch (type) {
                case CONCEPT -> buildConceptContent(content, viewContents, params);
                case RELATION -> buildRelationContent(content, viewContents, params);
                case PATTERN -> buildPatternContent(content, viewContents, params);
                case PREFERENCE -> buildPreferenceContent(content, viewContents, params);
            }

            return MAPPER.writeValueAsBytes(content);
        } catch (Exception e) {
            log.warn("Failed to build semantic content: {}", e.getMessage());
            return fallbackSemanticContent(viewIds, type);
        }
    }

    private void buildConceptContent(
            Map<String, Object> content,
            List<Map<String, Object>> viewContents,
            Map<String, Object> params) {
        Map<String, Integer> termFrequencies = new LinkedHashMap<>();
        for (Map<String, Object> vc : viewContents) {
            extractTerms(vc).forEach(term -> termFrequencies.merge(term, 1, Integer::sum));
        }
        List<Map<String, Object>> concepts =
                termFrequencies.entrySet().stream()
                        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                        .limit(getIntParam(params, "maxConcepts", 10))
                        .map(
                                e -> {
                                    Map<String, Object> concept = new LinkedHashMap<>();
                                    concept.put("term", e.getKey());
                                    concept.put("frequency", e.getValue());
                                    concept.put("confidence", Math.min(1.0, e.getValue() / 5.0));
                                    return concept;
                                })
                        .toList();
        content.put("concepts", concepts);
        content.put("conceptCount", concepts.size());
    }

    private void buildRelationContent(
            Map<String, Object> content,
            List<Map<String, Object>> viewContents,
            Map<String, Object> params) {
        List<Map<String, Object>> relations = new ArrayList<>();
        for (int i = 0; i < viewContents.size(); i++) {
            for (int j = i + 1; j < viewContents.size(); j++) {
                Map<String, Object> vc1 = viewContents.get(i);
                Map<String, Object> vc2 = viewContents.get(j);
                double similarity = computeSimilarity(vc1, vc2);
                double relationMu = MembershipFunctions.sigmoid(similarity, 0.1, 4.0);
                if (relationMu >= 0.5) {
                    Map<String, Object> relation = new LinkedHashMap<>();
                    relation.put("sourceViewIndex", i);
                    relation.put("targetViewIndex", j);
                    relation.put("similarity", similarity);
                    relation.put("relationType", classifyRelationType(similarity));
                    relations.add(relation);
                }
            }
        }
        content.put("relations", relations);
        content.put("relationCount", relations.size());
    }

    private void buildPatternContent(
            Map<String, Object> content,
            List<Map<String, Object>> viewContents,
            Map<String, Object> params) {
        List<Map<String, Object>> patterns = new ArrayList<>();
        Map<String, Integer> structureCounts = new LinkedHashMap<>();
        for (Map<String, Object> vc : viewContents) {
            String structure = extractStructureSignature(vc);
            structureCounts.merge(structure, 1, Integer::sum);
        }
        for (Map.Entry<String, Integer> entry : structureCounts.entrySet()) {
            if (entry.getValue() >= 2) {
                Map<String, Object> pattern = new LinkedHashMap<>();
                pattern.put("signature", entry.getKey());
                pattern.put("occurrences", entry.getValue());
                pattern.put(
                        "strength", Math.min(1.0, entry.getValue() / (double) viewContents.size()));
                patterns.add(pattern);
            }
        }
        content.put("patterns", patterns);
        content.put("patternCount", patterns.size());
        content.put("viewCount", viewContents.size());
    }

    private void buildPreferenceContent(
            Map<String, Object> content,
            List<Map<String, Object>> viewContents,
            Map<String, Object> params) {
        Map<String, Object> preferences = new LinkedHashMap<>();
        Map<String, Double> fieldScores = new LinkedHashMap<>();
        for (Map<String, Object> vc : viewContents) {
            for (Map.Entry<String, Object> entry : vc.entrySet()) {
                String key = entry.getKey();
                double score = scoreFieldValue(entry.getValue());
                fieldScores.merge(key, score, Double::sum);
            }
        }
        int viewCount = Math.max(1, viewContents.size());
        List<Map<String, Object>> rankedFields =
                fieldScores.entrySet().stream()
                        .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                        .limit(getIntParam(params, "maxPreferences", 5))
                        .map(
                                e -> {
                                    Map<String, Object> pref = new LinkedHashMap<>();
                                    pref.put("field", e.getKey());
                                    pref.put("avgScore", e.getValue() / viewCount);
                                    double avgScore = e.getValue() / viewCount;
                                    double highPrefMu =
                                            MembershipFunctions.sigmoid(avgScore, 0.5, 4.0);
                                    pref.put("preference", highPrefMu >= 0.5 ? "high" : "low");
                                    return pref;
                                })
                        .toList();
        preferences.put("rankedFields", rankedFields);
        content.put("preferences", preferences);
    }

    private byte[] buildActionContent(
            List<String> semanticIds, Action.ActionType type, Map<String, Object> params) {
        try {
            List<Map<String, Object>> semanticContents = readSemanticContents(semanticIds);
            Map<String, Object> content = new LinkedHashMap<>();
            content.put("actionType", type.name());
            content.put("sourceSemanticIds", semanticIds);
            content.put("generatedAt", System.currentTimeMillis());

            switch (type) {
                case RETRIEVE -> buildRetrieveSpec(content, semanticContents, params);
                case TRANSFORM -> buildTransformSpec(content, semanticContents, params);
                case NOTIFY -> buildNotifySpec(content, semanticContents, params);
                case CORRECT -> buildCorrectSpec(content, semanticContents, params);
            }

            return MAPPER.writeValueAsBytes(content);
        } catch (Exception e) {
            log.warn("Failed to build action content: {}", e.getMessage());
            return fallbackActionContent(semanticIds, type);
        }
    }

    private void buildRetrieveSpec(
            Map<String, Object> content,
            List<Map<String, Object>> semanticContents,
            Map<String, Object> params) {
        List<String> queryTerms = new ArrayList<>();
        for (Map<String, Object> sc : semanticContents) {
            extractQueryTerms(sc).forEach(queryTerms::add);
        }
        int topK = getIntParam(params, "topK", 10);
        double minScore = getDoubleParam(params, "minScore", 0.5);
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("queryTerms", queryTerms.stream().distinct().toList());
        spec.put("topK", topK);
        spec.put("minScore", minScore);
        spec.put("strategy", queryTerms.size() > 5 ? "semantic" : "keyword");
        content.put("retrieveSpec", spec);
    }

    private void buildTransformSpec(
            Map<String, Object> content,
            List<Map<String, Object>> semanticContents,
            Map<String, Object> params) {
        String mode = (String) params.getOrDefault("mode", "aggregate");
        List<String> inputTypes =
                semanticContents.stream()
                        .map(sc -> (String) sc.getOrDefault("semanticType", "UNKNOWN"))
                        .distinct()
                        .toList();
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("transformMode", mode);
        spec.put("inputSemanticTypes", inputTypes);
        spec.put("outputFormat", "view");
        spec.put("mergeStrategy", inputTypes.size() > 1 ? "cross_reference" : "direct");
        content.put("transformSpec", spec);
    }

    private void buildNotifySpec(
            Map<String, Object> content,
            List<Map<String, Object>> semanticContents,
            Map<String, Object> params) {
        List<String> channels = new ArrayList<>();
        Object channelsParam = params.get("channels");
        if (channelsParam instanceof List<?> list) {
            list.forEach(c -> channels.add(String.valueOf(c)));
        } else {
            channels.add("default");
        }
        String severity = (String) params.getOrDefault("severity", "info");
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("channels", channels);
        spec.put("severity", severity);
        spec.put("triggerSemanticCount", semanticContents.size());
        spec.put("messageTemplate", buildNotifyTemplate(semanticContents));
        content.put("notifySpec", spec);
    }

    private void buildCorrectSpec(
            Map<String, Object> content,
            List<Map<String, Object>> semanticContents,
            Map<String, Object> params) {
        List<Map<String, Object>> corrections = new ArrayList<>();
        for (Map<String, Object> sc : semanticContents) {
            List<String> issues = detectIssues(sc);
            if (!issues.isEmpty()) {
                Map<String, Object> correction = new LinkedHashMap<>();
                correction.put("sourceSemanticType", sc.get("semanticType"));
                correction.put("issues", issues);
                correction.put("confidence", computeCorrectionConfidence(issues));
                corrections.add(correction);
            }
        }
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("corrections", corrections);
        spec.put("correctionCount", corrections.size());
        spec.put("autoApply", getBoolParam(params, "autoApply", false));
        content.put("correctSpec", spec);
    }

    private List<Map<String, Object>> readViewContents(List<String> viewIds) {
        List<Map<String, Object>> contents = new ArrayList<>();
        for (String viewId : viewIds) {
            try {
                var raw = dataAccessor.readView(viewId);
                if (raw.isPresent()) {
                    contents.add(
                            MAPPER.readValue(
                                    raw.get(),
                                    new com.fasterxml.jackson.core.type.TypeReference<
                                            Map<String, Object>>() {}));
                } else {
                    contents.add(Map.of("viewId", viewId, "status", "not_found"));
                }
            } catch (IOException e) {
                contents.add(Map.of("viewId", viewId, "error", e.getMessage()));
            }
        }
        return contents;
    }

    private List<Map<String, Object>> readSemanticContents(List<String> semanticIds) {
        List<Map<String, Object>> contents = new ArrayList<>();
        for (String semanticId : semanticIds) {
            try {
                byte[] key = semanticKey(semanticId);
                var raw = dataAccessor.getWithFallback(key);
                if (raw.isPresent()) {
                    contents.add(
                            MAPPER.readValue(
                                    raw.get(),
                                    new com.fasterxml.jackson.core.type.TypeReference<
                                            Map<String, Object>>() {}));
                } else {
                    contents.add(Map.of("semanticId", semanticId, "status", "not_found"));
                }
            } catch (IOException e) {
                contents.add(Map.of("semanticId", semanticId, "error", e.getMessage()));
            }
        }
        return contents;
    }

    private List<String> extractTerms(Map<String, Object> viewContent) {
        List<String> terms = new ArrayList<>();
        for (Map.Entry<String, Object> entry : viewContent.entrySet()) {
            if (entry.getValue() instanceof String s && !s.isBlank()) {
                for (String word : s.split("[\\s,;:.!?]+")) {
                    if (word.length() > 2) {
                        terms.add(word.toLowerCase());
                    }
                }
            }
        }
        return terms;
    }

    private double computeSimilarity(Map<String, Object> vc1, Map<String, Object> vc2) {
        try {
            String text1 = MAPPER.writeValueAsString(vc1);
            String text2 = MAPPER.writeValueAsString(vc2);
            return similarityService.computeSimilarity(text1, text2);
        } catch (Exception e) {
            return 0.0;
        }
    }

    private String classifyRelationType(double similarity) {
        if (similarity > 0.8) return "equivalent";
        if (similarity > 0.5) return "related";
        return "weakly_related";
    }

    private String extractStructureSignature(Map<String, Object> viewContent) {
        List<String> sortedKeys = viewContent.keySet().stream().sorted().toList();
        return String.join(",", sortedKeys);
    }

    private double scoreFieldValue(Object value) {
        if (value == null) return 0.0;
        if (value instanceof Number) return 0.8;
        if (value instanceof String s) return Math.min(1.0, s.length() / 100.0);
        if (value instanceof Map) return 0.6;
        if (value instanceof List<?> l) return Math.min(1.0, l.size() / 5.0);
        return 0.3;
    }

    private List<String> extractQueryTerms(Map<String, Object> semanticContent) {
        List<String> terms = new ArrayList<>();
        Object concepts = semanticContent.get("concepts");
        if (concepts instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> m && m.get("term") instanceof String term) {
                    terms.add(term);
                }
            }
        }
        if (terms.isEmpty()) {
            for (Map.Entry<String, Object> entry : semanticContent.entrySet()) {
                if (entry.getValue() instanceof String s && s.length() > 2) {
                    terms.add(s);
                }
            }
        }
        return terms;
    }

    private String buildNotifyTemplate(List<Map<String, Object>> semanticContents) {
        long conceptCount =
                semanticContents.stream()
                        .filter(sc -> "CONCEPT".equals(sc.get("semanticType")))
                        .count();
        long relationCount =
                semanticContents.stream()
                        .filter(sc -> "RELATION".equals(sc.get("semanticType")))
                        .count();
        return String.format(
                "Cognitive update: %d concepts, %d relations derived", conceptCount, relationCount);
    }

    private List<String> detectIssues(Map<String, Object> semanticContent) {
        List<String> issues = new ArrayList<>();
        Object conceptCount = semanticContent.get("conceptCount");
        if (conceptCount instanceof Number n && n.intValue() == 0) {
            issues.add("no_concepts_extracted");
        }
        Object relationCount = semanticContent.get("relationCount");
        if (relationCount instanceof Number n && n.intValue() == 0) {
            issues.add("no_relations_found");
        }
        Object patternCount = semanticContent.get("patternCount");
        if (patternCount instanceof Number n && n.intValue() == 0) {
            issues.add("no_patterns_detected");
        }
        if (semanticContent.containsKey("error")) {
            issues.add("source_error:" + semanticContent.get("error"));
        }
        return issues;
    }

    private double computeCorrectionConfidence(List<String> issues) {
        if (issues.isEmpty()) return 1.0;
        long criticalIssues = issues.stream().filter(i -> i.startsWith("source_error")).count();
        return Math.max(0.1, 1.0 - criticalIssues * 0.3);
    }

    private byte[] fallbackSemanticContent(List<String> viewIds, Semantic.SemanticType type) {
        try {
            Map<String, Object> content = new LinkedHashMap<>();
            content.put("semanticType", type.name());
            content.put("sourceViewIds", viewIds);
            content.put("fallback", true);
            return MAPPER.writeValueAsBytes(content);
        } catch (Exception e) {
            return ("{semantic:" + type.name() + "}").getBytes(StandardCharsets.UTF_8);
        }
    }

    private byte[] fallbackActionContent(List<String> semanticIds, Action.ActionType type) {
        try {
            Map<String, Object> content = new LinkedHashMap<>();
            content.put("actionType", type.name());
            content.put("sourceSemanticIds", semanticIds);
            content.put("fallback", true);
            return MAPPER.writeValueAsBytes(content);
        } catch (Exception e) {
            return ("{action:" + type.name() + "}").getBytes(StandardCharsets.UTF_8);
        }
    }

    private static int getIntParam(Map<String, Object> params, String key, int defaultValue) {
        Object val = params != null ? params.get(key) : null;
        if (val instanceof Number n) return n.intValue();
        return defaultValue;
    }

    private static double getDoubleParam(
            Map<String, Object> params, String key, double defaultValue) {
        Object val = params != null ? params.get(key) : null;
        if (val instanceof Number n) return n.doubleValue();
        return defaultValue;
    }

    private static boolean getBoolParam(
            Map<String, Object> params, String key, boolean defaultValue) {
        Object val = params != null ? params.get(key) : null;
        if (val instanceof Boolean b) return b;
        return defaultValue;
    }

    private Map<String, Set<String>> buildDagDependencies(List<Action> actions) {
        Map<String, Set<String>> deps = new HashMap<>();
        if (actions.size() >= 2) {
            deps.put(actions.get(1).actionId(), Set.of(actions.get(0).actionId()));
        }
        return deps;
    }

    private List<String> extractSemanticIds(String actionId) {
        try {
            byte[] prefix = "deriv_src:SEMANTIC:".getBytes(StandardCharsets.UTF_8);
            List<KeyValue> entries = metastore.scanAll(prefix);
            List<String> ids = new ArrayList<>();
            for (KeyValue kv : entries) {
                String valueStr = new String(kv.value(), StandardCharsets.UTF_8);
                if (valueStr.contains(actionId)) {
                    String keyStr = new String(kv.key(), StandardCharsets.UTF_8);
                    ids.add(keyStr.substring("deriv_src:SEMANTIC:".length()));
                }
            }
            return ids;
        } catch (IOException e) {
            log.warn("Failed to extract semantic IDs for action {}: {}", actionId, e.getMessage());
            return List.of();
        }
    }

    private List<Action> loadPlanActions(String planId) {
        try {
            byte[] prefix = ("plan_action:" + planId).getBytes(StandardCharsets.UTF_8);
            List<KeyValue> entries = dataAccessor.scanWithFallback(prefix);
            List<Action> actions = new ArrayList<>();
            for (KeyValue kv : entries) {
                String keyStr = new String(kv.key(), StandardCharsets.UTF_8);
                String actionId = keyStr.substring(keyStr.lastIndexOf(':') + 1);
                actions.add(
                        new Action(
                                actionId, List.of(), Action.ActionType.RETRIEVE, kv.value(), true));
            }
            return actions;
        } catch (IOException e) {
            log.warn("Failed to load plan actions for {}: {}", planId, e.getMessage());
            return List.of();
        }
    }

    private static byte[] semanticKey(String semanticId) {
        return ("semantic:" + semanticId).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] actionKey(String actionId) {
        return ("action:" + actionId).getBytes(StandardCharsets.UTF_8);
    }

    private double computeSemanticCompleteness(byte[] content) {
        if (content == null || content.length == 0) return 0.0;
        try {
            var map =
                    MAPPER.readValue(
                            content,
                            new com.fasterxml.jackson.core.type.TypeReference<
                                    Map<String, Object>>() {});
            long totalFields = map.size();
            long nonEmptyFields =
                    map.values().stream()
                            .filter(v -> v != null && !(v instanceof String s && s.isBlank()))
                            .count();
            return totalFields > 0 ? (double) nonEmptyFields / totalFields : 0.0;
        } catch (Exception e) {
            return content.length > 0 ? 0.5 : 0.0;
        }
    }
}
