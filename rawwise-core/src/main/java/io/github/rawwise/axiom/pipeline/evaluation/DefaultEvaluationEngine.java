package io.github.rawwise.axiom.pipeline.evaluation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.FeedbackAnnotation;
import io.github.rawwise.model.record.View;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultEvaluationEngine implements EvaluationEngine {

    private static final String HISTORY_KEY_PREFIX = "eval_history:";
    private static final int MAX_HISTORY_ENTRIES = 100;
    private static final double DEFAULT_MAX_AGE_DAYS = 30.0;
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new Jdk8Module())
                    .enable(SerializationFeature.INDENT_OUTPUT);

    private final Metastore metastore;
    private final KnowledgeStore knowledgeStore;
    private final KnowledgeDataAccessor dataAccessor;
    private final ExternalEvaluationProvider externalProvider;
    private final ConcurrentHashMap<String, Object> historyLocks = new ConcurrentHashMap<>();

    public DefaultEvaluationEngine(Metastore metastore, KnowledgeStore knowledgeStore) {
        this(metastore, knowledgeStore, new DefaultExternalEvaluationProvider());
    }

    public DefaultEvaluationEngine(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            ExternalEvaluationProvider externalProvider) {
        this.metastore = metastore;
        this.knowledgeStore = knowledgeStore;
        this.dataAccessor = new KnowledgeDataAccessor(metastore, knowledgeStore);
        this.externalProvider =
                externalProvider != null
                        ? externalProvider
                        : new DefaultExternalEvaluationProvider();
    }

    @Override
    public EvaluationResult evaluate(View view, EvaluationCriteria criteria) {
        View.ViewType viewType = view.viewType();
        Map<EvaluationDimension, Double> dimensionScores = new EnumMap<>(EvaluationDimension.class);
        StringJoiner explanation = new StringJoiner("; ");

        double completeness = scoreCompleteness(view, viewType, criteria, explanation);
        dimensionScores.put(EvaluationDimension.COMPLETENESS, completeness);

        double relevance = scoreRelevance(view, viewType, criteria, explanation);
        dimensionScores.put(EvaluationDimension.RELEVANCE, relevance);

        double freshness = scoreFreshness(view, criteria, explanation);
        dimensionScores.put(EvaluationDimension.FRESHNESS, freshness);

        double perceptualQuality =
                scoreExternalDimension(
                        view, EvaluationDimension.PERCEPTUAL_QUALITY, criteria, explanation);
        dimensionScores.put(EvaluationDimension.PERCEPTUAL_QUALITY, perceptualQuality);

        double sequenceConsistency =
                scoreExternalDimension(
                        view, EvaluationDimension.SEQUENCE_CONSISTENCY, criteria, explanation);
        dimensionScores.put(EvaluationDimension.SEQUENCE_CONSISTENCY, sequenceConsistency);

        double instructionFollowing =
                scoreExternalDimension(
                        view, EvaluationDimension.INSTRUCTION_FOLLOWING, criteria, explanation);
        dimensionScores.put(EvaluationDimension.INSTRUCTION_FOLLOWING, instructionFollowing);

        double domainCoherence =
                scoreExternalDimension(
                        view, EvaluationDimension.DOMAIN_COHERENCE, criteria, explanation);
        dimensionScores.put(EvaluationDimension.DOMAIN_COHERENCE, domainCoherence);

        double qualityScore = computeWeightedScore(dimensionScores, criteria.weights());
        explanation.add(String.format("weighted_total=%.4f", qualityScore));

        // accumulate historical annotation signals into quality Score
        // PREFERENCE +N feedback actually raises quality score,trigger Feedback Loop Closer.should
        // Cascade
        double annotationBonus = aggregateAnnotationBonus(view.viewId(), view.version());
        if (annotationBonus != 0.0) {
            qualityScore = Math.max(0.0, Math.min(1.0, qualityScore + annotationBonus));
            explanation.add(
                    String.format(
                            "annotation_bonus=%+.4f, final=%.4f", annotationBonus, qualityScore));
            log.info(
                    "[feedback] viewId={} v={} annotationBonus={} finalQualityScore={}",
                    view.viewId(),
                    view.version(),
                    String.format("%.4f", annotationBonus),
                    String.format("%.4f", qualityScore));
        }

        EvaluationResult result =
                new EvaluationResult(
                        view.viewId(),
                        qualityScore,
                        dimensionScores,
                        explanation.toString(),
                        System.currentTimeMillis());

        appendToHistory(view.viewId(), result);
        return result;
    }

    @Override
    public ComparisonResult compare(View v1, View v2, EvaluationCriteria criteria) {
        EvaluationResult r1 = evaluate(v1, criteria);
        EvaluationResult r2 = evaluate(v2, criteria);

        StringJoiner diffLines = new StringJoiner("\n");
        diffLines.add(
                String.format(
                        "Comparison: %s (%.4f) vs %s (%.4f)",
                        v1.viewId(), r1.qualityScore(), v2.viewId(), r2.qualityScore()));
        diffLines.add(String.format("Delta: %+.4f", r2.qualityScore() - r1.qualityScore()));

        Map<String, Object> metrics = new LinkedHashMap<>();
        for (EvaluationDimension dim : EvaluationDimension.values()) {
            double s1 = r1.dimensionScores().getOrDefault(dim, 0.0);
            double s2 = r2.dimensionScores().getOrDefault(dim, 0.0);
            diffLines.add(String.format("  %s: %.4f -> %.4f (%+.4f)", dim, s1, s2, s2 - s1));
            metrics.put(dim.name() + "_v1", s1);
            metrics.put(dim.name() + "_v2", s2);
            metrics.put(dim.name() + "_delta", s2 - s1);
        }
        metrics.put("qualityScore_v1", r1.qualityScore());
        metrics.put("qualityScore_v2", r2.qualityScore());
        metrics.put("qualityScore_delta", r2.qualityScore() - r1.qualityScore());

        return new ComparisonResult(
                r1.qualityScore(), r2.qualityScore(), diffLines.toString(), metrics);
    }

    @Override
    public List<EvaluationResult> evaluationHistory(String viewId) {
        try {
            byte[] key = historyKey(viewId);
            Optional<byte[]> raw = dataAccessor.getWithFallback(key);
            if (raw.isEmpty()) {
                return List.of();
            }
            List<EvaluationResult> results =
                    MAPPER.readValue(raw.get(), new TypeReference<List<EvaluationResult>>() {});
            return List.copyOf(results);
        } catch (IOException e) {
            log.warn("Failed to read evaluation history for viewId={}: {}", viewId, e.getMessage());
            return List.of();
        }
    }

    @Override
    public EvaluationCriteria getDefaultCriteria(View.ViewType viewType) {
        return EvaluationCriteria.create(viewType);
    }

    private double scoreCompleteness(
            View view,
            View.ViewType viewType,
            EvaluationCriteria criteria,
            StringJoiner explanation) {
        double score =
                switch (viewType) {
                    case RAW_IDENTITY -> {
                        boolean hasSource = !view.sourceChunkIds().isEmpty();
                        explanation.add(
                                String.format(
                                        "completeness(RAW_IDENTITY)=%.2f(source=%s)",
                                        hasSource ? 1.0 : 0.0, hasSource));
                        yield hasSource ? 1.0 : 0.0;
                    }
                    case EMBEDDING_INDEX -> {
                        boolean hasEmbedding = view.embedding().isPresent();
                        int dim = hasEmbedding ? view.embedding().get().length : 0;
                        boolean hasContent = view.content() != null && view.content().length > 0;
                        double s = 0.0;
                        if (hasEmbedding) s += 0.6;
                        if (hasContent) s += 0.4;
                        explanation.add(
                                String.format(
                                        "completeness(EMBEDDING_INDEX)=%.2f(embedding=%s,dim=%d,content=%s)",
                                        s, hasEmbedding, dim, hasContent));
                        yield s;
                    }
                    case SUMMARY -> {
                        int contentLen = view.content() != null ? view.content().length : 0;
                        int sourceCount = view.sourceChunkIds().size();
                        double s = 0.0;
                        if (contentLen > 0) s += 0.5;
                        if (sourceCount > 0) s += Math.min(0.5, sourceCount * 0.1);
                        explanation.add(
                                String.format(
                                        "completeness(SUMMARY)=%.2f(contentLen=%d,sourceCount=%d)",
                                        s, contentLen, sourceCount));
                        yield s;
                    }
                    case QUERY_RESULT -> {
                        boolean hasContent = view.content() != null && view.content().length > 0;
                        int sourceCount = view.sourceChunkIds().size();
                        double s = 0.0;
                        if (hasContent) s += 0.6;
                        if (sourceCount > 0) s += 0.4;
                        explanation.add(
                                String.format(
                                        "completeness(QUERY_RESULT)=%.2f(content=%s,sourceCount=%d)",
                                        s, hasContent, sourceCount));
                        yield s;
                    }
                    case CLASSIFICATION -> {
                        boolean hasAnnotations = !view.annotations().isEmpty();
                        boolean hasContent = view.content() != null && view.content().length > 0;
                        double s = 0.0;
                        if (hasAnnotations) s += 0.5;
                        if (hasContent) s += 0.5;
                        explanation.add(
                                String.format(
                                        "completeness(CLASSIFICATION)=%.2f(annotations=%s,content=%s)",
                                        s, hasAnnotations, hasContent));
                        yield s;
                    }
                    case RETRIEVAL_TOPK -> {
                        boolean hasEmbedding = view.embedding().isPresent();
                        int sourceCount = view.sourceChunkIds().size();
                        double s = 0.0;
                        if (hasEmbedding) s += 0.6;
                        if (sourceCount > 0) s += 0.4;
                        explanation.add(
                                String.format(
                                        "completeness(RETRIEVAL_TOPK)=%.2f(embedding=%s,sourceCount=%d)",
                                        s, hasEmbedding, sourceCount));
                        yield s;
                    }
                };
        return clamp(score);
    }

    private double scoreRelevance(
            View view,
            View.ViewType viewType,
            EvaluationCriteria criteria,
            StringJoiner explanation) {
        double score =
                switch (viewType) {
                    case RAW_IDENTITY -> {
                        boolean hasSource = !view.sourceChunkIds().isEmpty();
                        explanation.add(
                                String.format(
                                        "relevance(RAW_IDENTITY)=%.2f", hasSource ? 1.0 : 0.0));
                        yield hasSource ? 1.0 : 0.0;
                    }
                    case QUERY_RESULT, CLASSIFICATION -> {
                        double qs = extractQualityScore(view).orElse(0.5);
                        explanation.add(
                                String.format(
                                        "relevance(%s)=%.2f(fromQualityScore=%s)",
                                        viewType, qs, extractQualityScore(view).isPresent()));
                        yield qs;
                    }
                    case SUMMARY -> {
                        double qs = extractQualityScore(view).orElse(0.5);
                        int sourceCount = view.sourceChunkIds().size();
                        double coverageRatio =
                                sourceCount > 0 ? Math.min(1.0, sourceCount / 10.0) : 0.5;
                        double s = (qs + coverageRatio) / 2.0;
                        explanation.add(
                                String.format(
                                        "relevance(SUMMARY)=%.2f(qualityScore=%.2f,coverageRatio=%.2f)",
                                        s, qs, coverageRatio));
                        yield s;
                    }
                    case EMBEDDING_INDEX, RETRIEVAL_TOPK -> {
                        double qs = extractQualityScore(view).orElse(0.5);
                        explanation.add(
                                String.format(
                                        "relevance(%s)=%.2f(fromQualityScore=%s)",
                                        viewType, qs, extractQualityScore(view).isPresent()));
                        yield qs;
                    }
                };
        return clamp(score);
    }

    private double scoreFreshness(
            View view, EvaluationCriteria criteria, StringJoiner explanation) {
        long viewTs = resolveTimestamp(view);
        long now = System.currentTimeMillis();
        double maxAgeDays = getMaxAgeDays(criteria);
        double ageInDays = (now - viewTs) / (1000.0 * 60 * 60 * 24);
        double score = 1.0 - Math.min(1.0, ageInDays / maxAgeDays);
        explanation.add(
                String.format(
                        "freshness=%.2f(age=%.1fd,maxAge=%.1fd)", score, ageInDays, maxAgeDays));
        return clamp(score);
    }

    private double scoreExternalDimension(
            View view,
            EvaluationDimension dimension,
            EvaluationCriteria criteria,
            StringJoiner explanation) {
        Optional<Double> external =
                externalProvider.evaluate(view, dimension.name(), criteria.parameters());
        if (external.isPresent()) {
            double score = clamp(external.get());
            explanation.add(String.format("%s=%.2f(external)", dimension.name(), score));
            return score;
        }
        double defaultScore = 0.5;
        explanation.add(String.format("%s=%.2f(default)", dimension.name(), defaultScore));
        return defaultScore;
    }

    private double computeWeightedScore(
            Map<EvaluationDimension, Double> scores, Map<EvaluationDimension, Double> weights) {
        double total = 0.0;
        double weightSum = 0.0;
        for (Map.Entry<EvaluationDimension, Double> entry : weights.entrySet()) {
            double s = scores.getOrDefault(entry.getKey(), 0.0);
            total += s * entry.getValue();
            weightSum += entry.getValue();
        }
        if (weightSum == 0.0) {
            weightSum = 1.0;
        }
        return total / weightSum;
    }

    private Optional<Double> extractQualityScore(View view) {
        if (view.qualityScore().isPresent()) {
            return view.qualityScore();
        }
        for (FeedbackAnnotation ann : view.annotations()) {
            if (ann.type() == FeedbackAnnotation.AnnotationType.QUALITY_SCORE) {
                Object val = ann.value();
                if (val instanceof Number n) {
                    return Optional.of(n.doubleValue());
                }
            }
        }
        return Optional.empty();
    }

    private long resolveTimestamp(View view) {
        return view.annotations().stream()
                .mapToLong(FeedbackAnnotation::timestamp)
                .max()
                .orElse(System.currentTimeMillis());
    }

    private double getMaxAgeDays(EvaluationCriteria criteria) {
        Object val = criteria.parameters().get("maxAgeDays");
        if (val instanceof Number n) {
            return n.doubleValue();
        }
        return DEFAULT_MAX_AGE_DAYS;
    }

    private double clamp(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }

    private void appendToHistory(String viewId, EvaluationResult result) {
        try {
            byte[] key = historyKey(viewId);
            Object lock = historyLocks.computeIfAbsent(viewId, k -> new Object());
            synchronized (lock) {
                List<EvaluationResult> history;
                Optional<byte[]> raw = knowledgeStore.get(key);
                if (raw.isPresent()) {
                    history =
                            new ArrayList<>(
                                    MAPPER.readValue(
                                            raw.get(),
                                            new TypeReference<List<EvaluationResult>>() {}));
                } else {
                    history = new ArrayList<>();
                }
                history.add(result);

                while (history.size() > MAX_HISTORY_ENTRIES) {
                    history.remove(0);
                }

                byte[] value = MAPPER.writeValueAsBytes(history);
                knowledgeStore.put(key, value);
            }
        } catch (IOException e) {
            log.warn(
                    "Failed to store evaluation history for viewId={}: {}", viewId, e.getMessage());
        }
    }

    private static byte[] historyKey(String viewId) {
        return (HISTORY_KEY_PREFIX + viewId).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * read view's historical annotations,accumulate PREFERENCE value as quality score increment.
     * bonus = tanh(sum(value) / 20) * 0.3 (tanh limits to [-0.3, +0.3]) makes multiple POSITIVE +10
     * cumulative effect diminishes,avoid quality running out of control.
     */
    private double aggregateAnnotationBonus(String viewId, int viewVersion) {
        try {
            // prefer reading versioned key,falls back to unversioned
            byte[] keyV =
                    viewVersion > 0
                            ? ("view_annotations:" + viewId + ":v" + viewVersion)
                                    .getBytes(StandardCharsets.UTF_8)
                            : ("view_annotations:" + viewId).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> raw = knowledgeStore.get(keyV);
            if (raw.isEmpty() && viewVersion > 0) {
                // when current version has no annotation,try key without version number
                raw =
                        knowledgeStore.get(
                                ("view_annotations:" + viewId).getBytes(StandardCharsets.UTF_8));
            }
            if (raw.isEmpty()) return 0.0;

            List<Map<String, Object>> annotations =
                    MAPPER.readValue(raw.get(), new TypeReference<List<Map<String, Object>>>() {});
            double sumValue = 0.0;
            for (Map<String, Object> ann : annotations) {
                String type = String.valueOf(ann.get("type"));
                String valueStr = String.valueOf(ann.get("value"));
                Object confObj = ann.get("confidence");
                double conf = confObj instanceof Number ? ((Number) confObj).doubleValue() : 1.0;

                if ("PREFERENCE".equals(type) || "QUALITY_SCORE".equals(type)) {
                    try {
                        double v = Double.parseDouble(valueStr);
                        sumValue += v * conf;
                    } catch (NumberFormatException ignored) {
                        // free text feedback ignored(FB2 LLM path already handled server-side)
                    }
                }
            }
            // tanh limits to [-0.3, +0.3],avoid single POSITIVE +10 score
            return Math.tanh(sumValue / 20.0) * 0.3;
        } catch (Exception e) {
            log.debug("aggregateAnnotationBonus failed for viewId={}: {}", viewId, e.getMessage());
            return 0.0;
        }
    }
}
