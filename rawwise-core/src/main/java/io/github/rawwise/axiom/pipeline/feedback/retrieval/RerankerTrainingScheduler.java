package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.rawwise.axiom.pipeline.cognition.MembershipFunctions;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RerankerTrainingScheduler {

    public static final String KEY_LAST_TRAIN_AT_MS = "reranker:last_train_at_ms";
    public static final String KEY_META_PREFIX = "reranker:meta:";
    public static final String KEY_WEIGHTS_PREFIX = "reranker:weights:";
    public static final String KEY_LATEST_VERSION = "reranker:latest_version";

    static final double POSITIVE_BOOST = 0.15;
    static final double NEGATIVE_PENALTY = -0.20;
    static final double CORRECTION_BOOST = 0.25;

    private static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new JavaTimeModule());

    private final RetrievalFeedbackCollector collector;
    private final RerankerTrainingConfig config;
    private final Metastore metastore;
    private final Runnable cacheInvalidator;

    private KnowledgeStore knowledgeStore;
    private ScheduledExecutorService scheduler;
    private final AtomicInteger versionCounter = new AtomicInteger(0);
    private final AtomicReference<RerankerModelMetadata> latestModel = new AtomicReference<>();

    public RerankerTrainingScheduler(
            RetrievalFeedbackCollector collector,
            RerankerTrainingConfig config,
            Metastore metastore,
            Runnable cacheInvalidator) {
        this.collector = collector;
        this.config = config;
        this.metastore = metastore;
        this.cacheInvalidator = cacheInvalidator == null ? () -> {} : cacheInvalidator;
    }

    public void setKnowledgeStore(KnowledgeStore knowledgeStore) {
        this.knowledgeStore = knowledgeStore;
    }

    public synchronized void start() {
        if (scheduler != null) return;
        if (!config.enabled()) {
            log.info("RerankerTrainingScheduler: disabled by config, not starting");
            return;
        }
        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "reranker-training-scheduler");
                            t.setDaemon(true);
                            return t;
                        });
        long pollIntervalSec =
                Long.parseLong(
                        System.getProperty("rawwise.reranker.training.poll-interval-sec", "60"));
        scheduler.scheduleAtFixedRate(
                this::safeTick, pollIntervalSec, pollIntervalSec, TimeUnit.SECONDS);
        log.info(
                "RerankerTrainingScheduler started: minSamples={} minInterval={} pollInterval={}s",
                config.minSamples(),
                config.minInterval(),
                pollIntervalSec);
    }

    public synchronized void stop() {
        if (scheduler == null) return;
        scheduler.shutdownNow();
        scheduler = null;
    }

    public RerankerTrainingTrigger triggerOnDemand() {
        return tick();
    }

    public Optional<RerankerModelMetadata> latestModel() {
        return Optional.ofNullable(latestModel.get());
    }

    public Optional<RerankerModelMetadata> loadModelMetadata(String version) {
        try {
            byte[] key = (KEY_META_PREFIX + version).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> raw = metastore.get(key);
            if (raw.isEmpty()) return Optional.empty();
            RerankerModelMetadata meta = MAPPER.readValue(raw.get(), RerankerModelMetadata.class);
            if (knowledgeStore != null) {
                byte[] weightsKey = (KEY_WEIGHTS_PREFIX + version).getBytes(StandardCharsets.UTF_8);
                Optional<byte[]> weightsRaw = knowledgeStore.get(weightsKey);
                if (weightsRaw.isPresent()) {
                    Map<String, Double> weights =
                            MAPPER.readValue(
                                    weightsRaw.get(), new TypeReference<Map<String, Double>>() {});
                    return Optional.of(
                            new RerankerModelMetadata(
                                    meta.version(),
                                    meta.modelPath(),
                                    meta.trainedAt(),
                                    meta.trainingSampleCount(),
                                    meta.validationMetrics(),
                                    weights));
                }
            }
            return Optional.of(meta);
        } catch (IOException e) {
            log.warn("loadModelMetadata failed for version={}: {}", version, e.getMessage());
            return Optional.empty();
        }
    }

    private void safeTick() {
        try {
            tick();
        } catch (Exception e) {
            log.warn("RerankerTrainingScheduler.tick failed: {}", e.getMessage());
        }
    }

    public synchronized RerankerTrainingTrigger tick() {
        long now = System.currentTimeMillis();
        long lastTrain = readLastTrainAt();
        long count = collector.count();

        if (now - lastTrain < config.minInterval().toMillis()) {
            log.debug(
                    "RerankerTrainingScheduler.tick skip: interval not elapsed (now={} last={} delta={}ms)",
                    now,
                    lastTrain,
                    now - lastTrain);
            return new RerankerTrainingTrigger(count, lastTrain, config.currentModelVersion());
        }
        if (sampleReadinessMembership(count) < 0.5) {
            log.debug(
                    "RerankerTrainingScheduler.tick skip: sample readiness low (count={} min={} membership={:.3})",
                    count,
                    config.minSamples(),
                    sampleReadinessMembership(count));
            return new RerankerTrainingTrigger(count, lastTrain, config.currentModelVersion());
        }

        String newVersion = "v" + versionCounter.incrementAndGet();
        List<FeedbackRecord> samples = collector.scanRecent((int) Math.min(count, 10_000), null);
        RerankerModelMetadata meta = trainModel(newVersion, samples, now);
        writeLastTrainAt(now);
        writeMetadata(meta);
        writeLatestVersion(newVersion);
        latestModel.set(meta);
        cacheInvalidator.run();
        log.info(
                "Reranker training completed: version={} samples={} weights={} metrics={}",
                newVersion,
                samples.size(),
                meta.weights().size(),
                meta.validationMetrics());
        return new RerankerTrainingTrigger(count, now, newVersion);
    }

    RerankerModelMetadata trainModel(
            String version, List<FeedbackRecord> samples, long trainedAtMs) {
        Map<String, Double> weights = new HashMap<>();
        int positiveCount = 0;
        int negativeCount = 0;
        int correctionCount = 0;
        int hitsWithExpectedInRetrieved = 0;

        for (FeedbackRecord fb : samples) {
            if (fb == null || fb.feedbackType() == null) continue;
            String expected = fb.userExpectedHitId();
            List<String> retrieved =
                    fb.retrievedHitIds() == null ? List.of() : fb.retrievedHitIds();

            boolean expectedInRetrieved = expected != null && retrieved.contains(expected);
            if (expectedInRetrieved) hitsWithExpectedInRetrieved++;

            switch (fb.feedbackType()) {
                case POSITIVE -> {
                    positiveCount++;
                    if (expected != null) {
                        weights.merge(expected, POSITIVE_BOOST, Double::sum);
                    }
                }
                case NEGATIVE -> {
                    negativeCount++;
                    for (String hitId : retrieved) {
                        if (hitId == null) continue;
                        if (!hitId.equals(expected)) {
                            weights.merge(hitId, NEGATIVE_PENALTY, Double::sum);
                        }
                    }
                    if (expected != null) {
                        weights.merge(expected, POSITIVE_BOOST, Double::sum);
                    }
                }
                case CORRECTION -> {
                    correctionCount++;
                    for (String hitId : retrieved) {
                        if (hitId == null) continue;
                        if (!hitId.equals(expected)) {
                            weights.merge(hitId, NEGATIVE_PENALTY, Double::sum);
                        }
                    }
                    if (expected != null) {
                        weights.merge(expected, CORRECTION_BOOST, Double::sum);
                    }
                }
            }
        }

        double precisionAtK =
                samples.isEmpty() ? 0.0 : (double) hitsWithExpectedInRetrieved / samples.size();
        double positiveRate = samples.isEmpty() ? 0.0 : (double) positiveCount / samples.size();

        Map<String, Double> metrics = new LinkedHashMap<>();
        metrics.put("precision_at_k", round4(precisionAtK));
        metrics.put("positive_rate", round4(positiveRate));
        metrics.put("negative_count", (double) negativeCount);
        metrics.put("correction_count", (double) correctionCount);

        Map<String, Double> frozenWeights = freezeWeights(weights);

        return new RerankerModelMetadata(
                version,
                config.modelOutputPath() + "/" + version,
                Instant.ofEpochMilli(trainedAtMs),
                samples.size(),
                metrics,
                frozenWeights);
    }

    private static Map<String, Double> freezeWeights(Map<String, Double> raw) {
        Map<String, Double> sorted = new LinkedHashMap<>();
        raw.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .forEach(e -> sorted.put(e.getKey(), round4(e.getValue())));
        return Collections.unmodifiableMap(sorted);
    }

    private static double round4(double v) {
        return Math.round(v * 10000.0) / 10000.0;
    }

    private long readLastTrainAt() {
        try {
            Optional<byte[]> raw =
                    metastore.get(KEY_LAST_TRAIN_AT_MS.getBytes(StandardCharsets.UTF_8));
            if (raw.isEmpty()) return 0L;
            return Long.parseLong(new String(raw.get(), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            log.debug("readLastTrainAt failed: {}", e.getMessage());
            return 0L;
        } catch (IOException e) {
            log.debug("readLastTrainAt I/O error: {}", e.getMessage());
            return 0L;
        }
    }

    private void writeLastTrainAt(long now) {
        try {
            metastore.put(
                    KEY_LAST_TRAIN_AT_MS.getBytes(StandardCharsets.UTF_8),
                    String.valueOf(now).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.warn("writeLastTrainAt failed: {}", e.getMessage());
        }
    }

    private void writeMetadata(RerankerModelMetadata meta) {
        try {
            if (knowledgeStore != null) {
                byte[] weightsKey =
                        (KEY_WEIGHTS_PREFIX + meta.version()).getBytes(StandardCharsets.UTF_8);
                knowledgeStore.put(weightsKey, MAPPER.writeValueAsBytes(meta.weights()));
                RerankerModelMetadata metaWithoutWeights =
                        new RerankerModelMetadata(
                                meta.version(),
                                meta.modelPath(),
                                meta.trainedAt(),
                                meta.trainingSampleCount(),
                                meta.validationMetrics(),
                                Map.of());
                byte[] key = (KEY_META_PREFIX + meta.version()).getBytes(StandardCharsets.UTF_8);
                metastore.put(key, MAPPER.writeValueAsBytes(metaWithoutWeights));
            } else {
                byte[] key = (KEY_META_PREFIX + meta.version()).getBytes(StandardCharsets.UTF_8);
                metastore.put(key, MAPPER.writeValueAsBytes(meta));
            }
        } catch (IOException e) {
            log.warn("writeMetadata failed for version={}: {}", meta.version(), e.getMessage());
        }
    }

    private void writeLatestVersion(String version) {
        try {
            metastore.put(
                    KEY_LATEST_VERSION.getBytes(StandardCharsets.UTF_8),
                    version.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.warn("writeLatestVersion failed: {}", e.getMessage());
        }
    }

    /**
     * Fuzzy sample readiness membership: how ready the scheduler is to train with current sample
     * count. Uses sigmoid centered at min Samples with steepness 0.5 (gentle transition for
     * discrete counts).
     *
     * <p>μ(count) = 1 / (1 + e^(-1.0*(count - min Samples))) At count = min Samples, μ = 0.5
     * (transition zone).
     */
    public double sampleReadinessMembership(long count) {
        return MembershipFunctions.sigmoid((double) count, config.minSamples(), 0.5);
    }
}
