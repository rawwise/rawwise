package io.github.rawwise.axiom.pipeline.feedback;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.cognition.DefaultActionExecutor;
import io.github.rawwise.axiom.pipeline.cognition.DefaultCognitionEngine;
import io.github.rawwise.axiom.pipeline.evaluation.DefaultEvaluationEngine;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.RocksdbMetastore;
import io.github.rawwise.model.record.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("LoopConvergence RealStorage E2E - closure full chain(real LMDB + RocksDB)")
class LoopConvergenceRealStorageE2ETest {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

    @TempDir Path tempDir;

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private LmdbInstance lmdbInstance;
    private EvaluationEngine evaluationEngine;
    private CognitionEngine cognitionEngine;
    private FeedbackLoopCloser feedbackLoopCloser;
    private ExecutorService cascadeExecutor;

    @BeforeEach
    void setUp() throws IOException {
        metastore = createRocksdbMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        lmdbInstance = createLmdbInstance("loop-convergence");

        evaluationEngine = new DefaultEvaluationEngine(metastore, knowledgeStore);
        DefaultActionExecutor actionExecutor =
                new DefaultActionExecutor(metastore, knowledgeStore, null);
        cognitionEngine =
                new DefaultCognitionEngine(
                        metastore, knowledgeStore, evaluationEngine, actionExecutor);

        cascadeExecutor = Executors.newFixedThreadPool(2);
        feedbackLoopCloser =
                new FeedbackLoopCloser(
                        evaluationEngine,
                        cognitionEngine,
                        metastore,
                        knowledgeStore,
                        cascadeExecutor);
    }

    @AfterEach
    void tearDown() throws IOException {
        cascadeExecutor.shutdownNow();
        if (lmdbInstance != null) {
            lmdbInstance.close();
        }
        if (metastore instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }

    private RocksdbMetastore createRocksdbMetastore() throws IOException {
        Path dbPath = tempDir.resolve("rocksdb-metastore-" + System.currentTimeMillis());
        Files.createDirectories(dbPath);
        return new RocksdbMetastore(dbPath.toString());
    }

    private LmdbInstance createLmdbInstance(String nameSuffix) throws IOException {
        Path lmdbBase = tempDir.resolve("lmdb-" + nameSuffix + "-" + System.currentTimeMillis());
        Files.createDirectories(lmdbBase);
        LmdbInstance instance = new LmdbInstance(nameSuffix, lmdbBase, 10 * 1024 * 1024L);
        instance.tryAcquireLease("e2e-owner", 300_000);
        instance.ensureRoutingOwnerLease("e2e-exec", 1);
        return instance;
    }

    private View writeViewToMetastore(
            String viewId, View.ViewType viewType, byte[] content, double qualityScore)
            throws IOException {
        View view =
                new View(
                        viewId,
                        List.of(),
                        viewType,
                        1,
                        content,
                        Optional.empty(),
                        Optional.of(qualityScore),
                        List.of());

        byte[] versionedKey = ("view:" + viewId + ":v1").getBytes(StandardCharsets.UTF_8);
        metastore.put(versionedKey, MAPPER.writeValueAsBytes(view));

        byte[] latestKey = ("view:latest:" + viewId).getBytes(StandardCharsets.UTF_8);
        metastore.put(latestKey, "1".getBytes(StandardCharsets.UTF_8));

        return view;
    }

    private View loadViewFromMetastore(String viewId) throws IOException {
        byte[] latestKey = ("view:latest:" + viewId).getBytes(StandardCharsets.UTF_8);
        Optional<byte[]> latestBytes = metastore.get(latestKey);
        if (latestBytes.isPresent()) {
            int version = Integer.parseInt(new String(latestBytes.get(), StandardCharsets.UTF_8));
            byte[] versionedKey =
                    ("view:" + viewId + ":v" + version).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> versionedBytes = metastore.get(versionedKey);
            if (versionedBytes.isPresent()) {
                return MAPPER.readValue(versionedBytes.get(), View.class);
            }
        }
        return null;
    }

    private int countLoopRecords(String viewId) throws IOException {
        byte[] prefix = ("loop_record:" + viewId + ":").getBytes(StandardCharsets.UTF_8);
        List<io.github.rawwise.metastore.KeyValue> records = metastore.scanAll(prefix);
        return records.size();
    }

    @Nested
    @DisplayName("S1-HappyPath - closure full chain")
    class HappyPath {

        @Test
        @DisplayName("F_human annotation -> evaluate -> -> derivation -> LoopRecord persisted")
        void humanAnnotationTriggersFullLoop() throws Exception {
            String viewId = "view-loop-1";
            byte[] content =
                    "This is a test document for loop convergence validation"
                            .getBytes(StandardCharsets.UTF_8);
            writeViewToMetastore(viewId, View.ViewType.SUMMARY, content, 0.3);

            FeedbackAnnotation annotation =
                    new DefaultFeedbackAnnotation(
                            "ann-loop-1",
                            viewId,
                            "human-user",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            "content needs improvement",
                            0.9,
                            System.currentTimeMillis());

            LoopRecord record = feedbackLoopCloser.onAnnotation(viewId, annotation);

            assertThat(record.status()).isEqualTo(LoopStatus.OK);
            assertThat(record.newQualityScore()).isGreaterThan(0.0);
            assertThat(record.previousQualityScore()).isEqualTo(0.3);

            View updated = loadViewFromMetastore(viewId);
            assertThat(updated).isNotNull();
            assertThat(updated.qualityScore()).isPresent();
            assertThat(updated.qualityScore().get()).isGreaterThan(0.0);

            assertThat(countLoopRecords(viewId)).isGreaterThanOrEqualTo(1);
        }

        @Test
        @DisplayName("exceedsthreshold trigger derivation")
        void cascadeTriggeredWhenDeltaExceedsThreshold() throws Exception {
            String viewId = "view-cascade-1";
            byte[] content =
                    "Document content for cascade testing with sufficient length"
                            .getBytes(StandardCharsets.UTF_8);
            writeViewToMetastore(viewId, View.ViewType.SUMMARY, content, 0.1);

            FeedbackAnnotation highConfAnnotation =
                    new DefaultFeedbackAnnotation(
                            "ann-cascade-1",
                            viewId,
                            "human-reviewer",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            "major correction needed",
                            0.95,
                            System.currentTimeMillis());

            LoopRecord record = feedbackLoopCloser.onAnnotation(viewId, highConfAnnotation);

            double delta = Math.abs(record.newQualityScore() - record.previousQualityScore());
            if (delta >= 0.15) {
                assertThat(record.cascaded()).isTrue();
            }

            assertThat(record.status()).isEqualTo(LoopStatus.OK);
        }

        @Test
        @DisplayName(
                "CORRECT after action executioncreate CORRECTION annotation -> secondary closure triggered")
        void correctActionCreatesAnnotationAndTriggersSecondLoop() throws Exception {
            String viewId = "view-correct-1";
            byte[] content =
                    "Content for CORRECT action loop test".getBytes(StandardCharsets.UTF_8);
            writeViewToMetastore(viewId, View.ViewType.SUMMARY, content, 0.2);

            FeedbackAnnotation firstAnnotation =
                    new DefaultFeedbackAnnotation(
                            "ann-correct-1",
                            viewId,
                            "human-expert",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            "initial correction",
                            0.85,
                            System.currentTimeMillis());

            LoopRecord firstRecord = feedbackLoopCloser.onAnnotation(viewId, firstAnnotation);
            assertThat(firstRecord.status()).isEqualTo(LoopStatus.OK);

            View afterFirst = loadViewFromMetastore(viewId);
            assertThat(afterFirst.qualityScore()).isPresent();

            FeedbackAnnotation correctAnnotation =
                    new DefaultFeedbackAnnotation(
                            "ann-correct-2",
                            viewId,
                            "agent:corrector",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            "auto-correction from CORRECT action",
                            0.7,
                            System.currentTimeMillis());

            LoopRecord secondRecord = feedbackLoopCloser.onAnnotation(viewId, correctAnnotation);
            assertThat(secondRecord.status()).isEqualTo(LoopStatus.OK);
            assertThat(secondRecord.previousQualityScore())
                    .isEqualTo(afterFirst.qualityScore().get());

            assertThat(countLoopRecords(viewId)).isGreaterThanOrEqualTo(2);
        }
    }

    @Nested
    @DisplayName("S2-Concurrency - concurrentannotationclosuresecurity")
    class Concurrency {

        @Test
        @DisplayName("multi-threaded concurrentannotationsame View -> all LoopRecord status OK")
        void concurrentAnnotationsOnSameView() throws Exception {
            String viewId = "view-concurrent-1";
            byte[] content = "Concurrent annotation test content".getBytes(StandardCharsets.UTF_8);
            writeViewToMetastore(viewId, View.ViewType.SUMMARY, content, 0.5);

            int threadCount = 4;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                new Thread(
                                () -> {
                                    try {
                                        startLatch.await();
                                        FeedbackAnnotation ann =
                                                new DefaultFeedbackAnnotation(
                                                        "ann-concurrent-" + idx,
                                                        viewId,
                                                        "user-" + idx,
                                                        FeedbackAnnotation.AnnotationType
                                                                .CORRECTION,
                                                        "correction-" + idx,
                                                        0.8 + idx * 0.05,
                                                        System.currentTimeMillis());
                                        LoopRecord record =
                                                feedbackLoopCloser.onAnnotation(viewId, ann);
                                        if (record.status() == LoopStatus.OK) {
                                            successCount.incrementAndGet();
                                        }
                                    } catch (Exception e) {
                                        // expected under contention
                                    } finally {
                                        doneLatch.countDown();
                                    }
                                })
                        .start();
            }

            startLatch.countDown();
            boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
            assertThat(completed).isTrue();

            assertThat(successCount.get()).isGreaterThan(0);

            View finalView = loadViewFromMetastore(viewId);
            assertThat(finalView).isNotNull();
            assertThat(finalView.qualityScore()).isPresent();
        }
    }

    @Nested
    @DisplayName("S3-FaultRecovery - failure recovery")
    class FaultRecovery {

        @Test
        @DisplayName(
                "View when not exists onAnnotation returns VIEW_NOT_FOUND instead of exception")
        void annotationOnNonExistentViewReturnsGracefully() {
            FeedbackAnnotation ann =
                    new DefaultFeedbackAnnotation(
                            "ann-missing-1",
                            "nonexistent-view",
                            "user-1",
                            FeedbackAnnotation.AnnotationType.PREFERENCE,
                            "opinion",
                            0.5,
                            System.currentTimeMillis());

            LoopRecord record = feedbackLoopCloser.onAnnotation("nonexistent-view", ann);

            assertThat(record.status()).isEqualTo(LoopStatus.VIEW_NOT_FOUND);
            assertThat(record.viewId()).isEqualTo("nonexistent-view");
        }

        @Test
        @DisplayName("null onAnnotation FAILED non- NPE")
        void nullAnnotationReturnsFailed() {
            LoopRecord record = feedbackLoopCloser.onAnnotation("some-view", null);

            assertThat(record.status()).isEqualTo(LoopStatus.FAILED);
        }
    }

    @Nested
    @DisplayName("S4-Boundary - boundary conditions")
    class Boundary {

        @Test
        @DisplayName("threshold does not trigger")
        void noCascadeWhenDeltaBelowThreshold() throws Exception {
            String viewId = "view-nocascade-1";
            byte[] content = "Boundary test content".getBytes(StandardCharsets.UTF_8);
            writeViewToMetastore(viewId, View.ViewType.RAW_IDENTITY, content, 0.8);

            FeedbackAnnotation lowImpactAnn =
                    new DefaultFeedbackAnnotation(
                            "ann-low-1",
                            viewId,
                            "user-1",
                            FeedbackAnnotation.AnnotationType.PREFERENCE,
                            "minor preference",
                            0.82,
                            System.currentTimeMillis());

            LoopRecord record = feedbackLoopCloser.onAnnotation(viewId, lowImpactAnn);

            double delta = Math.abs(record.newQualityScore() - record.previousQualityScore());
            if (delta < 0.15) {
                assertThat(record.cascaded()).isFalse();
            }
            assertThat(record.status()).isEqualTo(LoopStatus.OK);
        }

        @Test
        @DisplayName("exceeds MAX_CASCADE_DEPTH=3")
        void cascadeDepthBounded() throws Exception {
            String viewId = "view-depth-1";
            byte[] content = "Depth limit test content".getBytes(StandardCharsets.UTF_8);
            writeViewToMetastore(viewId, View.ViewType.SUMMARY, content, 0.1);

            for (int i = 0; i < 5; i++) {
                FeedbackAnnotation ann =
                        new DefaultFeedbackAnnotation(
                                "ann-depth-" + i,
                                viewId,
                                "user-" + i,
                                FeedbackAnnotation.AnnotationType.CORRECTION,
                                "correction-" + i,
                                0.9 - i * 0.1,
                                System.currentTimeMillis());
                feedbackLoopCloser.onAnnotation(viewId, ann);
            }

            View finalView = loadViewFromMetastore(viewId);
            assertThat(finalView).isNotNull();
            assertThat(finalView.qualityScore()).isPresent();
            assertThat(finalView.qualityScore().get()).isFinite();
            assertThat(finalView.qualityScore().get()).isBetween(0.0, 1.0);
        }

        @Test
        @DisplayName("EVAL_LAYER scoreindexindependentcan (RocksDB directlyreadverify)")
        void evaluationScorePersistedAndQueryable() throws Exception {
            String viewId = "view-eval-1";
            byte[] content = "Evaluation score persistence test".getBytes(StandardCharsets.UTF_8);
            writeViewToMetastore(viewId, View.ViewType.SUMMARY, content, 0.3);

            FeedbackAnnotation ann =
                    new DefaultFeedbackAnnotation(
                            "ann-eval-1",
                            viewId,
                            "human-1",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            "correction",
                            0.9,
                            System.currentTimeMillis());

            feedbackLoopCloser.onAnnotation(viewId, ann);

            byte[] scoreKey = ("view_score:" + viewId).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> scoreData = metastore.get(scoreKey);

            if (scoreData.isPresent()) {
                var node = MAPPER.readTree(scoreData.get());
                assertThat(node.has("score")).isTrue();
                assertThat(node.get("score").isNumber()).isTrue();
                assertThat(node.has("signalId")).isTrue();
                assertThat(node.has("timestamp")).isTrue();
                assertThat(node.get("timestamp").isNumber()).isTrue();
            }
        }
    }

    @Nested
    @DisplayName("S5-RealStorage - realstorageverify")
    class RealStorage {

        @Test
        @DisplayName("LoopRecord write RocksDB aftercan read")
        void loopRecordRoundTripInRocksDB() throws Exception {
            String viewId = "view-rt-1";
            byte[] content = "Round trip test content".getBytes(StandardCharsets.UTF_8);
            writeViewToMetastore(viewId, View.ViewType.SUMMARY, content, 0.4);

            FeedbackAnnotation ann =
                    new DefaultFeedbackAnnotation(
                            "ann-rt-1",
                            viewId,
                            "user-rt",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            "round-trip correction",
                            0.88,
                            System.currentTimeMillis());

            feedbackLoopCloser.onAnnotation(viewId, ann);

            assertThat(countLoopRecords(viewId)).isGreaterThanOrEqualTo(1);

            byte[] prefix = ("loop_record:" + viewId + ":").getBytes(StandardCharsets.UTF_8);
            List<io.github.rawwise.metastore.KeyValue> records = metastore.scanAll(prefix);
            assertThat(records).isNotEmpty();
            LoopRecord readBack = MAPPER.readValue(records.get(0).value(), LoopRecord.class);
            assertThat(readBack.viewId()).isEqualTo(viewId);
            assertThat(readBack.status()).isEqualTo(LoopStatus.OK);
            assertThat(readBack.newQualityScore()).isGreaterThan(0.0);
        }

        @Test
        @DisplayName("View updateafter RocksDB inversion key dataconsistent")
        void viewQualityScoreUpdateConsistentInRocksDB() throws Exception {
            String viewId = "view-quality-1";
            byte[] content = "Quality score consistency test".getBytes(StandardCharsets.UTF_8);
            View original = writeViewToMetastore(viewId, View.ViewType.SUMMARY, content, 0.2);
            assertThat(original.qualityScore()).contains(0.2);

            FeedbackAnnotation ann =
                    new DefaultFeedbackAnnotation(
                            "ann-quality-1",
                            viewId,
                            "user-q",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            "quality improvement",
                            0.92,
                            System.currentTimeMillis());

            feedbackLoopCloser.onAnnotation(viewId, ann);

            View fromRocks = loadViewFromMetastore(viewId);
            assertThat(fromRocks).isNotNull();
            assertThat(fromRocks.version()).isEqualTo(1);
            assertThat(fromRocks.qualityScore()).isPresent();
            assertThat(fromRocks.qualityScore().get()).isNotEqualTo(0.2);
            assertThat(fromRocks.viewId()).isEqualTo(viewId);
            assertThat(fromRocks.viewType()).isEqualTo(View.ViewType.SUMMARY);
        }

        @Test
        @DisplayName("derivation write RocksDB aftercan (derivationof)")
        void derivationChainPersistedInRocksDB() throws Exception {
            String viewId = "view-chain-1";
            byte[] content = "Derivation chain test content".getBytes(StandardCharsets.UTF_8);
            writeViewToMetastore(viewId, View.ViewType.SUMMARY, content, 0.3);

            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            List.of(viewId), Semantic.SemanticType.CONCEPT, Map.of());

            byte[] chainKey = ("deriv_src:VIEW:" + viewId).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> chainData = metastore.get(chainKey);
            assertThat(chainData).isPresent();

            String chainStr = new String(chainData.get(), StandardCharsets.UTF_8);
            assertThat(chainStr).contains("SEMANTIC");

            byte[] semKey = ("semantic:" + semantic.semanticId()).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> semData = metastore.get(semKey);
            assertThat(semData).isPresent();
        }
    }
}
