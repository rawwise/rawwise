package io.github.rawwise.axiom.pipeline.feedback;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.derivation.Layer;
import io.github.rawwise.axiom.pipeline.ingestion.DefaultImplicitViewGenerator;
import io.github.rawwise.axiom.pipeline.ingestion.DefaultIngestionEngine;
import io.github.rawwise.axiom.pipeline.ingestion.IngestionEngine;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.DefaultFeedbackAnnotation;
import io.github.rawwise.model.record.FeedbackAnnotation;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("FeedbackPath E2E - path full-chain business scenario test")
class FeedbackPathE2ETest {

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private DefaultImplicitViewGenerator viewGenerator;
    private IngestionEngine engine;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        viewGenerator = new DefaultImplicitViewGenerator(metastore, knowledgeStore);
        engine = new DefaultIngestionEngine(null, null, viewGenerator);
    }

    @Nested
    @DisplayName("S1-PathB - F_human -> View.annotations full chain")
    class PathB {

        @Test
        @DisplayName("singleitemannotationafter writing getViewAnnotations")
        void singleAnnotationWritesAndReads() {
            String viewId = "test-view-1";
            FeedbackAnnotation ann =
                    new DefaultFeedbackAnnotation(
                            "ann-1",
                            viewId,
                            "human-user",
                            FeedbackAnnotation.AnnotationType.PREFERENCE,
                            "preferred",
                            0.95,
                            System.currentTimeMillis());

            boolean result = engine.annotateView(viewId, ann);
            assertThat(result).isTrue();

            List<FeedbackAnnotation> annotations = engine.getViewAnnotations(viewId);
            assertThat(annotations).hasSize(1);
            assertThat(annotations.get(0).annotationId()).isEqualTo("ann-1");
            assertThat(annotations.get(0).type())
                    .isEqualTo(FeedbackAnnotation.AnnotationType.PREFERENCE);
            assertThat(annotations.get(0).confidence()).isEqualTo(0.95);
        }

        @Test
        @DisplayName("multiitemannotation after canquery")
        void multipleAnnotationsAppendedInOrder() {
            String viewId = "test-view-2";
            long now = System.currentTimeMillis();

            engine.annotateView(
                    viewId,
                    new DefaultFeedbackAnnotation(
                            "ann-a",
                            viewId,
                            "user-a",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            "corrected-value",
                            0.9,
                            now));
            engine.annotateView(
                    viewId,
                    new DefaultFeedbackAnnotation(
                            "ann-b",
                            viewId,
                            "user-b",
                            FeedbackAnnotation.AnnotationType.QUALITY_SCORE,
                            0.88,
                            1.0,
                            now + 1000));
            engine.annotateView(
                    viewId,
                    new DefaultFeedbackAnnotation(
                            "ann-c",
                            viewId,
                            "user-c",
                            FeedbackAnnotation.AnnotationType.DECISION_ADOPTED,
                            true,
                            0.8,
                            now + 2000));

            List<FeedbackAnnotation> annotations = engine.getViewAnnotations(viewId);
            assertThat(annotations).hasSize(3);
            assertThat(annotations.stream().map(FeedbackAnnotation::annotationId))
                    .containsExactly("ann-a", "ann-b", "ann-c");
        }

        @Test
        @DisplayName("not yetannotationof view query annotations returns empty list")
        void unannotatedViewReturnsEmptyList() {
            List<FeedbackAnnotation> annotations = engine.getViewAnnotations("never-annotated");
            assertThat(annotations).isEmpty();
        }

        @Test
        @DisplayName("DECISION_ADOPTED typeannotation value")
        void decisionAdoptedAnnotationWithBoolean() {
            String viewId = "test-view-decision";
            FeedbackAnnotation ann =
                    new DefaultFeedbackAnnotation(
                            "ann-decision",
                            viewId,
                            "system",
                            FeedbackAnnotation.AnnotationType.DECISION_ADOPTED,
                            true,
                            1.0,
                            System.currentTimeMillis());

            engine.annotateView(viewId, ann);
            List<FeedbackAnnotation> annotations = engine.getViewAnnotations(viewId);

            assertThat(annotations).hasSize(1);
            assertThat(annotations.get(0).value()).isEqualTo(true);
        }

        @Test
        @DisplayName("different views annotations do not interfere")
        void differentViewsAnnotationsAreIndependent() {
            String viewA = "view-a";
            String viewB = "view-b";

            engine.annotateView(
                    viewA,
                    new DefaultFeedbackAnnotation(
                            "ann-a1",
                            viewA,
                            "user",
                            FeedbackAnnotation.AnnotationType.PREFERENCE,
                            "val-a",
                            0.9,
                            System.currentTimeMillis()));
            engine.annotateView(
                    viewB,
                    new DefaultFeedbackAnnotation(
                            "ann-b1",
                            viewB,
                            "user",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            "val-b",
                            0.8,
                            System.currentTimeMillis()));

            assertThat(engine.getViewAnnotations(viewA)).hasSize(1);
            assertThat(engine.getViewAnnotations(viewB)).hasSize(1);
            assertThat(engine.getViewAnnotations(viewA).get(0).viewId()).isEqualTo(viewA);
            assertThat(engine.getViewAnnotations(viewB).get(0).viewId()).isEqualTo(viewB);
        }
    }

    @Nested
    @DisplayName("S2-FeedbackSignal - data")
    class FeedbackSignalModel {

        @Test
        @DisplayName("FeedbackSignal allfieldcorrect")
        void feedbackSignalAllFields() {
            long ts = System.currentTimeMillis();
            Map<String, Object> payload = Map.of("metric", "accuracy", "dataset", "test");
            FeedbackSignal signal =
                    createSignal(
                            "fb-001",
                            FeedbackSignal.FeedbackSource.ENV,
                            FeedbackSignal.FeedbackPath.FACT_LAYER,
                            FeedbackSignal.SignalType.QUALITY_SCORE,
                            "target-entity",
                            Layer.VIEW,
                            0.92,
                            payload,
                            ts);

            assertThat(signal.feedbackId()).isEqualTo("fb-001");
            assertThat(signal.source()).isEqualTo(FeedbackSignal.FeedbackSource.ENV);
            assertThat(signal.path()).isEqualTo(FeedbackSignal.FeedbackPath.FACT_LAYER);
            assertThat(signal.signalType()).isEqualTo(FeedbackSignal.SignalType.QUALITY_SCORE);
            assertThat(signal.targetEntityId()).isEqualTo("target-entity");
            assertThat(signal.targetLayer()).isEqualTo(Layer.VIEW);
            assertThat(signal.score()).isEqualTo(0.92);
            assertThat(signal.payload()).containsEntry("metric", "accuracy");
            assertThat(signal.timestamp()).isEqualTo(ts);
        }

        @Test
        @DisplayName("HUMAN source + EVAL_LAYER path correct")
        void humanSourceEvalLayer() {
            FeedbackSignal signal =
                    createSignal(
                            "fb-human",
                            FeedbackSignal.FeedbackSource.HUMAN,
                            FeedbackSignal.FeedbackPath.EVAL_LAYER,
                            FeedbackSignal.SignalType.CORRECTION,
                            "view-1",
                            Layer.VIEW,
                            0.0,
                            Map.of("comment", "needs revision"),
                            System.currentTimeMillis());

            assertThat(signal.source()).isEqualTo(FeedbackSignal.FeedbackSource.HUMAN);
            assertThat(signal.path()).isEqualTo(FeedbackSignal.FeedbackPath.EVAL_LAYER);
            assertThat(signal.signalType()).isEqualTo(FeedbackSignal.SignalType.CORRECTION);
        }

        @Test
        @DisplayName("SELF source F_self")
        void selfSourceForInternalFeedback() {
            FeedbackSignal signal =
                    createSignal(
                            "fb-self",
                            FeedbackSignal.FeedbackSource.SELF,
                            FeedbackSignal.FeedbackPath.EVAL_LAYER,
                            FeedbackSignal.SignalType.QUALITY_SCORE,
                            "view-eval",
                            Layer.VIEW,
                            0.85,
                            Map.of("dimension", "COMPLETENESS"),
                            System.currentTimeMillis());

            assertThat(signal.source()).isEqualTo(FeedbackSignal.FeedbackSource.SELF);
        }

        @Test
        @DisplayName("REWARD type can value")
        void rewardSignalWithNegativeScore() {
            FeedbackSignal signal =
                    createSignal(
                            "fb-neg",
                            FeedbackSignal.FeedbackSource.ENV,
                            FeedbackSignal.FeedbackPath.FACT_LAYER,
                            FeedbackSignal.SignalType.REWARD,
                            "target",
                            Layer.RAW,
                            -0.5,
                            Map.of(),
                            System.currentTimeMillis());

            assertThat(signal.score()).isEqualTo(-0.5);
        }
    }

    @Nested
    @DisplayName("S3-FeedbackLoop - path closure")
    class FeedbackLoop {

        @Test
        @DisplayName("same view multiple timesannotation , column")
        void annotationsAccumulateInOrder() {
            String viewId = "loop-view";
            long base = System.currentTimeMillis();

            for (int i = 0; i < 5; i++) {
                engine.annotateView(
                        viewId,
                        new DefaultFeedbackAnnotation(
                                "ann-" + i,
                                viewId,
                                "reviewer",
                                FeedbackAnnotation.AnnotationType.QUALITY_SCORE,
                                0.5 + i * 0.1,
                                0.9,
                                base + i * 100));
            }

            List<FeedbackAnnotation> annotations = engine.getViewAnnotations(viewId);
            assertThat(annotations).hasSize(5);
            for (int i = 0; i < 5; i++) {
                assertThat(annotations.get(i).annotationId()).isEqualTo("ann-" + i);
            }
        }

        @Test
        @DisplayName("multi annotationtype")
        void mixedAnnotationTypes() {
            String viewId = "mixed-view";
            long now = System.currentTimeMillis();

            engine.annotateView(
                    viewId,
                    new DefaultFeedbackAnnotation(
                            "a1",
                            viewId,
                            "u1",
                            FeedbackAnnotation.AnnotationType.PREFERENCE,
                            "good",
                            1.0,
                            now));
            engine.annotateView(
                    viewId,
                    new DefaultFeedbackAnnotation(
                            "a2",
                            viewId,
                            "u2",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            "fix: typo",
                            0.95,
                            now + 1));
            engine.annotateView(
                    viewId,
                    new DefaultFeedbackAnnotation(
                            "a3",
                            viewId,
                            "u3",
                            FeedbackAnnotation.AnnotationType.QUALITY_SCORE,
                            0.88,
                            0.9,
                            now + 2));
            engine.annotateView(
                    viewId,
                    new DefaultFeedbackAnnotation(
                            "a4",
                            viewId,
                            "u4",
                            FeedbackAnnotation.AnnotationType.DECISION_ADOPTED,
                            true,
                            1.0,
                            now + 3));

            List<FeedbackAnnotation> annotations = engine.getViewAnnotations(viewId);
            assertThat(annotations).hasSize(4);
            assertThat(annotations.stream().map(FeedbackAnnotation::type))
                    .containsExactly(
                            FeedbackAnnotation.AnnotationType.PREFERENCE,
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            FeedbackAnnotation.AnnotationType.QUALITY_SCORE,
                            FeedbackAnnotation.AnnotationType.DECISION_ADOPTED);
        }
    }

    @Nested
    @DisplayName("S4-Concurrent - concurrent")
    class Concurrent {

        @Test
        @DisplayName("multi-threaded concurrentannotationsame view, allannotation persisted")
        void concurrentPathBAnnotations() throws Exception {
            String viewId = "concurrent-view";
            int threadCount = 4;
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger errorCount = new AtomicInteger(0);
            var executor = Executors.newFixedThreadPool(threadCount);

            for (int t = 0; t < threadCount; t++) {
                int threadId = t;
                executor.submit(
                        () -> {
                            try {
                                FeedbackAnnotation ann =
                                        new DefaultFeedbackAnnotation(
                                                "ann-concurrent-" + threadId,
                                                viewId,
                                                "user-" + threadId,
                                                FeedbackAnnotation.AnnotationType.PREFERENCE,
                                                "value-" + threadId,
                                                0.9,
                                                System.currentTimeMillis());
                                engine.annotateView(viewId, ann);
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                            } finally {
                                latch.countDown();
                            }
                        });
            }

            assertThat(latch.await(15, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            assertThat(errorCount.get()).isEqualTo(0);
            List<FeedbackAnnotation> annotations = engine.getViewAnnotations(viewId);
            assertThat(annotations).hasSize(threadCount);
        }
    }

    private static FeedbackSignal createSignal(
            String feedbackId,
            FeedbackSignal.FeedbackSource source,
            FeedbackSignal.FeedbackPath path,
            FeedbackSignal.SignalType signalType,
            String targetEntityId,
            Layer targetLayer,
            double score,
            Map<String, Object> payload,
            long timestamp) {
        return new DefaultFeedbackSignal(
                feedbackId,
                source,
                path,
                signalType,
                targetEntityId,
                targetLayer,
                score,
                payload,
                timestamp);
    }
}
