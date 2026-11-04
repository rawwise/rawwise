package io.github.rawwise.axiom.pipeline.evaluation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.FeedbackAnnotation;
import io.github.rawwise.model.record.View;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("EvaluationEngine E2E - evaluate full-chain business scenario test")
class EvaluationEngineE2ETest {

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private DefaultEvaluationEngine engine;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        engine = new DefaultEvaluationEngine(metastore, knowledgeStore);
    }

    private static FeedbackAnnotation qualityAnnotation(String viewId, double score) {
        return new FeedbackAnnotation() {
            @Override
            public String annotationId() {
                return "ann-" + System.nanoTime();
            }

            @Override
            public String viewId() {
                return viewId;
            }

            @Override
            public String annotatorId() {
                return "system";
            }

            @Override
            public AnnotationType type() {
                return AnnotationType.QUALITY_SCORE;
            }

            @Override
            public Object value() {
                return score;
            }

            @Override
            public double confidence() {
                return 1.0;
            }

            @Override
            public long timestamp() {
                return System.currentTimeMillis();
            }
        };
    }

    @Nested
    @DisplayName("S1-HappyPath - viewevaluateandcomparison lifecycle")
    class HappyPath {

        @Test
        @DisplayName("evaluate RAW_IDENTITY view(has data)")
        void evaluateRawIdentityWithSource() {
            View view =
                    new View(
                            "v-raw-1",
                            List.of("chunk-1"),
                            View.ViewType.RAW_IDENTITY,
                            0,
                            "raw-content".getBytes(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of());

            EvaluationCriteria criteria = EvaluationCriteria.createDefault();
            EvaluationEngine.EvaluationResult result = engine.evaluate(view, criteria);

            assertThat(result.viewId()).isEqualTo("v-raw-1");
            assertThat(result.qualityScore()).isGreaterThan(0.0);
            assertThat(result.dimensionScores())
                    .containsKeys(
                            EvaluationEngine.EvaluationDimension.COMPLETENESS,
                            EvaluationEngine.EvaluationDimension.RELEVANCE,
                            EvaluationEngine.EvaluationDimension.FRESHNESS);
            assertThat(result.explanation()).isNotEmpty();
            assertThat(result.timestamp()).isGreaterThan(0);
        }

        @Test
        @DisplayName("evaluate RAW_IDENTITY view(no data,)")
        void evaluateRawIdentityWithoutSource() {
            View view =
                    new View(
                            "v-raw-empty",
                            List.of(),
                            View.ViewType.RAW_IDENTITY,
                            0,
                            "empty-content".getBytes(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of());

            EvaluationCriteria criteria = EvaluationCriteria.createDefault();
            EvaluationEngine.EvaluationResult result = engine.evaluate(view, criteria);

            assertThat(
                            result.dimensionScores()
                                    .get(EvaluationEngine.EvaluationDimension.COMPLETENESS))
                    .isEqualTo(0.0);
            assertThat(result.dimensionScores().get(EvaluationEngine.EvaluationDimension.RELEVANCE))
                    .isEqualTo(0.0);
        }

        @Test
        @DisplayName("evaluate EMBEDDING_INDEX view(hasvectorand)")
        void evaluateEmbeddingIndex() {
            float[] embedding = new float[256];
            for (int i = 0; i < 256; i++) embedding[i] = (float) i / 256.0f;
            View view =
                    new View(
                            "v-emb-1",
                            List.of("chunk-1"),
                            View.ViewType.EMBEDDING_INDEX,
                            0,
                            "emb-content".getBytes(),
                            Optional.of(embedding),
                            Optional.of(0.8),
                            List.of());

            EvaluationCriteria criteria = EvaluationCriteria.create(View.ViewType.EMBEDDING_INDEX);
            EvaluationEngine.EvaluationResult result = engine.evaluate(view, criteria);

            assertThat(
                            result.dimensionScores()
                                    .get(EvaluationEngine.EvaluationDimension.COMPLETENESS))
                    .isGreaterThan(0.5);
        }

        @Test
        @DisplayName("evaluate SUMMARY view")
        void evaluateSummary() {
            View view =
                    new View(
                            "v-summary-1",
                            List.of("chunk-1", "chunk-2", "chunk-3", "chunk-4", "chunk-5"),
                            View.ViewType.SUMMARY,
                            0,
                            "summary content text".getBytes(),
                            Optional.empty(),
                            Optional.of(0.7),
                            List.of());

            EvaluationCriteria criteria = EvaluationCriteria.create(View.ViewType.SUMMARY);
            EvaluationEngine.EvaluationResult result = engine.evaluate(view, criteria);

            assertThat(result.qualityScore()).isBetween(0.0, 1.0);
        }

        @Test
        @DisplayName("evaluate QUERY_RESULT view")
        void evaluateQueryResult() {
            View view =
                    new View(
                            "v-query-1",
                            List.of("src-1", "src-2"),
                            View.ViewType.QUERY_RESULT,
                            0,
                            "{\"rows\":10}".getBytes(),
                            Optional.empty(),
                            Optional.of(0.9),
                            List.of());

            EvaluationCriteria criteria = EvaluationCriteria.create(View.ViewType.QUERY_RESULT);
            EvaluationEngine.EvaluationResult result = engine.evaluate(view, criteria);

            assertThat(result.qualityScore()).isBetween(0.0, 1.0);
        }

        @Test
        @DisplayName("comparisontwoviewof")
        void compareTwoViews() {
            View v1 =
                    new View(
                            "v-compare-1",
                            List.of("src-1"),
                            View.ViewType.RAW_IDENTITY,
                            0,
                            "content-1".getBytes(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of());
            View v2 =
                    new View(
                            "v-compare-2",
                            List.of("src-1", "src-2"),
                            View.ViewType.SUMMARY,
                            0,
                            "content-2-longer".getBytes(),
                            Optional.empty(),
                            Optional.of(0.85),
                            List.of());

            EvaluationCriteria criteria = EvaluationCriteria.createDefault();
            EvaluationEngine.ComparisonResult result = engine.compare(v1, v2, criteria);

            assertThat(result.diffReport()).isNotEmpty();
            assertThat(result.metrics())
                    .containsKeys("qualityScore_v1", "qualityScore_v2", "qualityScore_delta");
        }

        @Test
        @DisplayName("compare will writeevaluatehistory")
        void compareWritesHistory() {
            View v1 =
                    new View(
                            "v-hist-1",
                            List.of(),
                            View.ViewType.RAW_IDENTITY,
                            0,
                            "data".getBytes(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of());
            View v2 =
                    new View(
                            "v-hist-2",
                            List.of("src"),
                            View.ViewType.RAW_IDENTITY,
                            0,
                            "data".getBytes(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of());

            engine.compare(v1, v2, EvaluationCriteria.createDefault());

            List<EvaluationEngine.EvaluationResult> hist1 = engine.evaluationHistory("v-hist-1");
            List<EvaluationEngine.EvaluationResult> hist2 = engine.evaluationHistory("v-hist-2");

            assertThat(hist1).isNotEmpty();
            assertThat(hist2).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("S2-Criteria - evaluation criteria for different view types")
    class CriteriaByViewType {

        @Test
        @DisplayName("RAW_IDENTITY default criteria weightis (0.5, 0.2, 0.3)")
        void rawIdentityCriteriaWeights() {
            EvaluationCriteria criteria = engine.getDefaultCriteria(View.ViewType.RAW_IDENTITY);

            assertThat(criteria.weights().get(EvaluationEngine.EvaluationDimension.COMPLETENESS))
                    .isEqualTo(0.5);
            assertThat(criteria.weights().get(EvaluationEngine.EvaluationDimension.RELEVANCE))
                    .isEqualTo(0.2);
            assertThat(criteria.weights().get(EvaluationEngine.EvaluationDimension.FRESHNESS))
                    .isEqualTo(0.3);
        }

        @Test
        @DisplayName("EMBEDDING_INDEX criteria package minDimension")
        void embeddingIndexCriteriaParams() {
            EvaluationCriteria criteria = engine.getDefaultCriteria(View.ViewType.EMBEDDING_INDEX);

            assertThat(criteria.parameters()).containsKey("minDimension");
            assertThat(criteria.parameters().get("minDimension")).isEqualTo(128);
        }

        @Test
        @DisplayName("SUMMARY criteria in RELEVANCE weight (0.40)")
        void summaryCriteriaRelevanceWeight() {
            EvaluationCriteria criteria = engine.getDefaultCriteria(View.ViewType.SUMMARY);

            assertThat(criteria.weights().get(EvaluationEngine.EvaluationDimension.RELEVANCE))
                    .isEqualTo(0.40);
        }

        @Test
        @DisplayName("RETRIEVAL_TOPK criteria in RELEVANCE weightis 0.45")
        void retrievalTopKRelevanceWeight() {
            EvaluationCriteria criteria = engine.getDefaultCriteria(View.ViewType.RETRIEVAL_TOPK);

            assertThat(criteria.weights().get(EvaluationEngine.EvaluationDimension.RELEVANCE))
                    .isEqualTo(0.45);
        }
    }

    @Nested
    @DisplayName("S3-History - evaluatehistorypersistedand")
    class History {

        @Test
        @DisplayName("evaluateafterhistorypackage")
        void historyContainsEvaluationResult() {
            View view =
                    new View(
                            "v-history-1",
                            List.of("src"),
                            View.ViewType.RAW_IDENTITY,
                            0,
                            "data".getBytes(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of());

            engine.evaluate(view, EvaluationCriteria.createDefault());
            engine.evaluate(view, EvaluationCriteria.createDefault());
            engine.evaluate(view, EvaluationCriteria.createDefault());

            List<EvaluationEngine.EvaluationResult> history =
                    engine.evaluationHistory("v-history-1");

            assertThat(history).hasSize(3);
            assertThat(history.get(0).viewId()).isEqualTo("v-history-1");
        }

        @Test
        @DisplayName("nohistoryof viewId returns empty list")
        void noHistoryReturnsEmpty() {
            List<EvaluationEngine.EvaluationResult> history =
                    engine.evaluationHistory("never-evaluated");

            assertThat(history).isEmpty();
        }

        @Test
        @DisplayName("history immutablecolumntable")
        void historyReturnsImmutableList() {
            View view =
                    new View(
                            "v-immutable-history",
                            List.of(),
                            View.ViewType.RAW_IDENTITY,
                            0,
                            "data".getBytes(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of());

            engine.evaluate(view, EvaluationCriteria.createDefault());

            List<EvaluationEngine.EvaluationResult> history =
                    engine.evaluationHistory("v-immutable-history");
            assertThat(history).isNotEmpty();

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> history.add(null))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    @DisplayName("S4-Concurrent - concurrentevaluatehistory")
    class Concurrent {

        @Test
        @DisplayName(
                "multi-threaded concurrentevaluatesameview, historyentry (MAX_HISTORY_ENTRIES constraint)")
        void concurrentEvaluationHistoryComplete() throws Exception {
            int threadCount = 4;
            int evalsPerThread = 20;
            String viewId = "v-concurrent";
            View view =
                    new View(
                            viewId,
                            List.of("src"),
                            View.ViewType.RAW_IDENTITY,
                            0,
                            "data".getBytes(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of());
            EvaluationCriteria criteria = EvaluationCriteria.createDefault();

            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger errorCount = new AtomicInteger(0);
            var executor = Executors.newFixedThreadPool(threadCount);

            for (int t = 0; t < threadCount; t++) {
                executor.submit(
                        () -> {
                            try {
                                for (int i = 0; i < evalsPerThread; i++) {
                                    engine.evaluate(view, criteria);
                                }
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
            List<EvaluationEngine.EvaluationResult> history = engine.evaluationHistory(viewId);
            assertThat(history).hasSize(threadCount * evalsPerThread);
        }
    }

    @Nested
    @DisplayName("S5-Feedback - F_self")
    class FeedbackLoop {

        @Test
        @DisplayName("qualityScore ofview relevance score")
        void qualityScoreAffectsRelevance() {
            View lowScore =
                    new View(
                            "v-fb-low",
                            List.of("src"),
                            View.ViewType.QUERY_RESULT,
                            0,
                            "data".getBytes(),
                            Optional.empty(),
                            Optional.of(0.1),
                            List.of());
            View highScore =
                    new View(
                            "v-fb-high",
                            List.of("src"),
                            View.ViewType.QUERY_RESULT,
                            0,
                            "data".getBytes(),
                            Optional.empty(),
                            Optional.of(0.95),
                            List.of());

            EvaluationCriteria criteria = EvaluationCriteria.createDefault();
            EvaluationEngine.EvaluationResult rLow = engine.evaluate(lowScore, criteria);
            EvaluationEngine.EvaluationResult rHigh = engine.evaluate(highScore, criteria);

            double relevanceLow =
                    rLow.dimensionScores().get(EvaluationEngine.EvaluationDimension.RELEVANCE);
            double relevanceHigh =
                    rHigh.dimensionScores().get(EvaluationEngine.EvaluationDimension.RELEVANCE);

            assertThat(relevanceHigh).isGreaterThan(relevanceLow);
        }

        @Test
        @DisplayName("FeedbackAnnotation ofviewvia annotation fallback relevance score")
        void annotationQualityScoreAffectsRelevance() {
            View view =
                    new View(
                            "v-ann-great",
                            List.of("src"),
                            View.ViewType.QUERY_RESULT,
                            0,
                            "data".getBytes(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of(qualityAnnotation("v-ann-great", 0.99)));

            EvaluationCriteria criteria = EvaluationCriteria.createDefault();
            EvaluationEngine.EvaluationResult result = engine.evaluate(view, criteria);

            double relevance =
                    result.dimensionScores().get(EvaluationEngine.EvaluationDimension.RELEVANCE);

            assertThat(relevance).isGreaterThan(0.5);
        }
    }
}
