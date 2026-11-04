package io.github.rawwise.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rawwise.axiom.pipeline.evaluation.DefaultEvaluationEngine;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationCriteria;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.axiom.pipeline.evaluation.ExternalEvaluationProvider;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.View;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EvaluationEngineExtendedDimensionE2ETest {

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
    }

    @Test
    void evaluateIncludesAllSevenDimensions() {
        ExternalEvaluationProvider externalProvider = new StubProvider(Map.of());
        EvaluationEngine engine =
                new DefaultEvaluationEngine(metastore, knowledgeStore, externalProvider);

        View view = makeView("v-all-7");
        EvaluationEngine.EvaluationResult result =
                engine.evaluate(view, EvaluationCriteria.createDefault());

        assertNotNull(result);
        assertEquals(7, result.dimensionScores().size());
        assertTrue(
                result.dimensionScores()
                        .containsKey(EvaluationEngine.EvaluationDimension.COMPLETENESS));
        assertTrue(
                result.dimensionScores()
                        .containsKey(EvaluationEngine.EvaluationDimension.RELEVANCE));
        assertTrue(
                result.dimensionScores()
                        .containsKey(EvaluationEngine.EvaluationDimension.FRESHNESS));
        assertTrue(
                result.dimensionScores()
                        .containsKey(EvaluationEngine.EvaluationDimension.PERCEPTUAL_QUALITY));
        assertTrue(
                result.dimensionScores()
                        .containsKey(EvaluationEngine.EvaluationDimension.SEQUENCE_CONSISTENCY));
        assertTrue(
                result.dimensionScores()
                        .containsKey(EvaluationEngine.EvaluationDimension.INSTRUCTION_FOLLOWING));
        assertTrue(
                result.dimensionScores()
                        .containsKey(EvaluationEngine.EvaluationDimension.DOMAIN_COHERENCE));
    }

    @Test
    void externalProviderScoreIsUsedForNewDimensions() {
        AtomicReference<String> capturedDim = new AtomicReference<>();
        ExternalEvaluationProvider externalProvider =
                (view, dim, params) -> {
                    if ("PERCEPTUAL_QUALITY".equals(dim)) {
                        capturedDim.set(dim);
                        return Optional.of(0.85);
                    }
                    return Optional.empty();
                };
        EvaluationEngine engine =
                new DefaultEvaluationEngine(metastore, knowledgeStore, externalProvider);

        View view = makeView("v-perceptual");
        EvaluationEngine.EvaluationResult result =
                engine.evaluate(view, EvaluationCriteria.createDefault());

        assertEquals("PERCEPTUAL_QUALITY", capturedDim.get());
        assertEquals(
                0.85,
                result.dimensionScores()
                        .get(EvaluationEngine.EvaluationDimension.PERCEPTUAL_QUALITY),
                0.001);
    }

    @Test
    void defaultScoreUsedWhenExternalProviderEmpty() {
        ExternalEvaluationProvider externalProvider = new StubProvider(Map.of());
        EvaluationEngine engine =
                new DefaultEvaluationEngine(metastore, knowledgeStore, externalProvider);

        View view = makeView("v-default");
        EvaluationEngine.EvaluationResult result =
                engine.evaluate(view, EvaluationCriteria.createDefault());

        assertEquals(
                0.5,
                result.dimensionScores()
                        .get(EvaluationEngine.EvaluationDimension.PERCEPTUAL_QUALITY),
                0.001);
        assertEquals(
                0.5,
                result.dimensionScores()
                        .get(EvaluationEngine.EvaluationDimension.SEQUENCE_CONSISTENCY),
                0.001);
        assertEquals(
                0.5,
                result.dimensionScores()
                        .get(EvaluationEngine.EvaluationDimension.INSTRUCTION_FOLLOWING),
                0.001);
        assertEquals(
                0.5,
                result.dimensionScores().get(EvaluationEngine.EvaluationDimension.DOMAIN_COHERENCE),
                0.001);
    }

    @Test
    void backwardCompatibleTwoArgConstructor() {
        EvaluationEngine engine = new DefaultEvaluationEngine(metastore, knowledgeStore);

        View view = makeView("v-backward");
        EvaluationEngine.EvaluationResult result =
                engine.evaluate(view, EvaluationCriteria.createDefault());

        assertEquals(7, result.dimensionScores().size());
        assertEquals(
                0.5,
                result.dimensionScores()
                        .get(EvaluationEngine.EvaluationDimension.PERCEPTUAL_QUALITY),
                0.001);
    }

    @Test
    void weightedScoreNormalizedAcrossSevenDimensions() {
        ExternalEvaluationProvider externalProvider = new StubProvider(Map.of());
        EvaluationEngine engine =
                new DefaultEvaluationEngine(metastore, knowledgeStore, externalProvider);

        View view = makeView("v-weighted");
        EvaluationEngine.EvaluationResult result =
                engine.evaluate(view, EvaluationCriteria.createDefault());

        assertTrue(result.qualityScore() >= 0.0);
        assertTrue(result.qualityScore() <= 1.0);
    }

    private View makeView(String viewId) {
        return new View(
                viewId,
                List.of("chunk:" + viewId),
                View.ViewType.EMBEDDING_INDEX,
                1,
                new byte[0],
                Optional.empty(),
                Optional.empty(),
                List.of());
    }

    static class StubProvider implements ExternalEvaluationProvider {
        private final Map<String, Double> scores;

        StubProvider(Map<String, Double> scores) {
            this.scores = scores;
        }

        @Override
        public Optional<Double> evaluate(View view, String dimension, Map<String, Object> params) {
            return Optional.ofNullable(scores.get(dimension));
        }
    }
}
