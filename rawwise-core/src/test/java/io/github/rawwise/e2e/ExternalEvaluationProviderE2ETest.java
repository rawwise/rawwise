package io.github.rawwise.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rawwise.axiom.pipeline.evaluation.DefaultExternalEvaluationProvider;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.axiom.pipeline.evaluation.ExternalEvaluationProvider;
import io.github.rawwise.model.record.View;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ExternalEvaluationProviderE2ETest {

    @Test
    void defaultProviderReturnsEmpty() {
        ExternalEvaluationProvider provider = new DefaultExternalEvaluationProvider();
        Optional<Double> result = provider.evaluate(null, "nonexistent_dimension", Map.of());
        assertTrue(result.isEmpty());
    }

    @Test
    void defaultProviderReturnsEmptyForNullView() {
        ExternalEvaluationProvider provider = new DefaultExternalEvaluationProvider();
        Optional<Double> result =
                provider.evaluate(
                        null,
                        EvaluationEngine.EvaluationDimension.PERCEPTUAL_QUALITY.name(),
                        Map.of());
        assertTrue(result.isEmpty());
    }

    @Test
    void defaultProviderHasProviderId() {
        ExternalEvaluationProvider provider = new DefaultExternalEvaluationProvider();
        assertEquals("default-noop", provider.providerId());
    }

    @Test
    void customProviderReturnsScore() {
        ExternalEvaluationProvider provider = new StubEvaluationProvider();
        Optional<Double> result =
                provider.evaluate(null, "PERCEPTUAL_QUALITY", Map.of("clip_score", "0.85"));
        assertTrue(result.isPresent());
        assertEquals(0.85, result.get(), 0.001);
    }

    @Test
    void customProviderReturnsEmptyForUnknownDimension() {
        ExternalEvaluationProvider provider = new StubEvaluationProvider();
        Optional<Double> result =
                provider.evaluate(null, "UNKNOWN_DIMENSION", Map.of("clip_score", "0.85"));
        assertTrue(result.isEmpty());
    }

    @Test
    void allSevenDimensionsExist() {
        EvaluationEngine.EvaluationDimension[] dimensions =
                EvaluationEngine.EvaluationDimension.values();
        assertEquals(7, dimensions.length);

        boolean hasPerceptual = false;
        boolean hasSequence = false;
        boolean hasInstruction = false;
        boolean hasDomain = false;
        for (EvaluationEngine.EvaluationDimension d : dimensions) {
            if (d == EvaluationEngine.EvaluationDimension.PERCEPTUAL_QUALITY) hasPerceptual = true;
            if (d == EvaluationEngine.EvaluationDimension.SEQUENCE_CONSISTENCY) hasSequence = true;
            if (d == EvaluationEngine.EvaluationDimension.INSTRUCTION_FOLLOWING)
                hasInstruction = true;
            if (d == EvaluationEngine.EvaluationDimension.DOMAIN_COHERENCE) hasDomain = true;
        }
        assertTrue(hasPerceptual);
        assertTrue(hasSequence);
        assertTrue(hasInstruction);
        assertTrue(hasDomain);
    }

    static class StubEvaluationProvider implements ExternalEvaluationProvider {
        @Override
        public Optional<Double> evaluate(View view, String dimension, Map<String, Object> params) {
            if ("PERCEPTUAL_QUALITY".equals(dimension) && params.containsKey("clip_score")) {
                return Optional.of(Double.parseDouble(params.get("clip_score").toString()));
            }
            return Optional.empty();
        }

        @Override
        public String providerId() {
            return "stub";
        }
    }
}
