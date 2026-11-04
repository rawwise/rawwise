package io.github.rawwise.axiom.pipeline.cognition;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EffectFeedbackFuzzyTest {

    @Test
    @DisplayName("reliable() returns true when reliabilityMembership >= 0.5")
    void reliableTrueAtThreshold() {
        var fb = EffectFeedback.of(0.8, Map.of(), 1.0, 2.0, 0.5, "test");
        assertTrue(fb.reliable());
    }

    @Test
    @DisplayName("reliable() returns false when reliabilityMembership < 0.5")
    void reliableFalseBelowThreshold() {
        var fb = EffectFeedback.of(0.3, Map.of(), 1.0, 1.5, 0.49, "test");
        assertFalse(fb.reliable());
    }

    @Test
    @DisplayName("unreliable factory sets membership to 0.0")
    void unreliableFactory() {
        var fb = EffectFeedback.unreliable(0.1, Map.of(), 1.0, 0.5);
        assertEquals(0.0, fb.reliabilityMembership(), 0.001);
        assertFalse(fb.reliable());
    }

    @Test
    @DisplayName("reliable factory sets membership to 1.0")
    void reliableFactory() {
        var fb = EffectFeedback.reliable(0.9, Map.of(), 0.5, 3.0);
        assertEquals(1.0, fb.reliabilityMembership(), 0.001);
        assertTrue(fb.reliable());
    }

    @Test
    @DisplayName("SNR boundary: DefaultEffectEvaluator produces membership near 0.5 at threshold")
    void evaluatorBoundaryMembership() {
        var evaluator = new DefaultEffectEvaluator();
        var result = new ExecutionResult("d1", true, Map.of("m1", 2.0), null, 100);
        var feedback = evaluator.evaluate(result, EvaluationCriteria.defaultCriteria());
        double mu = feedback.reliabilityMembership();
        assertTrue(
                mu > 0.4 && mu < 0.6, "Expected membership near 0.5 at SNR threshold, got " + mu);
    }
}
