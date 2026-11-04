package io.github.rawwise.axiom.pipeline.cognition;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.model.record.StrategyAction;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FuzzyPhase1Test {

    @Test
    @DisplayName("EffectRecord effectivenessMembership replaces boolean effective")
    void effectRecordFuzzyField() {
        var record =
                new StrategyAction.EffectRecord("a1", "v1", Map.of(), Map.of(), 0.5, 0.8, "ENV");
        assertEquals(0.8, record.effectivenessMembership(), 0.001);
        assertTrue(record.effective());
    }

    @Test
    @DisplayName("EffectRecord effective() returns false when membership < 0.5")
    void effectRecordNotEffective() {
        var record =
                new StrategyAction.EffectRecord("a1", "v1", Map.of(), Map.of(), 0.1, 0.3, "ENV");
        assertFalse(record.effective());
    }

    @Test
    @DisplayName("computeConfidence uses continuous weighting instead of boolean")
    void computeConfidenceContinuous() {
        var e1 = new StrategyAction.EffectRecord("a1", "v1", Map.of(), Map.of(), 0.5, 0.9, "HUMAN");
        var e2 = new StrategyAction.EffectRecord("a2", "v1", Map.of(), Map.of(), 0.3, 0.4, "HUMAN");
        double confidence = StrategyAction.computeConfidence(List.of(e1, e2));
        // HUMAN weight = 1.0 each; weighted = 0.9 + 0.4 = 1.3; total = 2.0
        assertEquals(0.65, confidence, 0.01);
    }

    @Test
    @DisplayName(
            "computeConfidence with boolean-true equivalent (membership=1.0) matches old behavior")
    void computeConfidenceBackwardCompat() {
        var e1 = new StrategyAction.EffectRecord("a1", "v1", Map.of(), Map.of(), 0.5, 1.0, "HUMAN");
        var e2 = new StrategyAction.EffectRecord("a2", "v1", Map.of(), Map.of(), 0.3, 1.0, "HUMAN");
        double confidence = StrategyAction.computeConfidence(List.of(e1, e2));
        // Both fully effective -> confidence = 1.0
        assertEquals(1.0, confidence, 0.001);
    }

    @Test
    @DisplayName("computeConfidence with zero membership (fully ineffective) returns 0.0")
    void computeConfidenceZeroMembership() {
        var e1 = new StrategyAction.EffectRecord("a1", "v1", Map.of(), Map.of(), 0.5, 0.0, "HUMAN");
        var e2 = new StrategyAction.EffectRecord("a2", "v1", Map.of(), Map.of(), 0.3, 0.0, "HUMAN");
        double confidence = StrategyAction.computeConfidence(List.of(e1, e2));
        assertEquals(0.0, confidence, 0.001);
    }

    @Test
    @DisplayName("computeConfidence with boundary membership 0.5 returns 0.5")
    void computeConfidenceBoundaryMembership() {
        var e1 = new StrategyAction.EffectRecord("a1", "v1", Map.of(), Map.of(), 0.5, 0.5, "HUMAN");
        var e2 = new StrategyAction.EffectRecord("a2", "v1", Map.of(), Map.of(), 0.3, 0.5, "HUMAN");
        double confidence = StrategyAction.computeConfidence(List.of(e1, e2));
        assertEquals(0.5, confidence, 0.001);
    }
}
