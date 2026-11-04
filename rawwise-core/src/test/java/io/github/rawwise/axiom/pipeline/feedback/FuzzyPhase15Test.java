package io.github.rawwise.axiom.pipeline.feedback;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FuzzyPhase15Test {

    private static final double THRESHOLD =
            Double.parseDouble(System.getProperty("rawwise.feedback.cascade-threshold", "0.05"));

    private final FeedbackLoopCloser closer = new FeedbackLoopCloser(null, null, null, null);

    @Test
    @DisplayName("cascadeMembership at threshold returns 0.5")
    void cascadeMembershipAtThreshold() {
        double mu = closer.cascadeMembership(THRESHOLD);
        assertEquals(0.5, mu, 0.001);
    }

    @Test
    @DisplayName("cascadeMembership well above threshold approaches 1.0")
    void cascadeMembershipAboveThreshold() {
        double mu = closer.cascadeMembership(0.5);
        assertTrue(mu > 0.95, "Expected mu > 0.95, got " + mu);
    }

    @Test
    @DisplayName("cascadeMembership well below threshold is very low")
    void cascadeMembershipBelowThreshold() {
        // delta=0.0 is 0.05 away from threshold=0.05; with k=100, μ≈0.007
        double mu = closer.cascadeMembership(0.0);
        assertTrue(mu < 0.05, "Expected mu < 0.05 at delta=0 with k=100, got " + mu);
    }

    @Test
    @DisplayName("shouldCascade backward compatible: at threshold returns true (μ>=0.5)")
    void shouldCascadeBackwardCompat() {
        assertTrue(closer.shouldCascade(THRESHOLD));
        assertTrue(closer.shouldCascade(THRESHOLD + 0.01));
        assertFalse(closer.shouldCascade(THRESHOLD - 0.02));
    }
}
