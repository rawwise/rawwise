package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.axiom.pipeline.cognition.MembershipFunctions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FuzzyPhase2Test {

    private static final int MIN_SAMPLES = 50;
    private static final double K = 1.0;

    @Test
    @DisplayName("sampleReadinessMembership at minSamples returns 0.5")
    void readinessAtMinSamples() {
        double mu = MembershipFunctions.sigmoid(MIN_SAMPLES, MIN_SAMPLES, K);
        assertEquals(0.5, mu, 0.001);
    }

    @Test
    @DisplayName("sampleReadinessMembership well above minSamples approaches 1.0")
    void readinessAboveMinSamples() {
        double mu = MembershipFunctions.sigmoid(MIN_SAMPLES + 20, MIN_SAMPLES, K);
        assertTrue(mu > 0.95, "Expected mu > 0.95, got " + mu);
    }

    @Test
    @DisplayName("sampleReadinessMembership well below minSamples approaches 0.0")
    void readinessBelowMinSamples() {
        double mu = MembershipFunctions.sigmoid(0, MIN_SAMPLES, K);
        assertTrue(mu < 0.05, "Expected mu < 0.05, got " + mu);
    }

    @Test
    @DisplayName("sampleReadinessMembership with k=1.0 is not too wide (count=min-10 gives mu<0.3)")
    void readinessNotTooWide() {
        double mu = MembershipFunctions.sigmoid(MIN_SAMPLES - 10, MIN_SAMPLES, K);
        assertTrue(mu < 0.3, "Expected mu < 0.3 at count=min-10 with k=1.0, got " + mu);
    }
}
