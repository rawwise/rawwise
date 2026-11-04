package io.github.rawwise.axiom.pipeline.cognition;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MembershipFunctionsTest {

    @Test
    @DisplayName("Sigmoid at center returns 0.5")
    void sigmoidAtCenter() {
        double mu = MembershipFunctions.sigmoid(2.0, 2.0, 2.0);
        assertEquals(0.5, mu, 0.001);
    }

    @Test
    @DisplayName("Sigmoid above center approaches 1.0")
    void sigmoidAboveCenter() {
        double mu = MembershipFunctions.sigmoid(4.0, 2.0, 2.0);
        assertTrue(mu > 0.95, "Expected mu > 0.95, got " + mu);
    }

    @Test
    @DisplayName("Sigmoid below center approaches 0.0")
    void sigmoidBelowCenter() {
        double mu = MembershipFunctions.sigmoid(0.0, 2.0, 2.0);
        assertTrue(mu < 0.05, "Expected mu < 0.05, got " + mu);
    }

    @Test
    @DisplayName("Sigmoid is monotonic increasing")
    void sigmoidMonotonic() {
        double prev = 0;
        for (double x = -5; x <= 5; x += 0.5) {
            double mu = MembershipFunctions.sigmoid(x, 0, 2.0);
            assertTrue(mu >= prev - 1e-12, "Not monotonic at x=" + x);
            prev = mu;
        }
    }

    @Test
    @DisplayName("Triangular at peak returns 1.0")
    void triangularAtPeak() {
        double mu = MembershipFunctions.triangular(5.0, 0, 5, 10);
        assertEquals(1.0, mu, 0.001);
    }

    @Test
    @DisplayName("Triangular at base returns 0.0")
    void triangularAtBase() {
        assertEquals(0.0, MembershipFunctions.triangular(0, 0, 5, 10), 0.001);
        assertEquals(0.0, MembershipFunctions.triangular(10, 0, 5, 10), 0.001);
    }

    @Test
    @DisplayName("Triangular outside base returns 0.0")
    void triangularOutside() {
        assertEquals(0.0, MembershipFunctions.triangular(-1, 0, 5, 10), 0.001);
        assertEquals(0.0, MembershipFunctions.triangular(11, 0, 5, 10), 0.001);
    }
}
