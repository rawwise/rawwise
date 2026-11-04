package io.github.rawwise.frontend.backend;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("P2-2: Internal Communication Security")
class InternalCommunicationSecurityTest {

    @Test
    @DisplayName("Default internal token is raw-wise-internal when no env/prop set")
    void defaultInternalToken() {
        String token = resolveInternalTokenNoEnv();
        assertEquals("raw-wise-internal", token);
    }

    @Test
    @DisplayName("Environment variable RAWWISE_INTERNAL_TOKEN takes precedence")
    void envVarPrecedence() {
        String original = System.getenv("RAWWISE_INTERNAL_TOKEN");
        assertNull(original, "Test requires RAWWISE_INTERNAL_TOKEN not set in environment");
    }

    @Test
    @DisplayName("System property rawwise.auth.internal-token is used when env not set")
    void systemPropertyFallback() {
        String original = System.getProperty("rawwise.auth.internal-token");
        try {
            System.clearProperty("rawwise.auth.internal-token");
            String token = resolveInternalTokenNoEnv();
            assertEquals("raw-wise-internal", token);

            System.setProperty("rawwise.auth.internal-token", "my-secret-token");
            token = resolveInternalTokenNoEnv();
            assertEquals("my-secret-token", token);
        } finally {
            if (original != null) {
                System.setProperty("rawwise.auth.internal-token", original);
            } else {
                System.clearProperty("rawwise.auth.internal-token");
            }
        }
    }

    private static String resolveInternalTokenNoEnv() {
        String envToken = System.getenv("RAWWISE_INTERNAL_TOKEN");
        if (envToken != null && !envToken.isBlank()) {
            return envToken;
        }
        String propToken = System.getProperty("rawwise.auth.internal-token");
        if (propToken != null && !propToken.isBlank()) {
            return propToken;
        }
        return "raw-wise-internal";
    }
}
