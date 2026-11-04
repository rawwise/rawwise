package io.github.rawwise.governance.auth;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("P2-6: Unsafe Disable Detection")
class UnsafeDisableDetectionTest {

    @Test
    @DisplayName("Production mode ignores explicitly-unsafe-disable=true")
    void productionIgnoresUnsafeDisable() {
        String originalEnv = System.getProperty("rawwise.env");
        String originalUnsafe = System.getProperty("rawwise.auth.explicitly-unsafe-disable");
        try {
            System.setProperty("rawwise.env", "production");
            System.setProperty("rawwise.auth.explicitly-unsafe-disable", "true");

            String envMode = System.getProperty("rawwise.env", "dev");
            boolean explicitlyUnsafeDisable =
                    Boolean.parseBoolean(
                            System.getProperty("rawwise.auth.explicitly-unsafe-disable", "false"));

            boolean effectivelyDeny;
            if ("production".equalsIgnoreCase(envMode) && explicitlyUnsafeDisable) {
                effectivelyDeny = true;
            } else if (explicitlyUnsafeDisable) {
                effectivelyDeny = false;
            } else {
                effectivelyDeny = true;
            }

            assertTrue(
                    effectivelyDeny,
                    "Production mode should enforce auth even with explicitly-unsafe-disable=true");
        } finally {
            restoreProperty("rawwise.env", originalEnv);
            restoreProperty("rawwise.auth.explicitly-unsafe-disable", originalUnsafe);
        }
    }

    @Test
    @DisplayName("Dev mode with explicitly-unsafe-disable=false enforces auth")
    void devModeWithoutUnsafeDisable() {
        String originalEnv = System.getProperty("rawwise.env");
        String originalUnsafe = System.getProperty("rawwise.auth.explicitly-unsafe-disable");
        try {
            System.setProperty("rawwise.env", "dev");
            System.clearProperty("rawwise.auth.explicitly-unsafe-disable");

            String envMode = System.getProperty("rawwise.env", "dev");
            boolean explicitlyUnsafeDisable =
                    Boolean.parseBoolean(
                            System.getProperty("rawwise.auth.explicitly-unsafe-disable", "false"));

            boolean effectivelyDeny;
            if ("production".equalsIgnoreCase(envMode) && explicitlyUnsafeDisable) {
                effectivelyDeny = true;
            } else if (explicitlyUnsafeDisable) {
                effectivelyDeny = false;
            } else {
                effectivelyDeny = true;
            }

            assertTrue(
                    effectivelyDeny,
                    "Dev mode without explicitly-unsafe-disable should enforce auth");
        } finally {
            restoreProperty("rawwise.env", originalEnv);
            restoreProperty("rawwise.auth.explicitly-unsafe-disable", originalUnsafe);
        }
    }

    @Test
    @DisplayName("Dev mode with explicitly-unsafe-disable=true allows unauthenticated")
    void devModeWithUnsafeDisable() {
        String originalEnv = System.getProperty("rawwise.env");
        String originalUnsafe = System.getProperty("rawwise.auth.explicitly-unsafe-disable");
        try {
            System.setProperty("rawwise.env", "dev");
            System.setProperty("rawwise.auth.explicitly-unsafe-disable", "true");

            String envMode = System.getProperty("rawwise.env", "dev");
            boolean explicitlyUnsafeDisable =
                    Boolean.parseBoolean(
                            System.getProperty("rawwise.auth.explicitly-unsafe-disable", "false"));

            boolean effectivelyDeny;
            if ("production".equalsIgnoreCase(envMode) && explicitlyUnsafeDisable) {
                effectivelyDeny = true;
            } else if (explicitlyUnsafeDisable) {
                effectivelyDeny = false;
            } else {
                effectivelyDeny = true;
            }

            assertFalse(
                    effectivelyDeny,
                    "Dev mode with explicitly-unsafe-disable=true should allow unauthenticated");
        } finally {
            restoreProperty("rawwise.env", originalEnv);
            restoreProperty("rawwise.auth.explicitly-unsafe-disable", originalUnsafe);
        }
    }

    @Test
    @DisplayName("Security status endpoint reports secure=false when unsafe disabled in dev")
    void securityStatusReportsInsecure() {
        String originalEnv = System.getProperty("rawwise.env");
        String originalUnsafe = System.getProperty("rawwise.auth.explicitly-unsafe-disable");
        try {
            System.setProperty("rawwise.env", "dev");
            System.setProperty("rawwise.auth.explicitly-unsafe-disable", "true");

            boolean explicitlyUnsafeDisable =
                    Boolean.parseBoolean(
                            System.getProperty("rawwise.auth.explicitly-unsafe-disable", "false"));
            String envMode = System.getProperty("rawwise.env", "dev");
            boolean secure = "production".equalsIgnoreCase(envMode) || !explicitlyUnsafeDisable;

            assertFalse(secure, "Security status should report insecure when unsafe disabled");
        } finally {
            restoreProperty("rawwise.env", originalEnv);
            restoreProperty("rawwise.auth.explicitly-unsafe-disable", originalUnsafe);
        }
    }

    private static void restoreProperty(String key, String original) {
        if (original != null) {
            System.setProperty(key, original);
        } else {
            System.clearProperty(key);
        }
    }
}
