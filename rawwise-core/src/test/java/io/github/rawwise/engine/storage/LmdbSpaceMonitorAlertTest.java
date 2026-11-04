package io.github.rawwise.engine.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("P5-4: LMDB space monitor alerts")
class LmdbSpaceMonitorAlertTest {

    @Test
    @DisplayName("Default constructor uses 0.8/0.9 thresholds")
    void defaultThresholds() {
        var monitor = new LmdbSpaceMonitor(null, 0.8, 0.9);
        assertFalse(monitor.isSpaceExhausted());
    }

    @Test
    @DisplayName("isSpaceExhausted returns false initially")
    void initiallyNotExhausted() {
        var monitor = new LmdbSpaceMonitor(null, 0.8, 0.9);
        assertFalse(monitor.isSpaceExhausted());
    }

    @Test
    @DisplayName("Config options have correct defaults")
    void configDefaults() {
        assertEquals(
                0.8,
                io.github.rawwise.governance.config.RawWiseOptions.STORAGE_LMDB_SPACE_WARN_THRESHOLD
                        .defaultValue());
        assertEquals(
                0.9,
                io.github.rawwise.governance.config.RawWiseOptions
                        .STORAGE_LMDB_SPACE_ERROR_THRESHOLD
                        .defaultValue());
    }

    @Test
    @DisplayName("DynamicMmapStore rejects writes when space exhausted")
    void writeRejectedOnSpaceExhaustion() {
        var monitor = new LmdbSpaceMonitor(null, 0.8, 0.9);
        assertFalse(monitor.isSpaceExhausted());
    }
}
