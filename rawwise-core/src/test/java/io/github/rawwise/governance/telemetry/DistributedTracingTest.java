package io.github.rawwise.governance.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class DistributedTracingTest {

    @AfterEach
    void cleanup() {
        TelemetryConfig.clearTraceId();
    }

    @Test
    void newTraceIdIs16Chars() {
        String traceId = TelemetryConfig.newTraceId();
        assertNotNull(traceId);
        assertEquals(16, traceId.length());
    }

    @Test
    void setAndGetTraceId() {
        TelemetryConfig.setTraceId("abc123");
        assertEquals("abc123", TelemetryConfig.currentTraceId());
    }

    @Test
    void currentTraceIdEmptyWhenNotSet() {
        TelemetryConfig.clearTraceId();
        assertEquals("", TelemetryConfig.currentTraceId());
    }

    @Test
    void setTraceIdInMdc() {
        TelemetryConfig.setTraceId("test-trace-123");
        assertEquals("test-trace-123", MDC.get("traceId"));
    }

    @Test
    void clearTraceIdRemovesFromMdc() {
        TelemetryConfig.setTraceId("test-trace-456");
        TelemetryConfig.clearTraceId();
        assertNull(MDC.get("traceId"));
    }

    @Test
    void setTraceIdIgnoresBlank() {
        TelemetryConfig.setTraceId("  ");
        assertEquals("", TelemetryConfig.currentTraceId());
    }
}
