package io.github.rawwise.governance.telemetry;

import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

@Slf4j
public class TelemetryConfig {

    public static String currentTraceId() {
        String traceId = MDC.get("traceId");
        return traceId != null ? traceId : "";
    }

    public static String newTraceId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    public static void setTraceId(String traceId) {
        if (traceId != null && !traceId.isBlank()) {
            MDC.put("traceId", traceId);
        }
    }

    public static void clearTraceId() {
        MDC.remove("traceId");
    }

    private TelemetryConfig() {}
}
