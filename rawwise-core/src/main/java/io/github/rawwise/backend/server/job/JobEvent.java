package io.github.rawwise.backend.server.job;

import java.util.Map;

public record JobEvent(
        String jobId, JobEventType type, long timestamp, Map<String, String> attributes) {
    public enum JobEventType {
        SUBMITTED,
        STARTED,
        SUCCEEDED,
        FAILED,
        CANCELLED,
        TIMED_OUT,
        ADMISSION_GRANTED,
        ADMISSION_REJECTED
    }

    public static JobEvent of(String jobId, JobEventType type) {
        return new JobEvent(jobId, type, System.currentTimeMillis(), Map.of());
    }

    public static JobEvent of(String jobId, JobEventType type, Map<String, String> attributes) {
        return new JobEvent(jobId, type, System.currentTimeMillis(), Map.copyOf(attributes));
    }
}
