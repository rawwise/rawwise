package io.github.rawwise.governance.config;

import java.io.Serializable;

public final class ConfigPropagationMessages {
    private ConfigPropagationMessages() {}

    public record ConfigPrepareResponse(
            String executorId, long configEpoch, boolean accepted, String rejectReason)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public ConfigPrepareResponse {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
            if (configEpoch < 0) {
                throw new IllegalArgumentException("configEpoch must be non-negative");
            }
            if (!accepted && (rejectReason == null || rejectReason.isBlank())) {
                throw new IllegalArgumentException(
                        "rejectReason must not be blank when accepted=false");
            }
            if (accepted && rejectReason != null) {
                throw new IllegalArgumentException("rejectReason must be null when accepted=true");
            }
        }

        public static ConfigPrepareResponse ok(String executorId, long configEpoch) {
            return new ConfigPrepareResponse(executorId, configEpoch, true, null);
        }

        public static ConfigPrepareResponse reject(
                String executorId, long configEpoch, String rejectReason) {
            return new ConfigPrepareResponse(executorId, configEpoch, false, rejectReason);
        }
    }

    public record ConfigEpochHeartbeat(String executorId, long configEpoch)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public ConfigEpochHeartbeat {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
            if (configEpoch < 0) {
                throw new IllegalArgumentException("configEpoch must be non-negative");
            }
        }
    }
}
