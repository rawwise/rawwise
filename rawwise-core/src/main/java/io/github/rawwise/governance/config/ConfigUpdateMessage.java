package io.github.rawwise.governance.config;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public record ConfigUpdateMessage(
        long configEpoch,
        Map<String, String> configValues,
        Set<String> changedKeys,
        String validationChecksum,
        PropagationMode propagationMode)
        implements Serializable {
    private static final long serialVersionUID = 2L;

    public ConfigUpdateMessage {
        configValues = Map.copyOf(configValues);
        changedKeys = Set.copyOf(changedKeys);
        Objects.requireNonNull(validationChecksum, "validationChecksum must not be null");
        Objects.requireNonNull(propagationMode, "propagationMode must not be null");
        if (configEpoch < 0) {
            throw new IllegalArgumentException("configEpoch must be non-negative");
        }
    }
}
