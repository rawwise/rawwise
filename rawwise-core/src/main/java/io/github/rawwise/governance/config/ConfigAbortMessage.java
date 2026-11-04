package io.github.rawwise.governance.config;

import java.io.Serializable;

public record ConfigAbortMessage(long configEpoch) implements Serializable {
    private static final long serialVersionUID = 1L;

    public ConfigAbortMessage {
        if (configEpoch < 0) {
            throw new IllegalArgumentException("configEpoch must be non-negative");
        }
    }
}
