package io.github.rawwise.governance.config;

import java.io.Serializable;
import java.util.Objects;

record ConfigSnapshot(long epoch, RawWiseConfig config, String fileHash) implements Serializable {
    private static final long serialVersionUID = 1L;

    ConfigSnapshot {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(fileHash, "fileHash must not be null");
    }
}
