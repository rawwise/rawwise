package io.github.rawwise.governance.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ConfigChangeListener {

    Logger LOG = LoggerFactory.getLogger(ConfigChangeListener.class);

    default boolean preConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
        return true;
    }

    void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig);

    default void onError(ConfigDiff diff, Exception e) {
        LOG.error("Config change error for epoch {}: {}", diff.epoch(), e.getMessage(), e);
    }
}
