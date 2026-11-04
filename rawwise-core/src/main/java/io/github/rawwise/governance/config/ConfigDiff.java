package io.github.rawwise.governance.config;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public record ConfigDiff(
        long epoch,
        long baseEpoch,
        Set<String> changedKeys,
        Set<String> hotReloadableKeys,
        Set<String> rejectedKeys,
        Map<String, TransitionPolicy> transitionPolicies,
        boolean appliedDespiteRejection,
        Instant timestamp) {
    public ConfigDiff {
        Objects.requireNonNull(changedKeys, "changedKeys must not be null");
        Objects.requireNonNull(hotReloadableKeys, "hotReloadableKeys must not be null");
        Objects.requireNonNull(rejectedKeys, "rejectedKeys must not be null");
        Objects.requireNonNull(transitionPolicies, "transitionPolicies must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        changedKeys = Set.copyOf(changedKeys);
        hotReloadableKeys = Set.copyOf(hotReloadableKeys);
        rejectedKeys = Set.copyOf(rejectedKeys);
        transitionPolicies = Map.copyOf(transitionPolicies);

        for (String key : hotReloadableKeys) {
            if (!changedKeys.contains(key)) {
                throw new IllegalArgumentException(
                        "hotReloadableKeys must be subset of changedKeys: " + key);
            }
        }
        for (String key : rejectedKeys) {
            if (!changedKeys.contains(key)) {
                throw new IllegalArgumentException(
                        "rejectedKeys must be subset of changedKeys: " + key);
            }
        }
        for (String key : hotReloadableKeys) {
            if (rejectedKeys.contains(key)) {
                throw new IllegalArgumentException(
                        "hotReloadableKeys and rejectedKeys must be disjoint: " + key);
            }
        }
        if (!changedKeys.isEmpty() && !transitionPolicies.keySet().equals(hotReloadableKeys)) {
            throw new IllegalArgumentException(
                    "transitionPolicies must cover exactly hotReloadableKeys");
        }
        if (epoch != -1 && baseEpoch > 0 && epoch <= baseEpoch) {
            throw new IllegalArgumentException(
                    "epoch must be greater than baseEpoch: epoch="
                            + epoch
                            + ", baseEpoch="
                            + baseEpoch);
        }
    }

    public boolean hasChanges() {
        return !changedKeys.isEmpty();
    }

    public boolean hasRejectedChanges() {
        return !rejectedKeys.isEmpty();
    }

    private static final Set<String> SENSITIVE_KEYWORDS =
            Set.of(
                    "password",
                    "secret",
                    "token",
                    "credential",
                    "api-key",
                    "access-key",
                    "secret-key",
                    "private-key");

    public static String redactIfNeeded(String key, Object value) {
        if (ConfigMetadata.isSensitive(key)) {
            return "***";
        }
        String lowerKey = key.toLowerCase();
        for (String keyword : SENSITIVE_KEYWORDS) {
            if (lowerKey.contains(keyword)) {
                return "***";
            }
        }
        return String.valueOf(value);
    }

    public <T> Optional<T> getOldValue(ConfigOption<T> option, RawWiseConfig oldConfig) {
        return changedKeys.contains(option.key())
                ? Optional.ofNullable(oldConfig.get(option))
                : Optional.empty();
    }

    public <T> Optional<T> getNewValue(ConfigOption<T> option, RawWiseConfig newConfig) {
        return changedKeys.contains(option.key())
                ? Optional.ofNullable(newConfig.get(option))
                : Optional.empty();
    }
}
