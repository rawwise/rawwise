package io.github.rawwise.governance.config;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ConfigMetadata {
    private static final Set<String> HOT_RELOADABLE_KEYS = ConcurrentHashMap.newKeySet();
    private static final Set<String> SENSITIVE_KEYS = ConcurrentHashMap.newKeySet();
    private static final ConcurrentHashMap<String, TransitionPolicy> TRANSITION_POLICIES =
            new ConcurrentHashMap<>();
    private static final Set<String> REGISTERED_KEYS = ConcurrentHashMap.newKeySet();
    private static volatile boolean registered = false;

    static void register(
            String key, boolean hotReloadable, boolean sensitive, TransitionPolicy policy) {
        if (registered) {
            throw new IllegalStateException(
                    "ConfigMetadata is already marked as registered; cannot register new key: "
                            + key);
        }
        REGISTERED_KEYS.add(key);
        if (hotReloadable) HOT_RELOADABLE_KEYS.add(key);
        if (sensitive) SENSITIVE_KEYS.add(key);
        if (policy != TransitionPolicy.IMMEDIATE || hotReloadable) {
            TRANSITION_POLICIES.put(key, policy);
        }
    }

    static void markRegistered() {
        registered = true;
    }

    static void ensureRegistered() {
        if (!registered) {
            try {
                Class.forName("io.github.rawwise.governance.config.RawWiseOptions");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("RawWiseOptions not found", e);
            }
        }
    }

    public static boolean isHotReloadable(String key) {
        ensureRegistered();
        return HOT_RELOADABLE_KEYS.contains(key);
    }

    public static boolean isSensitive(String key) {
        ensureRegistered();
        return SENSITIVE_KEYS.contains(key);
    }

    public static TransitionPolicy getTransitionPolicy(String key) {
        ensureRegistered();
        return TRANSITION_POLICIES.getOrDefault(key, TransitionPolicy.IMMEDIATE);
    }

    public static Set<String> allHotReloadableKeys() {
        ensureRegistered();
        return Set.copyOf(HOT_RELOADABLE_KEYS);
    }

    public static Set<String> allRegisteredKeys() {
        ensureRegistered();
        return Set.copyOf(REGISTERED_KEYS);
    }

    static void reset() {
        REGISTERED_KEYS.clear();
        HOT_RELOADABLE_KEYS.clear();
        SENSITIVE_KEYS.clear();
        TRANSITION_POLICIES.clear();
        registered = false;
    }

    private ConfigMetadata() {}
}
