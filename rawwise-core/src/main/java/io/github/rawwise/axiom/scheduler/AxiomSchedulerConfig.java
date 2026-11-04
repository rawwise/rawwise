package io.github.rawwise.axiom.scheduler;

import java.util.Arrays;
import java.util.List;

public record AxiomSchedulerConfig(
        boolean enabled,
        boolean warmupOnStart,
        int parallelism,
        long shutdownTimeoutSec,
        List<String> agents,
        boolean decayEnabled,
        long decayIntervalMs,
        boolean consolidateEnabled,
        long consolidateIntervalMs) {

    public static final String P_ENABLED = "rawwise.axiom.scheduler.enabled";
    public static final String P_WARMUP = "rawwise.axiom.scheduler.warmup-on-start";
    public static final String P_PARALLELISM = "rawwise.axiom.scheduler.parallelism";
    public static final String P_SHUTDOWN_TIMEOUT_SEC =
            "rawwise.axiom.scheduler.shutdown-timeout-sec";
    public static final String P_AGENTS = "rawwise.axiom.scheduler.agents";
    public static final String P_DECAY_ENABLED = "rawwise.axiom.scheduler.decay.enabled";
    public static final String P_DECAY_INTERVAL_MS = "rawwise.axiom.scheduler.decay.interval-ms";
    public static final String P_CONSOL_ENABLED = "rawwise.axiom.scheduler.consolidate.enabled";
    public static final String P_CONSOL_INTERVAL_MS =
            "rawwise.axiom.scheduler.consolidate.interval-ms";

    public static AxiomSchedulerConfig fromSystemProperties() {
        boolean enabled = parseBool(System.getProperty(P_ENABLED), true);
        boolean warmup = parseBool(System.getProperty(P_WARMUP), true);
        int parallelism = parseInt(System.getProperty(P_PARALLELISM), 4);
        long shutdownTimeout = parseLong(System.getProperty(P_SHUTDOWN_TIMEOUT_SEC), 5L);
        List<String> agents = parseAgents(System.getProperty(P_AGENTS));
        boolean decayEnabled = parseBool(System.getProperty(P_DECAY_ENABLED), true);
        long decayInterval = parseLong(System.getProperty(P_DECAY_INTERVAL_MS), 600_000L);
        boolean consolEnabled = parseBool(System.getProperty(P_CONSOL_ENABLED), true);
        long consolInterval = parseLong(System.getProperty(P_CONSOL_INTERVAL_MS), 300_000L);
        return new AxiomSchedulerConfig(
                enabled,
                warmup,
                parallelism,
                shutdownTimeout,
                agents,
                decayEnabled,
                decayInterval,
                consolEnabled,
                consolInterval);
    }

    private static boolean parseBool(String v, boolean def) {
        if (v == null || v.isBlank()) return def;
        return Boolean.parseBoolean(v.trim());
    }

    private static int parseInt(String v, int def) {
        if (v == null || v.isBlank()) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static long parseLong(String v, long def) {
        if (v == null || v.isBlank()) return def;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static List<String> parseAgents(String v) {
        if (v == null || v.isBlank()) {
            return List.of("default");
        }
        return Arrays.stream(v.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }
}
