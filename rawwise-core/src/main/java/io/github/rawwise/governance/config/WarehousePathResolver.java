package io.github.rawwise.governance.config;

import lombok.extern.slf4j.Slf4j;

/**
 * Centralized resolver for rawwise.warehouse.path with fail-fast on missing config.
 *
 * <p>Resolution order:
 *
 * <ol>
 *   <li>System property {@code rawwise.warehouse.path}
 *   <li>Environment variable {@code RAWWISE_WAREHOUSE_PATH}
 *   <li>Hard-coded fallback {@code /tmp/rawwise/warehouse} ONLY if explicitly enabled via {@code
 *       -Drawwise.warehouse.allow-tmp-fallback=true}; otherwise throw.
 * </ol>
 *
 * <p>The fail-fast behavior prevents silent data loss caused by Spark Executor JVMs not inheriting
 * the system property, which historically caused business data (LMDB / VectorIndex) to be written
 * to /tmp/rawwise/warehouse and lost on macOS reboot.
 */
@Slf4j
public final class WarehousePathResolver {

    private static final String SYSPROP_KEY = "rawwise.warehouse.path";
    private static final String ENV_KEY = "RAWWISE_WAREHOUSE_PATH";
    private static final String ALLOW_TMP_KEY = "rawwise.warehouse.allow-tmp-fallback";
    private static final String TMP_FALLBACK = "/tmp/rawwise/warehouse";

    private WarehousePathResolver() {}

    /**
     * Resolve the warehouse path or throw IllegalStateException if neither sysprop nor env var is
     * set and the unsafe fallback flag is not enabled.
     */
    public static String resolve() {
        String fromSys = System.getProperty(SYSPROP_KEY);
        if (fromSys != null && !fromSys.isEmpty()) {
            return fromSys;
        }
        String fromEnv = System.getenv(ENV_KEY);
        if (fromEnv != null && !fromEnv.isEmpty()) {
            log.warn(
                    "Warehouse path resolved from env {} (sysprop {} missing); "
                            + "Spark Executor may not have inherited -D properly",
                    ENV_KEY,
                    SYSPROP_KEY);
            return fromEnv;
        }
        boolean allowTmp = Boolean.parseBoolean(System.getProperty(ALLOW_TMP_KEY, "false"));
        if (allowTmp) {
            log.warn(
                    "Falling back to {} because {}=true; data WILL BE LOST on macOS reboot",
                    TMP_FALLBACK,
                    ALLOW_TMP_KEY);
            return TMP_FALLBACK;
        }
        throw new IllegalStateException(
                "rawwise.warehouse.path is not configured. Set -D"
                        + SYSPROP_KEY
                        + "=<persistent path> or env "
                        + ENV_KEY
                        + ". "
                        + "Refusing to fall back to "
                        + TMP_FALLBACK
                        + " to prevent data loss. To explicitly allow tmp fallback (e.g. for benchmarks), "
                        + "set -D"
                        + ALLOW_TMP_KEY
                        + "=true.");
    }
}
