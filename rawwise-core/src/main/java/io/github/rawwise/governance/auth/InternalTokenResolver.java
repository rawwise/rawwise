package io.github.rawwise.governance.auth;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InternalTokenResolver {
    public static String resolve() {
        String envToken = System.getenv("RAWWISE_INTERNAL_TOKEN");
        if (envToken != null && !envToken.isBlank()) return envToken;
        String propToken = System.getProperty("rawwise.auth.internal-token");
        if (propToken != null && !propToken.isBlank()) return propToken;
        String envMode = System.getProperty("rawwise.env", "dev");
        if ("production".equalsIgnoreCase(envMode)) {
            throw new IllegalStateException(
                    "Production mode requires RAWWISE_INTERNAL_TOKEN env var or -Drawwise.auth.internal-token");
        }
        log.warn("Using default internal token -- DO NOT use in production");
        return "raw-wise-internal";
    }
}
