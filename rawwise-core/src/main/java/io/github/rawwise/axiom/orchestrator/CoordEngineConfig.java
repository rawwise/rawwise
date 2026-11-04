package io.github.rawwise.axiom.orchestrator;

import io.github.rawwise.metastore.Metastore;
import java.util.Objects;

public record CoordEngineConfig(
        Metastore metastore,
        long defaultLockTtlMs,
        long defaultLeaseTtlMs,
        long defaultTermTtlMs,
        int maxRetryCas,
        long casRetryDelayMs,
        long maxCasRetryDelayMs,
        long gcIntervalMs,
        long messageTtlMs,
        CoordTrustAdapter trustAdapter) {
    public CoordEngineConfig {
        Objects.requireNonNull(metastore, "metastore must not be null");
        if (defaultLockTtlMs <= 0) defaultLockTtlMs = 30_000L;
        if (defaultLeaseTtlMs <= 0) defaultLeaseTtlMs = 60_000L;
        if (defaultTermTtlMs <= 0) defaultTermTtlMs = 30_000L;
        if (maxRetryCas <= 0) maxRetryCas = 3;
        if (casRetryDelayMs <= 0) casRetryDelayMs = 100L;
        if (maxCasRetryDelayMs <= 0) maxCasRetryDelayMs = 5_000L;
        if (gcIntervalMs <= 0) gcIntervalMs = 60_000L;
        if (messageTtlMs <= 0) messageTtlMs = 3_600_000L;
    }

    public static CoordEngineConfig defaults(Metastore metastore) {
        return new CoordEngineConfig(
                metastore, 30_000L, 60_000L, 30_000L, 3, 100L, 5_000L, 60_000L, 3_600_000L, null);
    }
}
