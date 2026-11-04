package io.github.rawwise.engine.compaction.cleanup;

import io.github.rawwise.engine.storage.mvcc.LmdbVersionChainStore;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * MVCC version chain garbage collector.
 *
 * <p>Prunes version chains to keep at most {@code MVCC_RETENTION_VERSIONS} versions per key, and
 * removes versions older than {@code MVCC_RETENTION_MINUTES}.
 *
 * <p>Designed to be scheduled periodically as a background task. Implements {@link Runnable} for
 * easy integration with scheduled executors.
 */
@Slf4j
public final class VersionChainGC implements Runnable {

    private final LmdbVersionChainStore store;
    private final RawWiseConfig config;
    private final Supplier<Long> clock;

    /**
     * @param store the LMDB version chain store to GC
     * @param config rawwise configuration (must not be null)
     */
    public VersionChainGC(LmdbVersionChainStore store, RawWiseConfig config) {
        this(store, config, System::currentTimeMillis);
    }

    /**
     * @param store the LMDB version chain store to GC
     * @param config rawwise configuration (must not be null)
     * @param clock clock supplier for testability (default: System::currentTimeMillis)
     */
    public VersionChainGC(LmdbVersionChainStore store, RawWiseConfig config, Supplier<Long> clock) {
        if (store == null) {
            throw new IllegalArgumentException("store must not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        this.store = store;
        this.config = config;
        this.clock = clock;
    }

    // FIX-7: Extract gc() as package-private method for testability.
    // run() wraps it with error handling.

    /** Execute garbage collection on the version chain store. Package-private for testability. */
    void gc() {
        int maxVersions = config.get(RawWiseOptions.MVCC_RETENTION_VERSIONS).intValue();
        long retentionMinutes = config.get(RawWiseOptions.MVCC_RETENTION_MINUTES);
        long minCommitTs = clock.get() - retentionMinutes * 60 * 1000;

        log.info(
                "VersionChainGC starting: maxVersions={} retentionMinutes={} minCommitTs={}",
                maxVersions,
                retentionMinutes,
                minCommitTs);

        store.gc(maxVersions, minCommitTs);

        log.info("VersionChainGC completed successfully");
    }

    @Override
    public void run() {
        try {
            gc();
        } catch (RuntimeException e) {
            log.error("VersionChainGC failed", e);
        }
    }
}
