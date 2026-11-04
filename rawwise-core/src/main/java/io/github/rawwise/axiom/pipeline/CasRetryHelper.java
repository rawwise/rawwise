package io.github.rawwise.axiom.pipeline;

import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class CasRetryHelper {

    private CasRetryHelper() {}

    public static boolean casWithRetry(
            Metastore metastore,
            List<Precondition> preconditions,
            List<Mutation> mutations,
            String resourceId,
            int maxRetries,
            long baseDelayMs,
            long maxDelayMs) {
        boolean isSingleflightClaim = isSingleflightClaim(preconditions);
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                boolean success = metastore.compareAndSwap(preconditions, mutations);
                if (success) return true;
                if (isSingleflightClaim) {
                    log.debug(
                            "Singleflight claim lost for resource={}, returning quickly",
                            resourceId);
                    return false;
                }
                retrySleep(attempt, baseDelayMs, maxDelayMs);
            } catch (IOException e) {
                log.warn(
                        "CAS attempt {}/{} failed for resource={}: {}",
                        attempt + 1,
                        maxRetries,
                        resourceId,
                        e.getMessage());
                retrySleep(attempt, baseDelayMs, maxDelayMs);
            }
        }
        log.warn("CAS retries exhausted for resource={}, attempts={}", resourceId, maxRetries);
        return false;
    }

    private static boolean isSingleflightClaim(List<Precondition> preconditions) {
        if (preconditions == null || preconditions.size() != 1) {
            return false;
        }
        return preconditions.get(0) instanceof Precondition.NotExists;
    }

    private static void retrySleep(int attempt, long baseDelayMs, long maxDelayMs) {
        try {
            long expDelay = Math.min(baseDelayMs * (1L << Math.min(attempt, 30)), maxDelayMs);
            long jitter = (long) (expDelay * ThreadLocalRandom.current().nextDouble(-0.5, 0.5));
            long delay = Math.max(1, expDelay + jitter);
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
