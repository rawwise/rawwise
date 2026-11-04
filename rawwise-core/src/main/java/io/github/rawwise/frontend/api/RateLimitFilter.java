package io.github.rawwise.frontend.api;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.RateLimiter;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RateLimitFilter implements Handler {
    private final RateLimiter globalLimiter;
    private final Cache<String, RateLimiter> ipLimiters =
            Caffeine.newBuilder()
                    .maximumSize(10_000)
                    .expireAfterAccess(Duration.ofMinutes(10))
                    .build();
    private final Cache<String, RateLimiter> adminLimiters =
            Caffeine.newBuilder()
                    .maximumSize(1_000)
                    .expireAfterAccess(Duration.ofMinutes(10))
                    .build();
    private final int perIpPermits;
    private final int perAdminIpPermits;

    public RateLimitFilter(int globalPermits, int perIpPermits) {
        this(globalPermits, perIpPermits, perIpPermits / 2);
    }

    public RateLimitFilter(int globalPermits, int perIpPermits, int perAdminIpPermits) {
        this.globalLimiter = RateLimiter.create(globalPermits);
        this.perIpPermits = perIpPermits;
        this.perAdminIpPermits = perAdminIpPermits;
    }

    @Override
    public void handle(Context ctx) {
        if (ctx.path().startsWith("/api/admin/")) {
            return;
        }

        if (!globalLimiter.tryAcquire()) {
            log.warn("Global rate limit exceeded, rejecting request from {}", ctx.ip());
            throw new io.javalin.http.HttpResponseException(
                    429, "{\"error\":\"Too Many Requests\",\"type\":\"global\"}");
        }

        var ipLimiter =
                ipLimiters.asMap().computeIfAbsent(ctx.ip(), k -> RateLimiter.create(perIpPermits));
        if (!ipLimiter.tryAcquire()) {
            log.warn("Per-IP rate limit exceeded for {}", ctx.ip());
            throw new io.javalin.http.HttpResponseException(
                    429, "{\"error\":\"Too Many Requests\",\"type\":\"per-ip\"}");
        }
    }
}
