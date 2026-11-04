package io.github.rawwise.backend.server.auth;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.rawwise.governance.auth.AuthMetastore;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetastoreTokenValidator implements TokenValidator {

    private static final int CB_THRESHOLD = 10;
    private static final long CB_RESET_MS = 120_000;

    private final AuthMetastore authMetastore;
    private final Cache<String, CachedEntry> tokenCache;
    private final SimpleTokenValidator fallback = new SimpleTokenValidator();
    private final AtomicInteger circuitBreakerFailures = new AtomicInteger(0);
    private volatile boolean circuitOpen = false;
    private volatile long circuitOpenSince = 0;

    public MetastoreTokenValidator(AuthMetastore authMetastore) {
        this.authMetastore = authMetastore;
        this.tokenCache =
                Caffeine.newBuilder()
                        .maximumSize(10000)
                        .expireAfterWrite(Duration.ofMinutes(30))
                        .build();
    }

    @Override
    public boolean validate(String token) {
        if (token == null || token.isBlank()) return false;
        return resolveFromMetastore(token) != null;
    }

    @Override
    public Principal extractPrincipal(String token) {
        if (token == null || token.isBlank()) return null;
        io.github.rawwise.governance.auth.Principal govPrincipal = resolveFromMetastore(token);
        if (govPrincipal == null) return null;
        return mapPrincipal(govPrincipal);
    }

    private io.github.rawwise.governance.auth.Principal resolveFromMetastore(String token) {
        var cached = tokenCache.getIfPresent(token);
        if (cached != null) {
            return cached.principal;
        }

        if (circuitOpen) {
            if (System.currentTimeMillis() - circuitOpenSince > CB_RESET_MS) {
                circuitOpen = false;
                circuitBreakerFailures.set(0);
            } else {
                log.warn("Metastore circuit breaker OPEN, falling back to SimpleTokenValidator");
                return null;
            }
        }

        try {
            io.github.rawwise.governance.auth.Principal principal =
                    authMetastore.resolveToken(token);
            if (principal != null) {
                tokenCache.put(token, new CachedEntry(principal));
            }
            circuitBreakerFailures.set(0);
            return principal;
        } catch (IOException e) {
            int failures = circuitBreakerFailures.incrementAndGet();
            if (failures >= CB_THRESHOLD) {
                circuitOpen = true;
                circuitOpenSince = System.currentTimeMillis();
                log.error(
                        "MetastoreTokenValidator circuit breaker OPEN after {} failures", failures);
            }
            log.warn("Metastore token validation failed: {}", e.getMessage());
            return null;
        }
    }

    private Principal mapPrincipal(io.github.rawwise.governance.auth.Principal govPrincipal) {
        return switch (govPrincipal) {
            case io.github.rawwise.governance.auth.Principal.UserPrincipal up ->
                    new Principal(
                            up.id(),
                            "default",
                            Role.USER,
                            Set.of(
                                    Permission.DDL,
                                    Permission.WRITE_PATH,
                                    Permission.READ_CROSS_TENANT));
            case io.github.rawwise.governance.auth.Principal.AgentPrincipal ap ->
                    new Principal(
                            ap.agentId(),
                            "default",
                            Role.GUEST,
                            Set.of(Permission.READ_CROSS_TENANT));
        };
    }

    private record CachedEntry(io.github.rawwise.governance.auth.Principal principal) {}
}
