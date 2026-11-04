package io.github.rawwise.governance.auth;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PermissionCache {

    private final Cache<String, ResolvedPermissions> cache;

    public PermissionCache(int maxSize, int expireSeconds) {
        this.cache =
                Caffeine.newBuilder()
                        .maximumSize(maxSize)
                        .expireAfterWrite(Duration.ofSeconds(expireSeconds))
                        .build();
    }

    public ResolvedPermissions get(String principalId, AuthMetastore authMetastore)
            throws IOException {
        try {
            return cache.get(
                    principalId,
                    id -> {
                        try {
                            return authMetastore.resolvePermissions(id);
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                    });
        } catch (CompletionException e) {
            if (e.getCause() instanceof IOException ioe) throw ioe;
            throw e;
        }
    }

    public void invalidate(String principalId) {
        cache.invalidate(principalId);
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }
}
