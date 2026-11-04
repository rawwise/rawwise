package io.github.rawwise.governance.auth;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TokenCache {

    private final Cache<String, Principal> cache;

    public TokenCache(int maxSize) {
        this(maxSize, 300);
    }

    public TokenCache(int maxSize, int expireSeconds) {
        this.cache =
                Caffeine.newBuilder()
                        .maximumSize(maxSize)
                        .expireAfterWrite(Duration.ofSeconds(expireSeconds))
                        .build();
    }

    public Principal get(String token, AuthMetastore authMetastore) throws IOException {
        String tokenHash = AuthCrypto.sha256Hex(token);
        try {
            return cache.get(
                    tokenHash,
                    key -> {
                        try {
                            return authMetastore.resolveToken(token);
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                    });
        } catch (CompletionException e) {
            if (e.getCause() instanceof IOException ioe) throw ioe;
            throw e;
        }
    }

    public void invalidate(String token) {
        cache.invalidate(AuthCrypto.sha256Hex(token));
    }

    public void invalidateByHash(String tokenHash) {
        cache.invalidate(tokenHash);
    }
}
