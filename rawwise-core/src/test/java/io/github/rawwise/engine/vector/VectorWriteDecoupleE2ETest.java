package io.github.rawwise.engine.vector;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName(".5-1: Vector write decoupling E2E")
class VectorWriteDecoupleE2ETest {

    @Test
    @DisplayName("EmbeddingProvider scalar write normal when unavailable")
    void scalarWriteSucceedsWhenEmbeddingUnavailable() {
        CircuitBreaker cb = new CircuitBreaker(1, 30_000L);
        HttpEmbeddingProvider provider =
                new HttpEmbeddingProvider(null, null, null, null, 128, 0, 32, cb, 1000);

        cb.onFailure();

        float[] result = provider.embed("test text");
        assertNotNull(result);
        assertEquals(128, result.length);
        for (float v : result) assertEquals(0.0f, v, 0.001f);

        provider.close();
    }

    @Test
    @DisplayName("vectorcolumnasync")
    void vectorColumnAsyncBackfill() {
        CircuitBreaker cb = new CircuitBreaker(5, 30_000L);
        HttpEmbeddingProvider httpProvider =
                new HttpEmbeddingProvider(null, null, null, null, 64, 0, 32, cb, 1000);
        AsyncEmbeddingProvider asyncProvider =
                new AsyncEmbeddingProvider(httpProvider, 2, 32, 5000);

        cb.onFailure();
        cb.onFailure();
        cb.onFailure();
        cb.onFailure();
        cb.onFailure();

        asyncProvider.embedAsync(
                "ns1",
                "key1",
                "text1",
                (key, embedding) -> {
                    fail("Callback should not be called when circuit breaker is OPEN");
                });

        assertTrue(asyncProvider.pendingCount() > 0);

        asyncProvider.close();
    }

    @Test
    @DisplayName("cache in test")
    void embeddingCacheHitRate() {
        VectorMetrics metrics = new VectorMetrics();

        metrics.recordCacheHit();
        metrics.recordCacheHit();
        metrics.recordCacheMiss();

        double hitRate = metrics.getEmbeddingCacheHitRate();
        assertEquals(2.0 / 3.0, hitRate, 0.001);

        assertEquals(0.0, new VectorMetrics().getEmbeddingCacheHitRate(), 0.001);
    }
}
