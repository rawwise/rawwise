package io.github.rawwise.engine.vector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class EmbeddingProviderFactoryE2ETest {

    private static final List<String> KEYS =
            List.of(
                    EmbeddingProviderFactory.KEY_TYPE,
                    EmbeddingProviderFactory.KEY_ENDPOINT,
                    EmbeddingProviderFactory.KEY_MODEL,
                    EmbeddingProviderFactory.KEY_DIMENSION,
                    EmbeddingProviderFactory.KEY_API_KEY,
                    EmbeddingProviderFactory.KEY_TIMEOUT_MS,
                    EmbeddingProviderFactory.KEY_MAX_RETRIES,
                    EmbeddingProviderFactory.KEY_BATCH_SIZE,
                    EmbeddingProviderFactory.KEY_FORMAT);

    @BeforeEach
    void clearProperties() {
        KEYS.forEach(System::clearProperty);
    }

    @AfterEach
    void resetProperties() {
        KEYS.forEach(System::clearProperty);
    }

    @Test
    void defaultConfigReturnsNoOpProviderWithZeroVector() {
        EmbeddingProvider provider = EmbeddingProviderFactory.createFromSystemProperties();

        assertNotNull(provider, "default factory must return a non-null provider");
        assertEquals("noop-zero-vector", provider.modelIdentifier());

        float[] vec = provider.embed("hello world");
        assertEquals(EmbeddingProviderFactory.DEFAULT_DIMENSION, vec.length);
        for (float v : vec) {
            assertEquals(0.0f, v, 0.0f, "noop provider must return zero vector");
        }

        float[][] batch = provider.embedBatch(List.of("a", "b", "c"));
        assertEquals(3, batch.length);
        for (float[] row : batch) {
            assertEquals(EmbeddingProviderFactory.DEFAULT_DIMENSION, row.length);
        }
    }

    @Test
    void httpProviderWithUnreachableEndpointThrowsEmbeddingUnavailable() {
        // Use an unroutable port so connect fails fast.
        System.setProperty(EmbeddingProviderFactory.KEY_TYPE, "http");
        System.setProperty(
                EmbeddingProviderFactory.KEY_ENDPOINT, "http://127.0.0.1:1/v1/embeddings");
        System.setProperty(EmbeddingProviderFactory.KEY_MODEL, "test-model");
        System.setProperty(EmbeddingProviderFactory.KEY_DIMENSION, "8");
        System.setProperty(EmbeddingProviderFactory.KEY_TIMEOUT_MS, "500");
        System.setProperty(EmbeddingProviderFactory.KEY_MAX_RETRIES, "0");

        EmbeddingProvider provider = EmbeddingProviderFactory.createFromSystemProperties();
        assertNotNull(provider);
        assertTrue(
                provider instanceof HttpEmbeddingProvider,
                "type=http with valid endpoint+model must construct HttpEmbeddingProvider, got: "
                        + provider.getClass().getName());

        // Must throw EmbeddingUnavailableException on unreachable endpoint
        // (either directly on first call, or because circuit breaker opens after retries).
        assertThrows(
                EmbeddingUnavailableException.class, () -> provider.embed("trigger network call"));

        if (provider instanceof AutoCloseable closeable) {
            assertDoesNotThrow(closeable::close);
        }
    }

    @Test
    void stubProviderIsDeterministicAndUsesConfiguredDimension() {
        System.setProperty(EmbeddingProviderFactory.KEY_TYPE, "stub");
        System.setProperty(EmbeddingProviderFactory.KEY_DIMENSION, "16");

        EmbeddingProvider provider = EmbeddingProviderFactory.createFromSystemProperties();
        assertNotNull(provider);
        assertEquals("stub-deterministic-l2norm", provider.modelIdentifier());

        float[] first = provider.embed("same query");
        float[] second = provider.embed("same query");
        assertEquals(16, first.length);
        assertArrayEquals(
                first,
                second,
                0.0f,
                "deterministic stub must return identical vectors for the same input");

        boolean anyNonZero = false;
        for (float v : first) {
            if (v != 0.0f) {
                anyNonZero = true;
                break;
            }
        }
        assertTrue(anyNonZero, "stub provider must produce non-zero entries");

        float[] other = provider.embed("different query");
        boolean differs = false;
        for (int i = 0; i < first.length; i++) {
            if (first[i] != other[i]) {
                differs = true;
                break;
            }
        }
        assertTrue(differs, "different inputs must produce different stub embeddings");
    }

    @Test
    void httpProviderWithMissingEndpointDegradesToNoOp() {
        System.setProperty(EmbeddingProviderFactory.KEY_TYPE, "http");
        // endpoint and model intentionally omitted
        System.setProperty(EmbeddingProviderFactory.KEY_DIMENSION, "32");

        EmbeddingProvider provider = EmbeddingProviderFactory.createFromSystemProperties();
        assertNotNull(provider);
        assertFalse(
                provider instanceof HttpEmbeddingProvider,
                "missing endpoint/model must degrade to NoOp, not HttpEmbeddingProvider");
        assertEquals("noop-zero-vector", provider.modelIdentifier());

        float[] vec = provider.embed("anything");
        assertEquals(32, vec.length);
        for (float v : vec) {
            assertEquals(0.0f, v, 0.0f);
        }
    }

    // ---------------------------------------------------------------------
    // .1 — Volcengine multimodal protocol adapter (A)
    // ---------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void volcengineMultimodalSingleEmbed_parsesObjectShapedDataAndReturns2048Vec()
            throws Exception {
        // simulate multimodal should: data is object, is not array
        int dim = 2048;
        StringBuilder vec = new StringBuilder("[");
        for (int i = 0; i < dim; i++) {
            if (i > 0) vec.append(',');
            vec.append(i == 0 ? "0.0043" : "0.001");
        }
        vec.append(']');
        String body = "{\"created\":1781969861,\"data\":{\"embedding\":" + vec + "}}";

        HttpClient mockClient = mock(HttpClient.class);
        HttpResponse<String> mockResp = mock(HttpResponse.class);
        when(mockResp.statusCode()).thenReturn(200);
        when(mockResp.body()).thenReturn(body);
        when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResp);

        try (HttpEmbeddingProvider provider =
                new HttpEmbeddingProvider(
                        mockClient,
                        "https://ark.cn-beijing.volces.com/api/v3/embeddings/multimodal",
                        "doubao-embedding-vision-251215",
                        "fake-key",
                        dim,
                        0,
                        32,
                        new CircuitBreaker(5, 30_000L),
                        5_000L,
                        HttpEmbeddingProvider.EmbeddingFormat.VOLCENGINE_MULTIMODAL)) {

            float[] result = provider.embed("hello world");
            assertEquals(dim, result.length);
            assertEquals(0.0043f, result[0], 1e-6f);

            // verify body is multimodal array format
            ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
            verify(mockClient, atLeastOnce())
                    .send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));
            String requestBody = extractRequestBody(requestCaptor.getValue());
            assertTrue(
                    requestBody.contains("\"type\":\"text\""),
                    "multimodal request body should contain {type:text,...}, got: " + requestBody);
            assertTrue(
                    requestBody.contains("\"text\":\"hello world\""),
                    "multimodal request body should carry the raw text, got: " + requestBody);
            assertTrue(requestBody.contains("doubao-embedding-vision-251215"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void volcengineMultimodalBatch_degradesToPerItemCallsAndReturnsAllVectors() throws Exception {
        int dim = 8;
        // eachitem different 0.1 / 0.2 / 0.3 verify
        String[] bodies = new String[3];
        for (int i = 0; i < 3; i++) {
            StringBuilder vec = new StringBuilder("[");
            for (int j = 0; j < dim; j++) {
                if (j > 0) vec.append(',');
                vec.append(j == 0 ? String.format("0.%d", i + 1) : "0.0");
            }
            vec.append(']');
            bodies[i] = "{\"data\":{\"embedding\":" + vec + "}}";
        }

        HttpClient mockClient = mock(HttpClient.class);
        HttpResponse<String> r0 = mock(HttpResponse.class);
        HttpResponse<String> r1 = mock(HttpResponse.class);
        HttpResponse<String> r2 = mock(HttpResponse.class);
        when(r0.statusCode()).thenReturn(200);
        when(r1.statusCode()).thenReturn(200);
        when(r2.statusCode()).thenReturn(200);
        when(r0.body()).thenReturn(bodies[0]);
        when(r1.body()).thenReturn(bodies[1]);
        when(r2.body()).thenReturn(bodies[2]);
        when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(r0, r1, r2);

        try (HttpEmbeddingProvider provider =
                new HttpEmbeddingProvider(
                        mockClient,
                        "https://ark.cn-beijing.volces.com/api/v3/embeddings/multimodal",
                        "doubao-embedding-vision-251215",
                        null,
                        dim,
                        0,
                        32,
                        new CircuitBreaker(5, 30_000L),
                        5_000L,
                        HttpEmbeddingProvider.EmbeddingFormat.VOLCENGINE_MULTIMODAL)) {

            float[][] result = provider.embedBatch(List.of("a", "b", "c"));
            assertEquals(3, result.length);
            for (float[] row : result) {
                assertEquals(dim, row.length);
            }
            assertEquals(0.1f, result[0][0], 1e-6f);
            assertEquals(0.2f, result[1][0], 1e-6f);
            assertEquals(0.3f, result[2][0], 1e-6f);

            // multimodal is itemcall: 3 item -> 3 send
            verify(mockClient, times(3))
                    .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void openaiStandardSingleEmbed_parsesArrayShapedDataAndStaysBackwardCompatible()
            throws Exception {
        // simulate OpenAI / SiliconFlow : data is array
        int dim = 4;
        String body =
                "{\"data\":[{\"embedding\":[0.5,0.25,-0.125,0.0],\"index\":0}],\"model\":\"text-embedding-3-large\"}";

        HttpClient mockClient = mock(HttpClient.class);
        HttpResponse<String> mockResp = mock(HttpResponse.class);
        when(mockResp.statusCode()).thenReturn(200);
        when(mockResp.body()).thenReturn(body);
        when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResp);

        // not passing format -> go throughold two-arg constructor, default OPENAI_STANDARD(backward
        // compatible)
        try (HttpEmbeddingProvider provider =
                new HttpEmbeddingProvider(
                        mockClient,
                        "https://api.openai.com/v1/embeddings",
                        "text-embedding-3-large",
                        "sk-test",
                        dim,
                        0,
                        32,
                        new CircuitBreaker(5, 30_000L),
                        5_000L)) {

            float[] result = provider.embed("legacy compatible");
            assertArrayEquals(new float[] {0.5f, 0.25f, -0.125f, 0.0f}, result, 1e-6f);

            ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
            verify(mockClient, atLeastOnce())
                    .send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));
            String requestBody = extractRequestBody(requestCaptor.getValue());
            // OPENAI_STANDARD singleitem input=string, is not array
            assertTrue(
                    requestBody.contains("\"input\":\"legacy compatible\""),
                    "openai_standard single embed should use input=<string>, got: " + requestBody);
            assertFalse(
                    requestBody.contains("\"type\":\"text\""),
                    "openai_standard must NOT emit multimodal {type,text}, got: " + requestBody);
        }
    }

    /**
     * real multimodal endpoint sanity check. default @Disabled, RAWWISE_EMBEDDING_API_KEY test :
     *
     * <pre>
     * mvn test -pl rawwise-core -Dtest=EmbeddingProviderFactoryE2ETest#testRealVolcengineMultimodal
     * </pre>
     */
    @Test
    @Disabled("Requires real RAWWISE_EMBEDDING_API_KEY; run manually for sanity check")
    void testRealVolcengineMultimodal() {
        String apiKey = System.getenv("RAWWISE_EMBEDDING_API_KEY");
        Assumptions.assumeTrue(
                apiKey != null && !apiKey.isBlank(),
                "RAWWISE_EMBEDDING_API_KEY env var not set; skipping real-endpoint test");

        String endpoint =
                System.getenv()
                        .getOrDefault(
                                "RAWWISE_EMBEDDING_ENDPOINT",
                                "https://ark.cn-beijing.volces.com/api/v3/embeddings/multimodal");
        String model =
                System.getenv()
                        .getOrDefault("RAWWISE_EMBEDDING_MODEL", "doubao-embedding-vision-251215");

        try (HttpEmbeddingProvider provider =
                new HttpEmbeddingProvider(
                        HttpClient.newHttpClient(),
                        endpoint,
                        model,
                        apiKey,
                        2048,
                        1,
                        32,
                        new CircuitBreaker(5, 30_000L),
                        30_000L,
                        HttpEmbeddingProvider.EmbeddingFormat.VOLCENGINE_MULTIMODAL)) {

            float[] vec = provider.embed("hello world");
            assertEquals(2048, vec.length, "doubao-embedding-vision should return 2048-dim vector");
            boolean anyNonZero = false;
            for (float v : vec) {
                if (v != 0.0f) {
                    anyNonZero = true;
                    break;
                }
            }
            assertTrue(anyNonZero, "real embedding must contain non-zero entries");
        }
    }

    private static String extractRequestBody(HttpRequest request) {
        return request.bodyPublisher()
                .map(EmbeddingProviderFactoryE2ETest::publisherToString)
                .orElse("");
    }

    private static String publisherToString(java.net.http.HttpRequest.BodyPublisher publisher) {
        StringBuilder buf = new StringBuilder();
        publisher.subscribe(
                new java.util.concurrent.Flow.Subscriber<>() {
                    @Override
                    public void onSubscribe(java.util.concurrent.Flow.Subscription s) {
                        s.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(java.nio.ByteBuffer item) {
                        byte[] arr = new byte[item.remaining()];
                        item.get(arr);
                        buf.append(new String(arr, java.nio.charset.StandardCharsets.UTF_8));
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        /* ignore */
                    }

                    @Override
                    public void onComplete() {
                        /* done */
                    }
                });
        return buf.toString();
    }
}
