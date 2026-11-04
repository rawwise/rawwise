package io.github.rawwise.axiom.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * E2E test for {@link SummarizationServiceFactory}.
 *
 * <p>Validates three scenarios required by P1.2:
 *
 * <ol>
 *   <li>Default configuration -> {@link NoOpSummarizationService} (empty l0/l1, isAvailable=false)
 *   <li>type=http with unreachable port -> {@link HttpSummarizationService} constructs
 *       successfully, summarize() throws IOException (network unreachable)
 *   <li>type=invalid -> degrades to NoOp with warn (no throw)
 * </ol>
 */
class SummarizationServiceFactoryE2ETest {

    private static final List<String> KEYS =
            List.of(
                    SummarizationServiceFactory.KEY_TYPE,
                    SummarizationServiceFactory.KEY_ENDPOINT,
                    SummarizationServiceFactory.KEY_MODEL,
                    SummarizationServiceFactory.KEY_API_KEY,
                    SummarizationServiceFactory.KEY_TIMEOUT_MS,
                    SummarizationServiceFactory.KEY_MAX_RETRIES,
                    SummarizationServiceFactory.KEY_L0_MAX_TOKENS,
                    SummarizationServiceFactory.KEY_L1_MAX_TOKENS);

    @BeforeEach
    void clearProperties() {
        KEYS.forEach(System::clearProperty);
    }

    @AfterEach
    void resetProperties() {
        KEYS.forEach(System::clearProperty);
    }

    @Test
    void defaultConfigReturnsNoOpAndProducesEmptySummary() throws IOException {
        SummarizationService service = SummarizationServiceFactory.createFromSystemProperties();

        assertNotNull(service, "factory must return non-null service");
        assertTrue(
                service instanceof NoOpSummarizationService,
                "default config must return NoOp, got: " + service.getClass().getName());
        assertFalse(service.isAvailable(), "NoOp must report isAvailable=false");

        SummarizationService.SummarizationResult result =
                service.summarize(
                        "some content body that would normally be summarized",
                        new SummarizationService.SummarizationRequest(
                                null, 100, 2000, "text/plain", ""));
        assertNotNull(result);
        assertEquals("", result.l0Abstract(), "NoOp must return empty l0Abstract");
        assertEquals("", result.l1Overview(), "NoOp must return empty l1Overview");
        assertEquals(0L, result.l0TokenCount());
        assertEquals(0L, result.l1TokenCount());
        assertEquals("noop", result.model());
    }

    @Test
    void httpProviderWithUnreachableEndpointConstructsButFailsOnSummarize() {
        // Unroutable port — connect must fail fast.
        System.setProperty(SummarizationServiceFactory.KEY_TYPE, "http");
        System.setProperty(
                SummarizationServiceFactory.KEY_ENDPOINT, "http://127.0.0.1:1/v1/chat/completions");
        System.setProperty(SummarizationServiceFactory.KEY_MODEL, "test-model");
        System.setProperty(SummarizationServiceFactory.KEY_API_KEY, "test-key");
        System.setProperty(SummarizationServiceFactory.KEY_TIMEOUT_MS, "1000");
        System.setProperty(SummarizationServiceFactory.KEY_MAX_RETRIES, "0");

        SummarizationService service = SummarizationServiceFactory.createFromSystemProperties();
        assertNotNull(service);
        assertTrue(
                service instanceof HttpSummarizationService,
                "type=http with valid endpoint+model must construct HttpSummarizationService, got: "
                        + service.getClass().getName());
        assertTrue(
                service.isAvailable(),
                "HttpSummarizationService with non-empty endpoint+apiKey must report isAvailable=true");

        // summarize() must surface the network failure as IOException.
        assertThrows(
                IOException.class,
                () ->
                        service.summarize(
                                "trigger network call",
                                new SummarizationService.SummarizationRequest(
                                        null, 100, 2000, "text/plain", "")));
    }

    @Test
    void httpProviderWithMissingEndpointDegradesToNoOp() throws IOException {
        System.setProperty(SummarizationServiceFactory.KEY_TYPE, "http");
        // endpoint and model intentionally omitted

        SummarizationService service = SummarizationServiceFactory.createFromSystemProperties();
        assertNotNull(service);
        assertFalse(
                service instanceof HttpSummarizationService,
                "missing endpoint/model must degrade to NoOp, not HttpSummarizationService");
        assertTrue(service instanceof NoOpSummarizationService);

        SummarizationService.SummarizationResult result =
                service.summarize(
                        "anything",
                        new SummarizationService.SummarizationRequest(
                                null, 100, 2000, "text/plain", ""));
        assertEquals("", result.l0Abstract());
        assertEquals("", result.l1Overview());
    }

    @Test
    void invalidTypeDegradesToNoOp() throws IOException {
        System.setProperty(SummarizationServiceFactory.KEY_TYPE, "definitely-not-a-real-type");

        SummarizationService service = SummarizationServiceFactory.createFromSystemProperties();
        assertNotNull(service);
        assertTrue(
                service instanceof NoOpSummarizationService,
                "invalid type must degrade to NoOp, got: " + service.getClass().getName());
        assertFalse(service.isAvailable());

        SummarizationService.SummarizationResult result =
                service.summarize(
                        "anything",
                        new SummarizationService.SummarizationRequest(
                                null, 100, 2000, "text/plain", ""));
        assertEquals("", result.l0Abstract());
        assertEquals("", result.l1Overview());
    }
}
