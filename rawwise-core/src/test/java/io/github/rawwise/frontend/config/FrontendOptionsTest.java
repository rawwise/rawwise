package io.github.rawwise.frontend.config;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.governance.config.RawWiseOptions;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.*;

@DisplayName("FrontendOptions")
class FrontendOptionsTest {

    private Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(RawWiseOptions.FRONTEND_PORT.key(), 8080);
        config.put("frontend.deployment.mode", "standalone");
        config.put(RawWiseOptions.FRONTEND_GRPC_BACKEND_HOST.key(), "localhost");
        config.put(RawWiseOptions.FRONTEND_GRPC_BACKEND_PORT.key(), 9090);
        config.put(RawWiseOptions.FRONTEND_QUERY_TIMEOUT_SECONDS.key(), 30L);
        config.put(RawWiseOptions.AUTH_ENABLED.key(), true);
        config.put(RawWiseOptions.AXIOM_ENABLED.key(), false);
        config.put(RawWiseOptions.AXIOM_ORCHESTRATOR_ENABLED.key(), false);
        config.put(RawWiseOptions.FRONTEND_GRPC_TLS_ENABLED.key(), false);
        return config;
    }

    @Test
    @DisplayName("FRONTEND_PORT key should match RawWiseOptions")
    void frontendOptions_shouldUseRawWiseOptionsKeys() {
        assertThat(RawWiseOptions.FRONTEND_PORT.key()).isEqualTo("rawwise.frontend.port");
        assertThat(RawWiseOptions.FRONTEND_GRPC_BACKEND_HOST.key())
                .isEqualTo("rawwise.frontend.grpc.backend.host");
        assertThat(RawWiseOptions.FRONTEND_GRPC_BACKEND_PORT.key())
                .isEqualTo("rawwise.frontend.grpc.backend.port");
        assertThat(RawWiseOptions.FRONTEND_QUERY_TIMEOUT_SECONDS.key())
                .isEqualTo("rawwise.frontend.query.timeout-seconds");

        Map<String, Object> config = baseConfig();
        FrontendOptions options = new FrontendOptions(config);
        assertThat(options.getPort()).isEqualTo(8080);
        assertThat(options.getGrpcBackendHost()).isEqualTo("localhost");
        assertThat(options.getGrpcBackendPort()).isEqualTo(9090);
        assertThat(options.getQueryTimeoutSeconds()).isEqualTo(30L);
    }

    @Test
    @DisplayName("STANDALONE mode should force auth.enabled=true when configured false")
    void standaloneMode_shouldForceAuthEnabled() {
        Map<String, Object> config = baseConfig();
        config.put("frontend.deployment.mode", "standalone");
        config.put(RawWiseOptions.AUTH_ENABLED.key(), false);

        String prev = System.getProperty("rawwise.auth.explicitly-unsafe-disable");
        try {
            System.clearProperty("rawwise.auth.explicitly-unsafe-disable");
            FrontendOptions options = new FrontendOptions(config);
            assertThat(options.isAuthEnabled()).isTrue();
        } finally {
            if (prev != null) {
                System.setProperty("rawwise.auth.explicitly-unsafe-disable", prev);
            } else {
                System.clearProperty("rawwise.auth.explicitly-unsafe-disable");
            }
        }
    }

    @Test
    @DisplayName("resolveSecret with invalid prefix should throw")
    void resolveSecret_withInvalidPrefix_shouldThrow() {
        Map<String, Object> config = baseConfig();
        config.put(RawWiseOptions.FRONTEND_GRPC_TLS_TRUSTSTORE_PATH.key(), "${env:AWS_KEY}");

        assertThatThrownBy(() -> new FrontendOptions(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RAWWISE_SECRET_");
    }

    @Test
    @DisplayName("resolveSecret with missing close brace should throw")
    void resolveSecret_withMissingCloseBrace_shouldThrow() {
        Map<String, Object> config = baseConfig();
        config.put(
                RawWiseOptions.FRONTEND_GRPC_TLS_TRUSTSTORE_PATH.key(),
                "${env:RAWWISE_SECRET_TEST");

        assertThatThrownBy(() -> new FrontendOptions(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid secret reference format");
    }

    @Test
    @DisplayName("resolveSecret with not-found env var should throw")
    void resolveSecret_withNotFoundEnvVar_shouldThrow() {
        Map<String, Object> config = baseConfig();
        config.put(
                RawWiseOptions.FRONTEND_GRPC_TLS_TRUSTSTORE_PATH.key(),
                "${env:RAWWISE_SECRET_NONEXISTENT}");

        assertThatThrownBy(() -> new FrontendOptions(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Secret environment variable not found");
    }
}
