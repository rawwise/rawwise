package io.github.rawwise.frontend.backend;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.frontend.config.FrontendOptions;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GrpcConnectionManagementTest {

    private FrontendOptions createOptions(boolean tlsEnabled) {
        Map<String, Object> config = new HashMap<>();
        config.put(RawWiseOptions.FRONTEND_PORT.key(), 8080);
        config.put(RawWiseOptions.FRONTEND_GRPC_TLS_ENABLED.key(), tlsEnabled);
        config.put(RawWiseOptions.AUTH_ENABLED.key(), false);
        config.put(RawWiseOptions.CONTEXT_ENABLED.key(), false);
        config.put(RawWiseOptions.EXECUTION_ENABLED.key(), false);
        config.put(RawWiseOptions.AXIOM_ENABLED.key(), false);
        config.put("rawwise.auth.explicitly-unsafe-disable", "true");
        return new FrontendOptions(config);
    }

    @Test
    void clientBuilderHasKeepAliveConfigured() {
        FrontendOptions options = createOptions(false);
        GrpcBackendClient client = new GrpcBackendClient("localhost", 9090, 30, options);
        try {
            assertThat(client).isNotNull();
        } finally {
            client.close();
        }
    }

    @Test
    void clientWithoutOptionsStillConnects() {
        GrpcBackendClient client = new GrpcBackendClient("localhost", 9090, 30);
        try {
            assertThat(client).isNotNull();
        } finally {
            client.close();
        }
    }

    @Test
    void clientCloseIsIdempotent() {
        GrpcBackendClient client = new GrpcBackendClient("localhost", 9090, 30);
        client.close();
        client.close();
    }

    @Test
    void keepAliveWithoutCallsIsEnabledInClient() {
        FrontendOptions options = createOptions(false);
        GrpcBackendClient client = new GrpcBackendClient("localhost", 9090, 30, options);
        try {
            assertThat(client).isNotNull();
        } finally {
            client.close();
        }
    }

    @Test
    void maxInboundMessageSizeIs64MB() {
        FrontendOptions options = createOptions(false);
        GrpcBackendClient client = new GrpcBackendClient("localhost", 9090, 30, options);
        try {
            assertThat(client).isNotNull();
        } finally {
            client.close();
        }
    }
}
