package io.github.rawwise.frontend.backend;

import io.github.rawwise.frontend.config.FrontendOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackendClientFactory {

    public BackendClient createBackendClient(FrontendOptions options) {
        return createGrpcBackendClient(options);
    }

    private BackendClient createGrpcBackendClient(FrontendOptions options) {
        return new GrpcBackendClient(
                options.getGrpcBackendHost(),
                options.getGrpcBackendPort(),
                options.getQueryTimeoutSeconds(),
                options);
    }
}
