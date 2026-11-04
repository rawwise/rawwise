package io.github.rawwise.backend.server.auth;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public class AuthServerInterceptor implements ServerInterceptor {
    private static final Metadata.Key<String> TOKEN_METADATA_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<Principal> PRINCIPAL_KEY = Context.key("principal");
    public static final Context.Key<String> TENANT_KEY = Context.key("tenantId");

    private final TokenValidator tokenValidator;

    public AuthServerInterceptor(TokenValidator tokenValidator) {
        this.tokenValidator = tokenValidator;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        String token = headers.get(TOKEN_METADATA_KEY);
        if (token == null || !tokenValidator.validate(token)) {
            call.close(
                    Status.UNAUTHENTICATED.withDescription("Missing or invalid token"),
                    new Metadata());
            return new ServerCall.Listener<>() {};
        }

        try {
            Principal principal = tokenValidator.extractPrincipal(token);
            Context ctx =
                    Context.current()
                            .withValue(PRINCIPAL_KEY, principal)
                            .withValue(TENANT_KEY, principal.tenantId());
            return Contexts.interceptCall(ctx, call, headers, next);
        } catch (Exception e) {
            call.close(
                    Status.UNAUTHENTICATED.withDescription("Authentication failed"),
                    new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }
}
