package io.github.rawwise.backend.observability;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.UUID;
import org.slf4j.MDC;

/**
 * gRPC trace ID propagator.
 *
 * <p>Architecture:
 *
 * <pre>
 *   Frontend -> [traceId in metadata] -> BE Server -> [traceId propagated] -> BE Node
 * Logging can be correlated via traceId across the full call chain.
 * </pre>
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>Client: if current thread has traceId, auto-injects into gRPC metadata
 *   <li>Server: extracts traceId from gRPC metadata, enters gRPC Context
 *   <li>If no traceId exists, auto-generates one (first 16 chars of UUID)
 *   <li>traceId can be acquired anywhere via {@link #currentTraceId()}
 * </ul>
 */
public class TraceIdPropagator {
    public static final Metadata.Key<String> TRACE_ID_KEY =
            Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<String> TRACE_ID_CONTEXT_KEY = Context.key("traceId");

    /**
     * Get the current thread's traceId (from gRPC Context or ThreadLocal).
     *
     * @return traceId, or null (not set)
     */
    public static String currentTraceId() {
        String ctxTraceId = TRACE_ID_CONTEXT_KEY.get();
        if (ctxTraceId != null) {
            return ctxTraceId;
        }
        return threadLocalTraceId.get();
    }

    /**
     * Set the current thread's traceId (for non-gRPC scenarios).
     *
     * @param traceId trace ID
     */
    public static void setTraceId(String traceId) {
        threadLocalTraceId.set(traceId);
        MDC.put("traceId", traceId);
    }

    /** Clear the current thread's traceId. */
    public static void clearTraceId() {
        threadLocalTraceId.remove();
        MDC.remove("traceId");
    }

    /** Generate a new traceId. */
    public static String generateTraceId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    private static final ThreadLocal<String> threadLocalTraceId = new ThreadLocal<>();

    /** gRPC Server interceptor: extracts traceId from metadata into Context. */
    public static ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                String traceId = headers.get(TRACE_ID_KEY);
                if (traceId == null || traceId.isBlank()) {
                    traceId = generateTraceId();
                }
                Context ctx = Context.current().withValue(TRACE_ID_CONTEXT_KEY, traceId);
                return Contexts.interceptCall(ctx, call, headers, next);
            }
        };
    }

    /** gRPC Client interceptor: injects current traceId into metadata. */
    public static ClientInterceptor clientInterceptor() {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                        next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        String traceId = currentTraceId();
                        if (traceId != null && !traceId.isBlank()) {
                            headers.put(TRACE_ID_KEY, traceId);
                        }
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }
}
