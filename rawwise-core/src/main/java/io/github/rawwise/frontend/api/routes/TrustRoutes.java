package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.axiom.memory.exception.AxiomException;
import io.github.rawwise.governance.auth.Principal;
import io.github.rawwise.governance.trust.AuditEntry;
import io.github.rawwise.governance.trust.ProvenanceChain;
import io.github.rawwise.governance.trust.TrustEngine;
import io.github.rawwise.governance.trust.TrustStatus;
import io.javalin.Javalin;
import io.javalin.http.Handler;
import java.util.List;
import java.util.Map;

public class TrustRoutes {

    private final TrustEngine trustEngine;

    public final Handler getProvenance;
    public final Handler queryAudit;
    public final Handler getStatus;

    public TrustRoutes(TrustEngine trustEngine) {
        this.trustEngine = trustEngine;

        this.getProvenance =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }

                    try {
                        String id = ctx.pathParam("id");
                        ProvenanceChain chain = trustEngine.getProvenance(id);
                        if (chain == null) {
                            throw new io.github.rawwise.axiom.memory.exception
                                    .TrustProvenanceNotFoundException(
                                    "Provenance chain not found: " + id);
                        }
                        ctx.json(chain);
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.queryAudit =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }

                    try {
                        long fromTs = parseLongParam(ctx.queryParam("fromTs"), "fromTs", 0L);
                        long toTs =
                                parseLongParam(
                                        ctx.queryParam("toTs"), "toTs", System.currentTimeMillis());
                        int limit = parseIntParam(ctx.queryParam("limit"), "limit", 100);
                        List<AuditEntry> entries =
                                trustEngine.queryAudit(principal.id(), fromTs, toTs, limit);
                        ctx.json(Map.of("entries", entries));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.getStatus =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }

                    try {
                        TrustStatus status = trustEngine.getStatus();
                        ctx.json(status);
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };
    }

    public void register(Javalin app) {
        app.get("/api/v1/axiom/trust/provenance/{id}", getProvenance)
                .get("/api/v1/axiom/trust/audit", queryAudit)
                .get("/api/v1/axiom/trust/status", getStatus);
    }

    private static int parseIntParam(String value, String paramName, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for " + paramName + ": " + value);
        }
    }

    private static long parseLongParam(String value, String paramName, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for " + paramName + ": " + value);
        }
    }
}
