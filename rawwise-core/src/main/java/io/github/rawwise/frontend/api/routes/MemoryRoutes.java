package io.github.rawwise.frontend.api.routes;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.github.rawwise.axiom.memory.Depth;
import io.github.rawwise.axiom.memory.MemoryEntry;
import io.github.rawwise.axiom.memory.MemoryFacade;
import io.github.rawwise.axiom.memory.MemorySharePolicy;
import io.github.rawwise.axiom.memory.MemoryStatus;
import io.github.rawwise.axiom.memory.MemoryTier;
import io.github.rawwise.axiom.memory.exception.AxiomException;
import io.github.rawwise.governance.auth.Principal;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.javalin.Javalin;
import io.javalin.http.Handler;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class MemoryRoutes {

    private final MemoryFacade memoryFacade;

    public final Handler store;
    public final Handler recall;
    public final Handler consolidate;
    public final Handler decay;
    public final Handler reflect;
    public final Handler share;
    public final Handler status;
    public final Handler entries;
    public final Handler vectorConfig;

    public MemoryRoutes(MemoryFacade memoryFacade) {
        this.memoryFacade = memoryFacade;

        this.store =
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
                        var body = ctx.bodyAsClass(StoreRequest.class);
                        MemoryTier tier =
                                requireEnum(MemoryTier.class, body.memoryTier(), "memoryTier");
                        Map<String, Object> contentMap = normalizeContent(body.content());
                        MemoryEntry entry =
                                memoryFacade.store(
                                        principal.id(), contentMap, tier, body.importance());
                        ctx.status(201).json(entry);
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
                    } catch (Exception e) {
                        String detail = e.getClass().getName() + ": " + e.getMessage();
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        detail)));
                    }
                };

        this.recall =
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
                        var body = ctx.bodyAsClass(RecallRequest.class);
                        MemoryTier tier =
                                requireEnum(MemoryTier.class, body.memoryTier(), "memoryTier");
                        Depth depth = resolveDepth(body);
                        List<MemoryEntry> result =
                                memoryFacade.recall(
                                        principal.id(), body.query(), body.topK(), tier, depth);
                        ctx.json(Map.of("entries", result));
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

        this.consolidate =
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
                        var body = ctx.bodyAsClass(ConsolidateRequest.class);
                        MemoryTier fromTier =
                                requireEnum(MemoryTier.class, body.fromTier(), "fromTier");
                        MemoryTier toTier = requireEnum(MemoryTier.class, body.toTier(), "toTier");
                        memoryFacade.consolidate(principal.id(), fromTier, toTier);
                        ctx.json(Map.of("status", "ok"));
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

        this.decay =
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
                        var body = ctx.bodyAsClass(DecayRequest.class);
                        memoryFacade.decay(principal.id(), Duration.ofMillis(body.ageMs()));
                        ctx.json(Map.of("status", "ok"));
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

        this.reflect =
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
                        var body = ctx.bodyAsClass(ReflectRequest.class);
                        var entry = memoryFacade.reflect(principal.id(), body.query());
                        if (entry.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "No reflection result found")));
                            return;
                        }
                        ctx.json(entry.get());
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

        this.share =
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
                        var body = ctx.bodyAsClass(ShareRequest.class);
                        MemorySharePolicy policy =
                                new MemorySharePolicy(
                                        Duration.ofMillis(body.ttlMs()),
                                        body.revocable(),
                                        body.permission());
                        memoryFacade.share(
                                principal.id(), body.toAgentId(), body.entryId(), policy);
                        ctx.json(Map.of("status", "ok"));
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

        this.status =
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
                        MemoryStatus memoryStatus = memoryFacade.status(principal.id());
                        ctx.json(memoryStatus);
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

        this.entries =
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
                        String tierStr = ctx.queryParam("memoryTier");
                        MemoryTier tier =
                                tierStr != null ? MemoryTier.valueOf(tierStr.toUpperCase()) : null;
                        int offset = parseIntParam(ctx.queryParam("offset"), "offset", 0);
                        int limit = parseIntParam(ctx.queryParam("limit"), "limit", 20);
                        List<MemoryEntry> result =
                                memoryFacade.listEntries(principal.id(), tier, offset, limit);
                        ctx.json(Map.of("entries", result));
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

        this.vectorConfig =
                ctx -> {
                    boolean simdEnabled = false;
                    try {
                        Class<?> vectorSpeciesClass =
                                Class.forName("jdk.incubator.vector.VectorSpecies");
                        simdEnabled = true;
                    } catch (ClassNotFoundException ignored) {
                    }

                    ctx.json(
                            Map.of(
                                    "engine", RawWiseOptions.VECTOR_ENGINE.key(),
                                    "mode", RawWiseOptions.VECTOR_JVECTOR_MODE.key(),
                                    "dimension", RawWiseOptions.VECTOR_JVECTOR_DIMENSION.key(),
                                    "simdEnabled", simdEnabled,
                                    "buildThreshold",
                                            RawWiseOptions.VECTOR_JVECTOR_BUILD_THRESHOLD.key()));
                };
    }

    public void register(Javalin app) {
        app.post("/api/v1/axiom/memory/store", store)
                .post("/api/v1/axiom/memory/recall", recall)
                .post("/api/v1/axiom/memory/consolidate", consolidate)
                .post("/api/v1/axiom/memory/decay", decay)
                .post("/api/v1/axiom/memory/reflect", reflect)
                .post("/api/v1/axiom/memory/share", share)
                .get("/api/v1/axiom/memory/status", status)
                .get("/api/v1/axiom/memory/entries", entries)
                .get("/api/v1/axiom/memory/vector/config", vectorConfig);
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

    private static <T extends Enum<T>> T requireEnum(
            Class<T> enumClass, String value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        return Enum.valueOf(enumClass, value.toUpperCase());
    }

    /**
     * Accepts either a String (legacy / smoke-test shape) or a Map (rich shape). String is wrapped
     * as {@code {"summary": <text>}} so downstream {@link MemoryFacade#store} always sees a Map.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeContent(Object content) {
        if (content == null) {
            throw new IllegalArgumentException("content is required");
        }
        if (content instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        if (content instanceof String s) {
            return Map.of("summary", s);
        }
        throw new IllegalArgumentException(
                "content must be either a JSON string or a JSON object, got: "
                        + content.getClass().getSimpleName());
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record StoreRequest(Object content, String memoryTier, double importance) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record RecallRequest(
            String query, int topK, String memoryTier, String depth, String loadTier) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ConsolidateRequest(String fromTier, String toTier) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record DecayRequest(long ageMs) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ReflectRequest(String query) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ShareRequest(
            String toAgentId, String entryId, long ttlMs, boolean revocable, String permission) {}

    private static Depth resolveDepth(RecallRequest body) {
        // 统一走 Depth.resolve 兼容映射（缺省→ABSTRACT、L0→ABSTRACT、L1/L2→DETAIL）
        String raw = body.depth();
        if ((raw == null || raw.isBlank())
                && body.loadTier() != null
                && !body.loadTier().isBlank()) {
            raw = body.loadTier();
        }
        return Depth.resolve(raw);
    }
}
