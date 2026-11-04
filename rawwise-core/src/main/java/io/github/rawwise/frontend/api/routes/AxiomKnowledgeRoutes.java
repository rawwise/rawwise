package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.axiom.memory.AxiomMemoryFacade;
import io.github.rawwise.axiom.memory.Depth;
import io.github.rawwise.axiom.memory.MemoryEntry;
import io.github.rawwise.axiom.memory.MemoryFacade;
import io.github.rawwise.axiom.memory.MemoryTier;
import io.github.rawwise.governance.auth.Principal;
import io.github.rawwise.proto.axiom.AxiomAnnotateRequest;
import io.github.rawwise.proto.axiom.AxiomAnnotateResponse;
import io.github.rawwise.proto.axiom.AxiomIngestRequest;
import io.github.rawwise.proto.axiom.AxiomIngestResponse;
import io.github.rawwise.proto.axiom.AxiomProvenanceRequest;
import io.github.rawwise.proto.axiom.AxiomProvenanceResponse;
import io.github.rawwise.proto.axiom.AxiomRederiveRequest;
import io.github.rawwise.proto.axiom.AxiomRederiveResponse;
import io.github.rawwise.proto.axiom.AxiomSearchRequest;
import io.github.rawwise.proto.axiom.AxiomSearchResponse;
import io.github.rawwise.proto.axiom.AxiomServiceGrpc;
import io.github.rawwise.proto.axiom.ImplicitFeedbackRequest;
import io.github.rawwise.proto.axiom.RederiveScope;
import io.javalin.Javalin;
import io.javalin.http.Handler;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AxiomKnowledgeRoutes {

    private static final boolean MEMORY_IN_SEARCH_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.memory.in-search.enabled", "false"));
    private static final long MEMORY_RECALL_TIMEOUT_MS =
            Long.parseLong(System.getProperty("rawwise.axiom.memory.in-search.timeout-ms", "50"));
    private static final int MEMORY_EXPANSION_TOPK = 3;

    private final AxiomServiceGrpc.AxiomServiceBlockingStub axiomStub;
    private final MemoryFacade memoryFacade;

    public final Handler ingest;
    public final Handler search;
    public final Handler annotate;
    public final Handler getProvenance;
    public final Handler recordImplicitFeedback;
    public final Handler rederive;
    public final Handler fetchDetails;
    public final Handler cognize;

    public AxiomKnowledgeRoutes(
            AxiomServiceGrpc.AxiomServiceBlockingStub axiomStub, MemoryFacade memoryFacade) {
        this.axiomStub = axiomStub;
        this.memoryFacade = memoryFacade;

        this.ingest =
                ctx -> {
                    // Axiom paths must not route back through MemoryFacade (recursion guard).
                    // The wired AxiomMemoryFacade calls back into /api/axiom/* over HTTP, so
                    // calling memoryFacade from these handlers would self-recurse.
                    log.debug("axiom ingest path, no memory recursion");
                    try {
                        var body = ctx.bodyAsClass(IngestBody.class);
                        if (body.tableId() == null || body.tableId().isBlank()) {
                            ctx.status(400).json(Map.of("error", "tableId is required"));
                            return;
                        }
                        if (body.key() == null || body.key().isBlank()) {
                            ctx.status(400).json(Map.of("error", "key is required"));
                            return;
                        }
                        AxiomIngestRequest request =
                                AxiomIngestRequest.newBuilder()
                                        .setTableId(body.tableId())
                                        .setKey(body.key())
                                        .setBody(
                                                com.google.protobuf.ByteString.copyFromUtf8(
                                                        body.body() != null ? body.body() : ""))
                                        .setImmutable(body.immutable())
                                        .putAllProperties(
                                                body.properties() != null
                                                        ? body.properties()
                                                        : Map.of())
                                        .build();
                        AxiomIngestResponse response = axiomStub.ingest(request);
                        ctx.status(201)
                                .json(
                                        Map.of(
                                                "recordKey", response.getRecordKey(),
                                                "chunkCount", response.getChunkCount(),
                                                "status", response.getStatus()));
                    } catch (Exception e) {
                        String msg =
                                e.getMessage() != null
                                        ? e.getMessage()
                                        : e.getClass().getSimpleName();
                        log.error("Axiom ingest failed: {}", msg, e);
                        ctx.status(errorStatus(e)).json(Map.of("error", msg));
                    }
                };

        this.search =
                ctx -> {
                    // C4/B1 tradeoff: the HTTP-backed AxiomMemoryFacade routes recall back to
                    // /api/axiom/search, so expanding the query via memoryFacade here would
                    // self-recurse. Skip in-search expansion for HTTP-backed facades; a future
                    // gRPC-backed AxiomClient (AxiomGrpcClient) can restore this path untouched.
                    log.debug("axiom search path, no memory recursion");
                    try {
                        var body = ctx.bodyAsClass(SearchBody.class);
                        String effectiveQuery = body.query();
                        if (MEMORY_IN_SEARCH_ENABLED
                                && !(memoryFacade instanceof AxiomMemoryFacade)
                                && memoryFacade != null
                                && effectiveQuery != null
                                && !effectiveQuery.isBlank()) {
                            String agentId = "default";
                            try {
                                Principal principal = ctx.attribute("principal");
                                if (principal != null) {
                                    agentId = principal.id();
                                }
                            } catch (Exception e) {
                                log.debug("Memory recall fallback", e);
                            }
                            effectiveQuery = expandQueryWithMemory(effectiveQuery, agentId);
                        }
                        AxiomSearchRequest.Builder requestBuilder =
                                AxiomSearchRequest.newBuilder()
                                        .setTableId(body.tableId())
                                        .setQuery(effectiveQuery)
                                        .setTopK(body.topK() > 0 ? body.topK() : 10)
                                        .setSnapshotId(body.snapshotId());
                        if (body.filter() != null && !body.filter().isEmpty()) {
                            requestBuilder.putAllFilter(body.filter());
                        }
                        AxiomSearchRequest request = requestBuilder.build();
                        AxiomSearchResponse response = axiomStub.search(request);

                        List<Map<String, Object>> results =
                                response.getResultsList().stream()
                                        .<Map<String, Object>>map(
                                                r -> {
                                                    Map<String, Object> result =
                                                            new java.util.LinkedHashMap<>();
                                                    result.put("viewId", r.getViewId());
                                                    result.put("score", r.getScore());
                                                    result.put("summary", r.getSummary());
                                                    if (!r.getPropertiesMap().isEmpty()) {
                                                        result.put(
                                                                "properties", r.getPropertiesMap());
                                                    }
                                                    if (!r.getRawText().isEmpty()) {
                                                        result.put("body", r.getRawText());
                                                    }
                                                    if (r.hasProvenance()) {
                                                        java.util.LinkedHashMap<String, Object>
                                                                prov =
                                                                        new java.util
                                                                                .LinkedHashMap<>();
                                                        prov.put(
                                                                "rawKey",
                                                                r.getProvenance().getRawKey());
                                                        prov.put(
                                                                "chain",
                                                                r
                                                                        .getProvenance()
                                                                        .getChainList()
                                                                        .stream()
                                                                        .map(
                                                                                l -> {
                                                                                    java.util
                                                                                                            .LinkedHashMap<
                                                                                                    String,
                                                                                                    Object>
                                                                                            link =
                                                                                                    new java
                                                                                                            .util
                                                                                                            .LinkedHashMap<>();
                                                                                    link.put(
                                                                                            "from",
                                                                                            l
                                                                                                    .getFrom());
                                                                                    link.put(
                                                                                            "fromLayer",
                                                                                            l
                                                                                                    .getFromLayer());
                                                                                    link.put(
                                                                                            "to",
                                                                                            l
                                                                                                    .getTo());
                                                                                    link.put(
                                                                                            "toLayer",
                                                                                            l
                                                                                                    .getToLayer());
                                                                                    return link;
                                                                                })
                                                                        .toList());
                                                        result.put("provenance", prov);
                                                    }
                                                    return result;
                                                })
                                        .toList();

                        ctx.json(Map.of("results", results));
                    } catch (Exception e) {
                        String msg =
                                e.getMessage() != null
                                        ? e.getMessage()
                                        : e.getClass().getSimpleName();
                        log.error("Axiom search failed: {}", msg, e);
                        ctx.status(errorStatus(e)).json(Map.of("error", msg));
                    }
                };

        this.annotate =
                ctx -> {
                    try {
                        var body = ctx.bodyAsClass(AnnotateBody.class);
                        AxiomAnnotateRequest request =
                                AxiomAnnotateRequest.newBuilder()
                                        .setViewId(body.viewId())
                                        .setType(body.type())
                                        .setValue(body.value())
                                        .setAnnotatorId(body.annotatorId())
                                        .setConfidence(body.confidence())
                                        .build();
                        AxiomAnnotateResponse response = axiomStub.annotate(request);
                        ctx.json(
                                Map.of(
                                        "success", response.getSuccess(),
                                        "adjustedScore", response.getAdjustedScore()));
                    } catch (Exception e) {
                        String msg =
                                e.getMessage() != null
                                        ? e.getMessage()
                                        : e.getClass().getSimpleName();
                        log.error("Axiom annotate failed: {}", msg, e);
                        ctx.status(errorStatus(e)).json(Map.of("error", msg));
                    }
                };

        this.getProvenance =
                ctx -> {
                    try {
                        String viewId = ctx.pathParam("viewId");
                        AxiomProvenanceRequest request =
                                AxiomProvenanceRequest.newBuilder().setViewId(viewId).build();
                        AxiomProvenanceResponse response = axiomStub.getProvenance(request);
                        java.util.LinkedHashMap<String, Object> result =
                                new java.util.LinkedHashMap<>();
                        result.put("viewId", response.getViewId());
                        result.put("rawKey", response.getRawKey());
                        result.put(
                                "fullChain",
                                response.getFullChainList().stream()
                                        .map(
                                                l -> {
                                                    java.util.LinkedHashMap<String, Object> link =
                                                            new java.util.LinkedHashMap<>();
                                                    link.put("from", l.getFrom());
                                                    link.put("fromLayer", l.getFromLayer());
                                                    link.put("to", l.getTo());
                                                    link.put("toLayer", l.getToLayer());
                                                    return link;
                                                })
                                        .toList());
                        ctx.json(result);
                    } catch (Exception e) {
                        String msg =
                                e.getMessage() != null
                                        ? e.getMessage()
                                        : e.getClass().getSimpleName();
                        log.error("Axiom getProvenance failed: {}", msg, e);
                        ctx.status(errorStatus(e)).json(Map.of("error", msg));
                    }
                };

        this.recordImplicitFeedback =
                ctx -> {
                    try {
                        var body = ctx.bodyAsClass(ImplicitFeedbackBody.class);
                        ImplicitFeedbackRequest request =
                                ImplicitFeedbackRequest.newBuilder()
                                        .setViewId(body.viewId() != null ? body.viewId() : "")
                                        .setSignalType(
                                                body.signalType() != null ? body.signalType() : "")
                                        .setDwellMs(body.dwellMs())
                                        .build();
                        axiomStub
                                .withDeadlineAfter(5, TimeUnit.SECONDS)
                                .recordImplicitFeedback(request);
                    } catch (Exception e) {
                        log.warn(
                                "Implicit feedback recording failed, graceful skip: {}",
                                e.getMessage() != null
                                        ? e.getMessage()
                                        : e.getClass().getSimpleName());
                    }
                    ctx.status(204);
                };

        this.rederive =
                ctx -> {
                    try {
                        var body = ctx.bodyAsClass(RederiveBody.class);
                        if (body.tableId() == null || body.tableId().isBlank()) {
                            ctx.status(400).json(Map.of("error", "tableId is required"));
                            return;
                        }
                        AxiomRederiveRequest.Builder reqBuilder =
                                AxiomRederiveRequest.newBuilder()
                                        .setTableId(body.tableId())
                                        .setForceReindex(body.forceReindex());
                        RederiveScope scope = RederiveScope.FULL_TABLE;
                        if (body.scope() != null && !body.scope().isBlank()) {
                            try {
                                scope = RederiveScope.valueOf(body.scope().toUpperCase());
                            } catch (IllegalArgumentException e) {
                                ctx.status(400)
                                        .json(
                                                Map.of(
                                                        "error",
                                                        "Invalid scope: "
                                                                + body.scope()
                                                                + ", must be one of FULL_TABLE, KEY_RANGE, SINGLE_KEY"));
                                return;
                            }
                        }
                        reqBuilder.setScope(scope);
                        if (body.startKey() != null && !body.startKey().isBlank()) {
                            reqBuilder.setStartKey(body.startKey());
                        }
                        if (body.endKey() != null && !body.endKey().isBlank()) {
                            reqBuilder.setEndKey(body.endKey());
                        }
                        if (body.singleKey() != null && !body.singleKey().isBlank()) {
                            reqBuilder.setSingleKey(body.singleKey());
                        }
                        AxiomRederiveRequest request = reqBuilder.build();
                        AxiomRederiveResponse response =
                                axiomStub
                                        .withDeadlineAfter(120, TimeUnit.SECONDS)
                                        .rederive(request);
                        ctx.json(
                                Map.of(
                                        "totalKeys", response.getTotalKeys(),
                                        "rederivedCount", response.getRederivedCount(),
                                        "failedCount", response.getFailedCount(),
                                        "status", response.getStatus()));
                    } catch (Exception e) {
                        String msg =
                                e.getMessage() != null
                                        ? e.getMessage()
                                        : e.getClass().getSimpleName();
                        log.error("Axiom rederive failed: {}", msg, e);
                        ctx.status(errorStatus(e)).json(Map.of("error", msg));
                    }
                };

        this.fetchDetails =
                ctx -> {
                    try {
                        var body = ctx.bodyAsClass(FetchDetailsBody.class);
                        if (body.tableId() == null || body.tableId().isBlank()) {
                            ctx.status(400).json(Map.of("error", "tableId is required"));
                            return;
                        }
                        if (body.entryKeys() == null || body.entryKeys().isEmpty()) {
                            ctx.status(400).json(Map.of("error", "entryKeys must not be empty"));
                            return;
                        }
                        List<Map<String, Object>> results = new java.util.ArrayList<>();
                        for (String key : body.entryKeys()) {
                            AxiomSearchRequest request =
                                    AxiomSearchRequest.newBuilder()
                                            .setTableId(body.tableId())
                                            .setQuery(key)
                                            .setTopK(1)
                                            .setSnapshotId(0)
                                            .build();
                            AxiomSearchResponse response = axiomStub.search(request);
                            for (var item : response.getResultsList()) {
                                java.util.LinkedHashMap<String, Object> result =
                                        new java.util.LinkedHashMap<>();
                                result.put("key", key);
                                result.put("viewId", item.getViewId());
                                result.put("score", item.getScore());
                                result.put("summary", item.getSummary());
                                result.put("body", item.getRawText());
                                result.put("properties", item.getPropertiesMap());
                                results.add(result);
                            }
                        }
                        ctx.json(Map.of("results", results));
                    } catch (Exception e) {
                        String msg =
                                e.getMessage() != null
                                        ? e.getMessage()
                                        : e.getClass().getSimpleName();
                        log.error("Axiom fetch-details failed: {}", msg, e);
                        ctx.status(errorStatus(e)).json(Map.of("error", msg));
                    }
                };

        this.cognize =
                ctx -> {
                    try {
                        var body = ctx.bodyAsClass(CognizeBody.class);
                        if (body.tableId() == null || body.tableId().isBlank()) {
                            ctx.status(400).json(Map.of("error", "tableId is required"));
                            return;
                        }
                        AxiomSearchRequest searchReq =
                                AxiomSearchRequest.newBuilder()
                                        .setTableId(body.tableId())
                                        .setQuery(body.query())
                                        .setTopK(body.topK() > 0 ? body.topK() : 5)
                                        .setSnapshotId(0)
                                        .build();
                        AxiomSearchResponse searchResp = axiomStub.search(searchReq);
                        List<Map<String, Object>> cognitiveResults = new java.util.ArrayList<>();
                        for (var item : searchResp.getResultsList()) {
                            java.util.LinkedHashMap<String, Object> result =
                                    new java.util.LinkedHashMap<>();
                            result.put("viewId", item.getViewId());
                            result.put("score", item.getScore());
                            result.put("summary", item.getSummary());
                            List<Map<String, Object>> layers = new java.util.ArrayList<>();
                            try {
                                AxiomProvenanceRequest provReq =
                                        AxiomProvenanceRequest.newBuilder()
                                                .setViewId(item.getViewId())
                                                .build();
                                AxiomProvenanceResponse provResp = axiomStub.getProvenance(provReq);
                                for (var link : provResp.getFullChainList()) {
                                    java.util.LinkedHashMap<String, Object> layer =
                                            new java.util.LinkedHashMap<>();
                                    layer.put("from", link.getFrom());
                                    layer.put("fromLayer", link.getFromLayer());
                                    layer.put("to", link.getTo());
                                    layer.put("toLayer", link.getToLayer());
                                    layers.add(layer);
                                }
                                if (!provResp.getRawKey().isEmpty()) {
                                    layers.add(
                                            Map.of("layer", "L0_raw", "id", provResp.getRawKey()));
                                }
                            } catch (Exception e) {
                                log.debug(
                                        "Provenance fetch failed for cognize: {}",
                                        item.getViewId(),
                                        e);
                            }
                            result.put("layers", layers);
                            cognitiveResults.add(result);
                        }
                        ctx.json(Map.of("query", body.query(), "results", cognitiveResults));
                    } catch (Exception e) {
                        String msg =
                                e.getMessage() != null
                                        ? e.getMessage()
                                        : e.getClass().getSimpleName();
                        log.error("Axiom cognize failed: {}", msg, e);
                        ctx.status(errorStatus(e)).json(Map.of("error", msg));
                    }
                };
    }

    /** Maps a backend exception (gRPC StatusRuntimeException or other) to an HTTP status. */
    private static int errorStatus(Exception e) {
        if (e instanceof io.grpc.StatusRuntimeException sre) {
            return switch (sre.getStatus().getCode()) {
                case INVALID_ARGUMENT, OUT_OF_RANGE -> 400;
                case FAILED_PRECONDITION, ALREADY_EXISTS -> 409;
                case NOT_FOUND -> 404;
                case PERMISSION_DENIED -> 403;
                case UNAUTHENTICATED -> 401;
                case UNIMPLEMENTED -> 501;
                case UNAVAILABLE -> 503;
                case DEADLINE_EXCEEDED -> 504;
                default -> 500;
            };
        }
        return 500;
    }

    private String expandQueryWithMemory(String query, String agentId) {
        CompletableFuture<List<MemoryEntry>> future =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return memoryFacade.recall(
                                        agentId,
                                        query,
                                        MEMORY_EXPANSION_TOPK,
                                        MemoryTier.SEMANTIC,
                                        Depth.DETAIL);
                            } catch (Exception e) {
                                log.debug(
                                        "Memory recall failed during query expansion: {}",
                                        e.getMessage());
                                return List.of();
                            }
                        });
        try {
            List<MemoryEntry> entries = future.get(MEMORY_RECALL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            String expansion = extractExpansionTerms(entries);
            if (expansion.isBlank()) {
                return query;
            }
            log.debug(
                    "Memory query expansion applied: original='{}' expansion='{}'",
                    query,
                    expansion);
            return query + " " + expansion;
        } catch (TimeoutException e) {
            future.cancel(true);
            log.warn(
                    "Memory recall timed out ({}ms) during query expansion, using original query",
                    MEMORY_RECALL_TIMEOUT_MS);
            return query;
        } catch (Exception e) {
            future.cancel(true);
            log.warn("Memory query expansion failed, using original query: {}", e.getMessage());
            return query;
        }
    }

    private String extractExpansionTerms(List<MemoryEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (MemoryEntry entry : entries) {
            String text = pickSummary(entry);
            if (text == null || text.isBlank()) {
                continue;
            }
            text = text.trim();
            if (text.length() > 60) {
                text = text.substring(0, 60);
            }
            sb.append(text).append(' ');
        }
        return sb.toString().trim();
    }

    private String pickSummary(MemoryEntry entry) {
        Object summary = entry.content().get("summary");
        if (summary != null && !summary.toString().isBlank()) {
            return summary.toString();
        }
        return null;
    }

    public void register(Javalin app) {
        app.post("/api/axiom/ingest", ingest)
                .post("/api/axiom/search", search)
                .post("/api/axiom/annotate", annotate)
                .get("/api/axiom/provenance/{viewId}", getProvenance)
                .post("/api/axiom/feedback/implicit", recordImplicitFeedback)
                .post("/api/axiom/rederive", rederive)
                .post("/api/axiom/fetch-details", fetchDetails)
                .post("/api/axiom/cognize", cognize);
    }

    private record IngestBody(
            String tableId,
            String key,
            String body,
            boolean immutable,
            Map<String, String> properties) {}

    private record SearchBody(
            String tableId, String query, int topK, long snapshotId, Map<String, String> filter) {}

    private record AnnotateBody(
            String viewId, String type, String value, String annotatorId, double confidence) {}

    private record ImplicitFeedbackBody(String viewId, String signalType, long dwellMs) {}

    private record RederiveBody(
            String tableId,
            String scope,
            String startKey,
            String endKey,
            String singleKey,
            boolean forceReindex) {}

    private record FetchDetailsBody(String tableId, List<String> entryKeys) {}

    private record CognizeBody(String tableId, String query, int topK) {}
}
