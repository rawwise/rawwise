package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.axiom.memory.Depth;
import io.github.rawwise.governance.auth.Principal;
import io.github.rawwise.governance.context.ContextEntry;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.governance.context.KnowledgeContext;
import io.github.rawwise.governance.context.L0Summary;
import io.github.rawwise.governance.context.SessionContext;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextRoutes {

    private static final Logger log = LoggerFactory.getLogger(ContextRoutes.class);

    private final ContextManager contextManager;

    public final Handler createSession;
    public final Handler getSession;
    public final Handler terminateSession;
    public final Handler putSessionEntry;
    public final Handler getSessionEntry;
    public final Handler listSessionEntries;
    public final Handler removeSessionEntry;
    public final Handler getEntryHistory;
    public final Handler getEntryAtTier;
    public final Handler scanL0;
    public final Handler createKnowledge;
    public final Handler getKnowledge;
    public final Handler listKnowledge;
    public final Handler deleteKnowledge;
    public final Handler putKnowledgeEntry;
    public final Handler getKnowledgeEntry;
    public final Handler listKnowledgeEntries;
    public final Handler removeKnowledgeEntry;
    public final Handler getKnowledgeEntryHistory;
    public final Handler getKnowledgeEntryAtTier;
    public final Handler scanKnowledgeL0;

    public ContextRoutes(ContextManager contextManager) {
        this.contextManager = contextManager;

        this.createSession =
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
                        @SuppressWarnings("unchecked")
                        Map<String, Object> body = ctx.bodyAsClass(Map.class);
                        long ttlMs =
                                body != null && body.get("ttlMs") instanceof Number num
                                        ? num.longValue()
                                        : 1800000L;
                        String agentId = principal.id();
                        String userId = getUserId(principal);
                        SessionContext session =
                                contextManager.createSession(agentId, userId, ttlMs);
                        ctx.status(201).json(session);
                    } catch (IOException e) {
                        log.error("Failed to create session", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to create session")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.getSession =
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
                        String sessionId = ctx.pathParam("sessionId");
                        Optional<SessionContext> session = contextManager.getSession(sessionId);
                        if (session.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Session not found: " + sessionId)));
                            return;
                        }
                        ctx.json(session.get());
                    } catch (IOException e) {
                        log.error("Failed to get session", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to get session")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.terminateSession =
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
                        String sessionId = ctx.pathParam("sessionId");
                        contextManager.terminateSession(sessionId);
                        ctx.json(Map.of("sessionId", sessionId, "status", "terminated"));
                    } catch (IOException e) {
                        log.error("Failed to terminate session", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to terminate session")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.putSessionEntry =
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
                        String sessionId = ctx.pathParam("sessionId");
                        @SuppressWarnings("unchecked")
                        Map<String, Object> body = ctx.bodyAsClass(Map.class);
                        if (body == null) {
                            ctx.status(400)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "INVALID_REQUEST",
                                                            "message",
                                                            "Request body is required")));
                            return;
                        }
                        String key = (String) body.get("key");
                        String value = (String) body.get("value");
                        String contentType =
                                (String) body.getOrDefault("contentType", "text/plain");
                        @SuppressWarnings("unchecked")
                        Map<String, String> metadata =
                                (Map<String, String>) body.getOrDefault("metadata", Map.of());
                        ContextEntry entry =
                                contextManager.putSessionEntry(
                                        sessionId, key, value, contentType, "utf-8", metadata);
                        ctx.status(201).json(entry);
                    } catch (IOException e) {
                        log.error("Failed to put session entry", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to put session entry")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.getSessionEntry =
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
                        String sessionId = ctx.pathParam("sessionId");
                        String key = ctx.pathParam("key");
                        Optional<ContextEntry> entry =
                                contextManager.getSessionEntry(sessionId, key);
                        if (entry.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Entry not found: " + key)));
                            return;
                        }
                        ctx.json(entry.get());
                    } catch (IOException e) {
                        log.error("Failed to get session entry", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to get session entry")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.listSessionEntries =
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
                        String sessionId = ctx.pathParam("sessionId");
                        List<ContextEntry> entries = contextManager.listSessionEntries(sessionId);
                        ctx.json(Map.of("entries", entries));
                    } catch (IOException e) {
                        log.error("Failed to list session entries", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to list session entries")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.removeSessionEntry =
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
                        String sessionId = ctx.pathParam("sessionId");
                        String key = ctx.pathParam("key");
                        boolean removed = contextManager.removeSessionEntry(sessionId, key);
                        if (!removed) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Entry not found: " + key)));
                            return;
                        }
                        ctx.json(Map.of("sessionId", sessionId, "key", key, "status", "removed"));
                    } catch (IOException e) {
                        log.error("Failed to remove session entry", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to remove session entry")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.getEntryHistory =
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
                        String sessionId = ctx.pathParam("sessionId");
                        String key = ctx.pathParam("key");
                        int limit = parseQueryInt(ctx, "limit", 10);
                        List<ContextEntry> history =
                                contextManager.getEntryHistory("session", sessionId, key, limit);
                        ctx.json(Map.of("history", history));
                    } catch (IOException e) {
                        log.error("Failed to get entry history", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to get entry history")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.getEntryAtTier =
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
                        String sessionId = ctx.pathParam("sessionId");
                        String key = ctx.pathParam("key");
                        String tierStr = ctx.pathParam("tier");
                        Depth depth = Depth.resolve(tierStr);
                        Optional<ContextEntry> entry =
                                contextManager.getEntryAtTier("session", sessionId, key, depth);
                        if (entry.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Entry not found at tier "
                                                                    + depth
                                                                    + ": "
                                                                    + key)));
                            return;
                        }
                        ctx.json(entry.get());
                    } catch (IOException e) {
                        log.error("Failed to get entry at tier", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to get entry at tier")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.scanL0 =
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
                        int limit = parseQueryInt(ctx, "limit", 50);
                        List<L0Summary> summaries =
                                contextManager.scanL0(principal.id(), "session", limit);
                        ctx.json(Map.of("summaries", summaries));
                    } catch (IOException e) {
                        log.error("Failed to scan L0", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to scan L0 summaries")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.createKnowledge =
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
                        @SuppressWarnings("unchecked")
                        Map<String, Object> body = ctx.bodyAsClass(Map.class);
                        if (body == null) {
                            ctx.status(400)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "INVALID_REQUEST",
                                                            "message",
                                                            "Request body is required")));
                            return;
                        }
                        String domain = (String) body.getOrDefault("domain", "");
                        String description = (String) body.getOrDefault("description", "");
                        String scopeStr = (String) body.getOrDefault("scope", "PRIVATE");
                        KnowledgeContext.KnowledgeScope scope =
                                KnowledgeContext.KnowledgeScope.valueOf(scopeStr);
                        @SuppressWarnings("unchecked")
                        List<String> sharedAgents =
                                (List<String>) body.getOrDefault("sharedAgents", List.of());
                        String agentId = principal.id();
                        String userId = getUserId(principal);
                        KnowledgeContext knowledge =
                                contextManager.createKnowledge(
                                        agentId, userId, domain, description, scope, sharedAgents);
                        ctx.status(201).json(knowledge);
                    } catch (IOException e) {
                        log.error("Failed to create knowledge", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to create knowledge")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.getKnowledge =
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
                        String knowledgeId = ctx.pathParam("knowledgeId");
                        Optional<KnowledgeContext> knowledge =
                                contextManager.getKnowledge(knowledgeId);
                        if (knowledge.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Knowledge not found: "
                                                                    + knowledgeId)));
                            return;
                        }
                        ctx.json(knowledge.get());
                    } catch (IOException e) {
                        log.error("Failed to get knowledge", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to get knowledge")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.listKnowledge =
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
                        List<KnowledgeContext> agentKnowledge =
                                contextManager.listKnowledgeByAgent(principal.id());
                        List<KnowledgeContext> publicKnowledge =
                                contextManager.listPublicKnowledge();
                        Map<String, KnowledgeContext> merged = new LinkedHashMap<>();
                        for (KnowledgeContext k : agentKnowledge) {
                            merged.put(k.knowledgeId(), k);
                        }
                        for (KnowledgeContext k : publicKnowledge) {
                            merged.putIfAbsent(k.knowledgeId(), k);
                        }
                        ctx.json(Map.of("knowledge", new ArrayList<>(merged.values())));
                    } catch (IOException e) {
                        log.error("Failed to list knowledge", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to list knowledge")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.deleteKnowledge =
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
                        String knowledgeId = ctx.pathParam("knowledgeId");
                        boolean deleted = contextManager.deleteKnowledge(knowledgeId);
                        if (!deleted) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Knowledge not found: "
                                                                    + knowledgeId)));
                            return;
                        }
                        ctx.json(Map.of("knowledgeId", knowledgeId, "status", "deleted"));
                    } catch (IOException e) {
                        log.error("Failed to delete knowledge", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to delete knowledge")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.putKnowledgeEntry =
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
                        String knowledgeId = ctx.pathParam("knowledgeId");
                        @SuppressWarnings("unchecked")
                        Map<String, Object> body = ctx.bodyAsClass(Map.class);
                        if (body == null) {
                            ctx.status(400)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "INVALID_REQUEST",
                                                            "message",
                                                            "Request body is required")));
                            return;
                        }
                        String key = (String) body.get("key");
                        String value = (String) body.get("value");
                        String contentType =
                                (String) body.getOrDefault("contentType", "text/plain");
                        @SuppressWarnings("unchecked")
                        Map<String, String> metadata =
                                (Map<String, String>) body.getOrDefault("metadata", Map.of());
                        ContextEntry entry =
                                contextManager.putKnowledgeEntry(
                                        knowledgeId, key, value, contentType, "utf-8", metadata);
                        ctx.status(201).json(entry);
                    } catch (IOException e) {
                        log.error("Failed to put knowledge entry", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to put knowledge entry")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.getKnowledgeEntry =
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
                        String knowledgeId = ctx.pathParam("knowledgeId");
                        String key = ctx.pathParam("key");
                        Optional<ContextEntry> entry =
                                contextManager.getKnowledgeEntry(knowledgeId, key);
                        if (entry.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Entry not found: " + key)));
                            return;
                        }
                        ctx.json(entry.get());
                    } catch (IOException e) {
                        log.error("Failed to get knowledge entry", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to get knowledge entry")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.listKnowledgeEntries =
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
                        String knowledgeId = ctx.pathParam("knowledgeId");
                        List<ContextEntry> entries =
                                contextManager.listKnowledgeEntries(knowledgeId);
                        ctx.json(Map.of("entries", entries));
                    } catch (IOException e) {
                        log.error("Failed to list knowledge entries", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to list knowledge entries")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.removeKnowledgeEntry =
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
                        String knowledgeId = ctx.pathParam("knowledgeId");
                        String key = ctx.pathParam("key");
                        boolean removed = contextManager.removeKnowledgeEntry(knowledgeId, key);
                        if (!removed) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Entry not found: " + key)));
                            return;
                        }
                        ctx.json(
                                Map.of(
                                        "knowledgeId",
                                        knowledgeId,
                                        "key",
                                        key,
                                        "status",
                                        "removed"));
                    } catch (IOException e) {
                        log.error("Failed to remove knowledge entry", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to remove knowledge entry")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.getKnowledgeEntryHistory =
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
                        String knowledgeId = ctx.pathParam("knowledgeId");
                        String key = ctx.pathParam("key");
                        int limit = parseQueryInt(ctx, "limit", 10);
                        List<ContextEntry> history =
                                contextManager.getEntryHistory(
                                        "knowledge", knowledgeId, key, limit);
                        ctx.json(Map.of("history", history));
                    } catch (IOException e) {
                        log.error("Failed to get knowledge entry history", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to get knowledge entry history")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.getKnowledgeEntryAtTier =
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
                        String knowledgeId = ctx.pathParam("knowledgeId");
                        String key = ctx.pathParam("key");
                        String tierStr = ctx.pathParam("tier");
                        Depth depth = Depth.resolve(tierStr);
                        Optional<ContextEntry> entry =
                                contextManager.getEntryAtTier("knowledge", knowledgeId, key, depth);
                        if (entry.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Entry not found at tier "
                                                                    + depth
                                                                    + ": "
                                                                    + key)));
                            return;
                        }
                        ctx.json(entry.get());
                    } catch (IOException e) {
                        log.error("Failed to get knowledge entry at tier", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to get knowledge entry at tier")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

        this.scanKnowledgeL0 =
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
                        int limit = parseQueryInt(ctx, "limit", 50);
                        List<L0Summary> summaries =
                                contextManager.scanL0(principal.id(), "knowledge", limit);
                        ctx.json(Map.of("summaries", summaries));
                    } catch (IOException e) {
                        log.error("Failed to scan knowledge L0", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to scan knowledge L0 summaries")));
                    } catch (IllegalArgumentException | IllegalStateException e) {
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

    private static String getUserId(Principal principal) {
        if (principal instanceof Principal.AgentPrincipal agentPrincipal) {
            return agentPrincipal.userId();
        }
        return principal.id();
    }

    private static int parseQueryInt(Context ctx, String param, int defaultValue) {
        String val = ctx.queryParam(param);
        if (val == null || val.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
