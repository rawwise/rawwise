package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.axiom.memory.exception.AxiomException;
import io.github.rawwise.axiom.memory.exception.StateEntryNotFoundException;
import io.github.rawwise.axiom.memory.exception.StateSpaceNotFoundException;
import io.github.rawwise.axiom.pipeline.state.StateEngine;
import io.github.rawwise.axiom.pipeline.state.StateEntry;
import io.github.rawwise.axiom.pipeline.state.StateSpace;
import io.github.rawwise.axiom.pipeline.state.StateTransition;
import io.github.rawwise.governance.auth.Principal;
import io.javalin.Javalin;
import io.javalin.http.Handler;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StateRoutes {

    private final StateEngine stateEngine;

    public final Handler createSpace;
    public final Handler listSpaces;
    public final Handler getSpace;
    public final Handler deleteSpace;
    public final Handler readEntry;
    public final Handler writeEntry;
    public final Handler transition;
    public final Handler scanEntries;

    public StateRoutes(StateEngine stateEngine) {
        this.stateEngine = stateEngine;

        this.createSpace =
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
                        var body = ctx.bodyAsClass(CreateSpaceRequest.class);
                        StateSpace space =
                                stateEngine.createStateSpace(
                                        principal.id(),
                                        body.name(),
                                        body.initialState(),
                                        body.states(),
                                        body.transitions(),
                                        body.metadata());
                        ctx.status(201).json(space);
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
                    } catch (IOException e) {
                        throw wrapStateIoException(e);
                    }
                };

        this.listSpaces =
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
                        List<StateSpace> spaces =
                                stateEngine.listStateSpacesByAgent(principal.id());
                        ctx.json(Map.of("spaces", spaces));
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
                    } catch (IOException e) {
                        throw wrapStateIoException(e);
                    }
                };

        this.getSpace =
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
                        String spaceId = ctx.pathParam("spaceId");
                        StateSpace space =
                                stateEngine
                                        .getStateSpace(spaceId)
                                        .orElseThrow(
                                                () ->
                                                        new StateSpaceNotFoundException(
                                                                "StateSpace not found: "
                                                                        + spaceId));
                        if (!space.agentId().equals(principal.id())) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not authorized to access this resource")));
                            return;
                        }
                        ctx.json(space);
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
                    } catch (IOException e) {
                        throw wrapStateIoException(e);
                    }
                };

        this.deleteSpace =
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
                        String spaceId = ctx.pathParam("spaceId");
                        StateSpace space = stateEngine.getStateSpace(spaceId).orElse(null);
                        if (space == null) {
                            throw new StateSpaceNotFoundException(
                                    "StateSpace not found: " + spaceId);
                        }
                        if (!space.agentId().equals(principal.id())) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not authorized to access this resource")));
                            return;
                        }
                        boolean deleted = stateEngine.deleteStateSpace(spaceId);
                        if (!deleted) {
                            throw new StateSpaceNotFoundException(
                                    "StateSpace not found: " + spaceId);
                        }
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
                    } catch (IOException e) {
                        throw wrapStateIoException(e);
                    }
                };

        this.readEntry =
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
                        String spaceId = ctx.pathParam("spaceId");
                        StateSpace space = stateEngine.getStateSpace(spaceId).orElse(null);
                        if (space == null) {
                            throw new StateSpaceNotFoundException(
                                    "StateSpace not found: " + spaceId);
                        }
                        if (!space.agentId().equals(principal.id())) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not authorized to access this resource")));
                            return;
                        }
                        String key = ctx.pathParam("key");
                        StateEntry entry = stateEngine.read(spaceId, key);
                        if (entry == null) {
                            throw new StateEntryNotFoundException(
                                    "State entry not found: " + spaceId + "/" + key);
                        }
                        ctx.json(entry);
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
                    } catch (IOException e) {
                        throw wrapStateIoException(e);
                    }
                };

        this.writeEntry =
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
                        String spaceId = ctx.pathParam("spaceId");
                        StateSpace space = stateEngine.getStateSpace(spaceId).orElse(null);
                        if (space == null) {
                            throw new StateSpaceNotFoundException(
                                    "StateSpace not found: " + spaceId);
                        }
                        if (!space.agentId().equals(principal.id())) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not authorized to access this resource")));
                            return;
                        }
                        String key = ctx.pathParam("key");
                        var body = ctx.bodyAsClass(WriteEntryRequest.class);
                        StateEntry entry =
                                stateEngine.write(
                                        spaceId,
                                        key,
                                        body.value(),
                                        body.stateName(),
                                        body.metadata());
                        ctx.json(entry);
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
                    } catch (IOException e) {
                        throw wrapStateIoException(e);
                    }
                };

        this.transition =
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
                        String spaceId = ctx.pathParam("spaceId");
                        StateSpace space = stateEngine.getStateSpace(spaceId).orElse(null);
                        if (space == null) {
                            throw new StateSpaceNotFoundException(
                                    "StateSpace not found: " + spaceId);
                        }
                        if (!space.agentId().equals(principal.id())) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not authorized to access this resource")));
                            return;
                        }
                        var body = ctx.bodyAsClass(TransitionRequest.class);
                        StateEntry entry =
                                stateEngine.transition(
                                        spaceId, body.key(), body.toState(), body.metadata());
                        ctx.json(entry);
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
                    } catch (IOException e) {
                        throw wrapStateIoException(e);
                    }
                };

        this.scanEntries =
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
                        String spaceId = ctx.pathParam("spaceId");
                        StateSpace space = stateEngine.getStateSpace(spaceId).orElse(null);
                        if (space == null) {
                            throw new StateSpaceNotFoundException(
                                    "StateSpace not found: " + spaceId);
                        }
                        if (!space.agentId().equals(principal.id())) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not authorized to access this resource")));
                            return;
                        }
                        String prefix =
                                ctx.queryParam("prefix") != null ? ctx.queryParam("prefix") : "";
                        int limit = parseIntParam(ctx.queryParam("limit"), "limit", 100);
                        List<StateEntry> entries = stateEngine.scan(spaceId, prefix, limit);
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
                    } catch (IOException e) {
                        throw wrapStateIoException(e);
                    }
                };
    }

    public void register(Javalin app) {
        app.post("/api/v1/axiom/state/spaces", createSpace)
                .get("/api/v1/axiom/state/spaces", listSpaces)
                .get("/api/v1/axiom/state/spaces/{spaceId}", getSpace)
                .delete("/api/v1/axiom/state/spaces/{spaceId}", deleteSpace)
                .get("/api/v1/axiom/state/spaces/{spaceId}/entries/{key}", readEntry)
                .put("/api/v1/axiom/state/spaces/{spaceId}/entries/{key}", writeEntry)
                .post("/api/v1/axiom/state/spaces/{spaceId}/transition", transition)
                .get("/api/v1/axiom/state/spaces/{spaceId}/entries", scanEntries);
    }

    private static AxiomException wrapStateIoException(IOException e) {
        return new AxiomException(
                "AXIOM_STATE_IO_ERROR", 500, "State engine IO error: " + e.getMessage(), e);
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

    private record CreateSpaceRequest(
            String name,
            String initialState,
            List<String> states,
            List<StateTransition> transitions,
            Map<String, String> metadata) {}

    private record WriteEntryRequest(
            String value, String stateName, Map<String, String> metadata) {}

    private record TransitionRequest(String key, String toState, Map<String, String> metadata) {}
}
