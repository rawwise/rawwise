package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.axiom.memory.exception.AxiomException;
import io.github.rawwise.axiom.memory.semantic.ExtractionPolicy;
import io.github.rawwise.axiom.memory.semantic.ExtractionResult;
import io.github.rawwise.axiom.memory.semantic.GraphEdge;
import io.github.rawwise.axiom.memory.semantic.GraphEdgeNotFoundException;
import io.github.rawwise.axiom.memory.semantic.GraphEngine;
import io.github.rawwise.axiom.memory.semantic.GraphNode;
import io.github.rawwise.axiom.memory.semantic.GraphNodeNotFoundException;
import io.github.rawwise.axiom.memory.semantic.GraphNotFoundException;
import io.github.rawwise.axiom.memory.semantic.GraphSchema;
import io.github.rawwise.axiom.memory.semantic.PathResult;
import io.github.rawwise.axiom.memory.semantic.TraversalDirection;
import io.github.rawwise.axiom.memory.semantic.VectorSearchHit;
import io.github.rawwise.governance.auth.Principal;
import io.javalin.Javalin;
import io.javalin.http.Handler;
import java.util.List;
import java.util.Map;

public class GraphRoutes {

    private final GraphEngine graphEngine;

    public final Handler createGraph;
    public final Handler listGraphs;
    public final Handler getGraph;
    public final Handler deleteGraph;
    public final Handler addNode;
    public final Handler getNode;
    public final Handler updateNode;
    public final Handler removeNode;
    public final Handler queryNodes;
    public final Handler addEdge;
    public final Handler getEdge;
    public final Handler updateEdge;
    public final Handler removeEdge;
    public final Handler getOutEdges;
    public final Handler getInEdges;
    public final Handler neighbors;
    public final Handler findPaths;
    public final Handler vectorSearch;
    public final Handler extractFromContext;

    public GraphRoutes(GraphEngine graphEngine) {
        this.graphEngine = graphEngine;

        this.createGraph =
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
                        var body = ctx.bodyAsClass(CreateGraphRequest.class);
                        GraphSchema schema =
                                graphEngine.createGraph(
                                        principal.id(),
                                        body.name(),
                                        body.description(),
                                        body.nodeTypes(),
                                        body.edgeLabels(),
                                        body.metadata());
                        ctx.status(201).json(schema);
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

        this.listGraphs =
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
                        List<GraphSchema> graphs = graphEngine.listGraphsByAgent(principal.id());
                        ctx.json(Map.of("graphs", graphs));
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

        this.getGraph =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema =
                                graphEngine
                                        .getGraph(graphId)
                                        .orElseThrow(() -> new GraphNotFoundException(graphId));
                        if (!isGraphOwner(schema, principal)) {
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
                        ctx.json(schema);
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

        this.deleteGraph =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        boolean deleted = graphEngine.deleteGraph(graphId);
                        if (!deleted) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
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
                    }
                };

        this.addNode =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        var body = ctx.bodyAsClass(AddNodeRequest.class);
                        GraphNode node =
                                graphEngine.addNode(graphId, body.type(), body.properties());
                        ctx.status(201).json(node);
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

        this.getNode =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        String nodeId = ctx.pathParam("nodeId");
                        GraphNode node =
                                graphEngine
                                        .getNode(graphId, nodeId)
                                        .orElseThrow(
                                                () ->
                                                        new GraphNodeNotFoundException(
                                                                graphId, nodeId));
                        ctx.json(node);
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

        this.updateNode =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        var req = ctx.bodyAsClass(UpdateNodeRequest.class);
                        GraphNode node =
                                new GraphNode(
                                        req.nodeId(),
                                        null,
                                        req.type(),
                                        req.properties(),
                                        0,
                                        0f,
                                        null,
                                        null,
                                        null,
                                        0,
                                        0,
                                        0);
                        GraphNode updated = graphEngine.updateNode(graphId, node);
                        ctx.json(updated);
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

        this.removeNode =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        String nodeId = ctx.pathParam("nodeId");
                        boolean removed = graphEngine.removeNode(graphId, nodeId);
                        if (!removed) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NODE_NOT_FOUND",
                                                            "message",
                                                            "Node not found: " + nodeId)));
                            return;
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
                    }
                };

        this.queryNodes =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        String type = ctx.queryParam("type");
                        int limit = parseIntParam(ctx.queryParam("limit"), "limit", 100);
                        Map<String, String> propertyFilter =
                                ctx.bodyAsClass(PropertyFilterRequest.class).propertyFilter();
                        List<GraphNode> nodes =
                                graphEngine.queryNodes(graphId, type, propertyFilter, limit);
                        ctx.json(Map.of("nodes", nodes));
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

        this.addEdge =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        var body = ctx.bodyAsClass(AddEdgeRequest.class);
                        GraphEdge edge =
                                graphEngine.addEdge(
                                        graphId,
                                        body.srcNodeId(),
                                        body.label(),
                                        body.dstNodeId(),
                                        body.properties(),
                                        body.weight());
                        ctx.status(201).json(edge);
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

        this.getEdge =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        String edgeId = ctx.pathParam("edgeId");
                        GraphEdge edge =
                                graphEngine
                                        .getEdge(graphId, edgeId)
                                        .orElseThrow(
                                                () ->
                                                        new GraphEdgeNotFoundException(
                                                                graphId, edgeId));
                        ctx.json(edge);
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

        this.updateEdge =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        var req = ctx.bodyAsClass(UpdateEdgeRequest.class);
                        GraphEdge edge =
                                new GraphEdge(
                                        req.edgeId(),
                                        null,
                                        req.srcNodeId(),
                                        req.label(),
                                        req.dstNodeId(),
                                        req.properties(),
                                        req.weight(),
                                        0,
                                        0,
                                        0);
                        GraphEdge updated = graphEngine.updateEdge(graphId, edge);
                        ctx.json(updated);
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

        this.removeEdge =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        String edgeId = ctx.pathParam("edgeId");
                        boolean removed = graphEngine.removeEdge(graphId, edgeId);
                        if (!removed) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_EDGE_NOT_FOUND",
                                                            "message",
                                                            "Edge not found: " + edgeId)));
                            return;
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
                    }
                };

        this.getOutEdges =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        String nodeId = ctx.pathParam("nodeId");
                        String label = ctx.queryParam("label");
                        List<GraphEdge> edges = graphEngine.getOutEdges(graphId, nodeId, label);
                        ctx.json(Map.of("edges", edges));
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

        this.getInEdges =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        String nodeId = ctx.pathParam("nodeId");
                        String label = ctx.queryParam("label");
                        List<GraphEdge> edges = graphEngine.getInEdges(graphId, nodeId, label);
                        ctx.json(Map.of("edges", edges));
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

        this.neighbors =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        String nodeId = ctx.queryParam("nodeId");
                        String label = ctx.queryParam("label");
                        int maxHops = parseIntParam(ctx.queryParam("maxHops"), "maxHops", 2);
                        int limit = parseIntParam(ctx.queryParam("limit"), "limit", 100);
                        TraversalDirection direction =
                                requireEnum(
                                        TraversalDirection.class,
                                        ctx.queryParam("direction"),
                                        "direction");
                        List<GraphNode> nodes =
                                graphEngine.neighbors(
                                        graphId, nodeId, label, maxHops, limit, direction);
                        ctx.json(Map.of("nodes", nodes));
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

        this.findPaths =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        var body = ctx.bodyAsClass(FindPathsRequest.class);
                        List<PathResult> paths =
                                graphEngine.findPaths(
                                        graphId,
                                        body.srcNodeId(),
                                        body.dstNodeId(),
                                        body.maxHops());
                        ctx.json(Map.of("paths", paths));
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

        this.vectorSearch =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        var body = ctx.bodyAsClass(VectorSearchRequest.class);
                        List<VectorSearchHit> hits =
                                graphEngine.vectorSearch(graphId, body.query(), body.topK());
                        ctx.json(Map.of("hits", hits));
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

        this.extractFromContext =
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
                        String graphId = ctx.pathParam("graphId");
                        GraphSchema schema = graphEngine.getGraph(graphId).orElse(null);
                        if (schema == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_GRAPH_NOT_FOUND",
                                                            "message",
                                                            "Graph not found: " + graphId)));
                            return;
                        }
                        if (!isGraphOwner(schema, principal)) {
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
                        var req = ctx.bodyAsClass(ExtractRequest.class);
                        ExtractionResult result =
                                graphEngine.extractFromContext(
                                        principal.id(), graphId, req.policy());
                        ctx.json(result);
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
        app.post("/api/v1/axiom/graph/graphs", createGraph)
                .get("/api/v1/axiom/graph/graphs", listGraphs)
                .get("/api/v1/axiom/graph/graphs/{graphId}", getGraph)
                .delete("/api/v1/axiom/graph/graphs/{graphId}", deleteGraph)
                .post("/api/v1/axiom/graph/graphs/{graphId}/nodes", addNode)
                .get("/api/v1/axiom/graph/graphs/{graphId}/nodes/{nodeId}", getNode)
                .put("/api/v1/axiom/graph/graphs/{graphId}/nodes/{nodeId}", updateNode)
                .delete("/api/v1/axiom/graph/graphs/{graphId}/nodes/{nodeId}", removeNode)
                .post("/api/v1/axiom/graph/graphs/{graphId}/nodes/query", queryNodes)
                .post("/api/v1/axiom/graph/graphs/{graphId}/edges", addEdge)
                .get("/api/v1/axiom/graph/graphs/{graphId}/edges/{edgeId}", getEdge)
                .put("/api/v1/axiom/graph/graphs/{graphId}/edges/{edgeId}", updateEdge)
                .delete("/api/v1/axiom/graph/graphs/{graphId}/edges/{edgeId}", removeEdge)
                .get("/api/v1/axiom/graph/graphs/{graphId}/nodes/{nodeId}/out-edges", getOutEdges)
                .get("/api/v1/axiom/graph/graphs/{graphId}/nodes/{nodeId}/in-edges", getInEdges)
                .get("/api/v1/axiom/graph/graphs/{graphId}/neighbors", neighbors)
                .post("/api/v1/axiom/graph/graphs/{graphId}/paths", findPaths)
                .post("/api/v1/axiom/graph/graphs/{graphId}/vector-search", vectorSearch)
                .post("/api/v1/axiom/graph/graphs/{graphId}/extract", extractFromContext);
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

    private record CreateGraphRequest(
            String name,
            String description,
            List<String> nodeTypes,
            List<String> edgeLabels,
            Map<String, String> metadata) {}

    private record AddNodeRequest(String type, Map<String, String> properties) {}

    private record UpdateNodeRequest(String nodeId, String type, Map<String, String> properties) {}

    private record PropertyFilterRequest(Map<String, String> propertyFilter) {}

    private record AddEdgeRequest(
            String srcNodeId,
            String label,
            String dstNodeId,
            Map<String, String> properties,
            float weight) {}

    private record UpdateEdgeRequest(
            String edgeId,
            String srcNodeId,
            String label,
            String dstNodeId,
            Map<String, String> properties,
            float weight) {}

    private record FindPathsRequest(String srcNodeId, String dstNodeId, int maxHops) {}

    private record VectorSearchRequest(String query, int topK) {}

    private record ExtractRequest(ExtractionPolicy policy) {}

    private boolean isGraphOwner(GraphSchema schema, Principal principal) {
        return schema != null && schema.agentId().equals(principal.id());
    }
}
