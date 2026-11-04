package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.axiom.memory.exception.AxiomException;
import io.github.rawwise.axiom.orchestrator.AgentOrchestrator;
import io.github.rawwise.axiom.orchestrator.CompensationAction;
import io.github.rawwise.axiom.orchestrator.CompensationType;
import io.github.rawwise.axiom.orchestrator.EdgeCondition;
import io.github.rawwise.axiom.orchestrator.RetryPolicy;
import io.github.rawwise.axiom.orchestrator.StepConfig;
import io.github.rawwise.axiom.orchestrator.StepError;
import io.github.rawwise.axiom.orchestrator.StepType;
import io.github.rawwise.axiom.orchestrator.WorkflowDefinition;
import io.github.rawwise.axiom.orchestrator.WorkflowEdgeDef;
import io.github.rawwise.axiom.orchestrator.WorkflowInstance;
import io.github.rawwise.axiom.orchestrator.WorkflowStepDef;
import io.github.rawwise.axiom.orchestrator.WorkflowStepState;
import io.github.rawwise.axiom.orchestrator.WorkflowValidationResult;
import io.github.rawwise.governance.auth.Principal;
import io.javalin.Javalin;
import io.javalin.http.Handler;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class WorkflowRoutes {

    private final AgentOrchestrator orchestrator;

    public final Handler defineWorkflow;
    public final Handler listWorkflows;
    public final Handler getWorkflow;
    public final Handler deleteWorkflow;
    public final Handler startWorkflow;
    public final Handler getInstance;
    public final Handler listInstances;
    public final Handler cancelWorkflow;
    public final Handler compensateWorkflow;
    public final Handler resumeWorkflow;
    public final Handler retryCompensation;
    public final Handler completeStep;
    public final Handler failStep;
    public final Handler compensateStep;

    public WorkflowRoutes(AgentOrchestrator orchestrator) {
        this.orchestrator = orchestrator;

        this.defineWorkflow =
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
                        var body = ctx.bodyAsClass(DefineWorkflowRequest.class);
                        List<WorkflowStepDef> steps =
                                body.steps().stream()
                                        .map(
                                                s ->
                                                        new WorkflowStepDef(
                                                                s.stepId(),
                                                                s.name(),
                                                                new Principal.AgentPrincipal(
                                                                        s.executor(), null, null),
                                                                requireEnum(
                                                                        StepType.class,
                                                                        s.type(),
                                                                        "step type"),
                                                                parseStepConfig(
                                                                        requireEnum(
                                                                                StepType.class,
                                                                                s.type(),
                                                                                "step type"),
                                                                        s.config()),
                                                                s.timeoutMs(),
                                                                new RetryPolicy(
                                                                        s.maxRetries(),
                                                                        s.initialDelayMs(),
                                                                        s.backoffMultiplier()),
                                                                s.compensationType() != null
                                                                        ? new CompensationAction(
                                                                                requireEnum(
                                                                                        CompensationType
                                                                                                .class,
                                                                                        s
                                                                                                .compensationType(),
                                                                                        "compensationType"),
                                                                                s
                                                                                        .compensationConfig(),
                                                                                s
                                                                                        .compensationInputMapping())
                                                                        : null,
                                                                s.inputMapping()))
                                        .toList();
                        List<WorkflowEdgeDef> edges =
                                body.edges().stream()
                                        .map(
                                                e ->
                                                        new WorkflowEdgeDef(
                                                                e.fromStepId(),
                                                                e.toStepId(),
                                                                requireEnum(
                                                                        EdgeCondition.class,
                                                                        e.condition(),
                                                                        "edge condition")))
                                        .toList();
                        WorkflowValidationResult result =
                                orchestrator.defineWorkflow(
                                        body.name(), principal.id(), steps, edges);
                        switch (result) {
                            case WorkflowValidationResult.Valid v ->
                                    ctx.status(201).json(v.definition());
                            case WorkflowValidationResult.Invalid inv ->
                                    ctx.status(422)
                                            .json(
                                                    Map.of(
                                                            "error",
                                                            Map.of(
                                                                    "code",
                                                                    "VALIDATION_FAILED",
                                                                    "message",
                                                                    "Workflow validation failed",
                                                                    "errors",
                                                                    inv.errors())));
                        }
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

        this.listWorkflows =
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
                        int offset = parseIntParam(ctx.queryParam("offset"), "offset", 0);
                        int limit = parseIntParam(ctx.queryParam("limit"), "limit", 20);
                        List<WorkflowDefinition> workflows =
                                orchestrator.listWorkflows(principal.id(), offset, limit);
                        ctx.json(Map.of("workflows", workflows));
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

        this.getWorkflow =
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
                        WorkflowDefinition definition = orchestrator.getWorkflow(id);
                        if (definition == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow not found: " + id)));
                            return;
                        }
                        if (!isWorkflowOwner(definition, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not the owner of this workflow")));
                            return;
                        }
                        ctx.json(definition);
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

        this.deleteWorkflow =
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
                        WorkflowDefinition definition = orchestrator.getWorkflow(id);
                        if (definition == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow not found: " + id)));
                            return;
                        }
                        if (!isWorkflowOwner(definition, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not the owner of this workflow")));
                            return;
                        }
                        boolean deleted = orchestrator.deleteWorkflow(id);
                        if (!deleted) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow not found: " + id)));
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

        this.startWorkflow =
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
                        WorkflowDefinition definition = orchestrator.getWorkflow(id);
                        if (definition == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow not found: " + id)));
                            return;
                        }
                        if (!isWorkflowOwner(definition, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not authorized to start this workflow")));
                            return;
                        }
                        var body = ctx.bodyAsClass(StartWorkflowRequest.class);
                        Duration timeout =
                                body.timeoutMs() > 0 ? Duration.ofMillis(body.timeoutMs()) : null;
                        WorkflowInstance instance =
                                orchestrator.startWorkflow(
                                        id, principal.id(), body.input(), timeout);
                        ctx.status(201).json(instance);
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

        this.getInstance =
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
                        WorkflowInstance instance = orchestrator.getWorkflowInstance(id);
                        if (instance == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
                            return;
                        }
                        if (!isInstanceOwner(instance, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not the owner of this workflow instance")));
                            return;
                        }
                        ctx.json(instance);
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

        this.listInstances =
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
                        int offset = parseIntParam(ctx.queryParam("offset"), "offset", 0);
                        int limit = parseIntParam(ctx.queryParam("limit"), "limit", 20);
                        List<WorkflowInstance> instances =
                                orchestrator.listInstances(principal.id(), offset, limit);
                        ctx.json(Map.of("instances", instances));
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

        this.cancelWorkflow =
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
                        WorkflowInstance instance = orchestrator.getWorkflowInstance(id);
                        if (instance == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
                            return;
                        }
                        if (!isInstanceOwner(instance, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not the owner of this workflow instance")));
                            return;
                        }
                        var body = ctx.bodyAsClass(CancelRequest.class);
                        boolean cancelled = orchestrator.cancelWorkflow(id, body.reason());
                        if (!cancelled) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
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

        this.compensateWorkflow =
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
                        WorkflowInstance instance = orchestrator.getWorkflowInstance(id);
                        if (instance == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
                            return;
                        }
                        if (!isInstanceOwner(instance, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not the owner of this workflow instance")));
                            return;
                        }
                        var body = ctx.bodyAsClass(CompensateRequest.class);
                        boolean compensated = orchestrator.compensateWorkflow(id, body.reason());
                        if (!compensated) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
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

        this.resumeWorkflow =
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
                        WorkflowInstance inst = orchestrator.getWorkflowInstance(id);
                        if (inst == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
                            return;
                        }
                        if (!isInstanceOwner(inst, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not the owner of this workflow instance")));
                            return;
                        }
                        var body = ctx.bodyAsClass(ResumeRequest.class);
                        WorkflowInstance instance =
                                orchestrator.resumeWorkflow(
                                        id, body.fromStepId(), body.resumeInput());
                        ctx.json(instance);
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

        this.retryCompensation =
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
                        WorkflowInstance instance = orchestrator.getWorkflowInstance(id);
                        if (instance == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
                            return;
                        }
                        if (!isInstanceOwner(instance, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not the owner of this workflow instance")));
                            return;
                        }
                        boolean retried = orchestrator.retryCompensation(id);
                        if (!retried) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
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

        this.completeStep =
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
                        String stepId = ctx.pathParam("stepId");
                        WorkflowInstance instance = orchestrator.getWorkflowInstance(id);
                        if (instance == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
                            return;
                        }
                        if (!isInstanceOwner(instance, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not the owner of this workflow instance")));
                            return;
                        }
                        var body = ctx.bodyAsClass(CompleteStepRequest.class);
                        WorkflowStepState state =
                                orchestrator.completeStep(
                                        id, stepId, body.output(), body.requestToken());
                        ctx.json(state);
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

        this.failStep =
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
                        String stepId = ctx.pathParam("stepId");
                        WorkflowInstance instance = orchestrator.getWorkflowInstance(id);
                        if (instance == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
                            return;
                        }
                        if (!isInstanceOwner(instance, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not the owner of this workflow instance")));
                            return;
                        }
                        var body = ctx.bodyAsClass(FailStepRequest.class);
                        StepError error =
                                new StepError(
                                        body.code(),
                                        body.errorMessage(),
                                        body.retryable(),
                                        body.category());
                        WorkflowStepState state =
                                orchestrator.failStep(id, stepId, error, body.requestToken());
                        ctx.json(state);
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

        this.compensateStep =
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
                        String stepId = ctx.pathParam("stepId");
                        WorkflowInstance instance = orchestrator.getWorkflowInstance(id);
                        if (instance == null) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "NOT_FOUND",
                                                            "message",
                                                            "Workflow instance not found: " + id)));
                            return;
                        }
                        if (!isInstanceOwner(instance, principal)) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not the owner of this workflow instance")));
                            return;
                        }
                        var body = ctx.bodyAsClass(CompensateStepRequest.class);
                        WorkflowStepState state =
                                orchestrator.compensateStep(
                                        id, stepId, body.compensateInput(), body.requestToken());
                        ctx.json(state);
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
        app.post("/api/v1/axiom/orchestrator/workflows", defineWorkflow)
                .get("/api/v1/axiom/orchestrator/workflows", listWorkflows)
                .get("/api/v1/axiom/orchestrator/workflows/{id}", getWorkflow)
                .delete("/api/v1/axiom/orchestrator/workflows/{id}", deleteWorkflow)
                .post("/api/v1/axiom/orchestrator/workflows/{id}/start", startWorkflow)
                .get("/api/v1/axiom/orchestrator/instances/{id}", getInstance)
                .get("/api/v1/axiom/orchestrator/instances", listInstances)
                .post("/api/v1/axiom/orchestrator/instances/{id}/cancel", cancelWorkflow)
                .post("/api/v1/axiom/orchestrator/instances/{id}/compensate", compensateWorkflow)
                .post("/api/v1/axiom/orchestrator/instances/{id}/resume", resumeWorkflow)
                .post(
                        "/api/v1/axiom/orchestrator/instances/{id}/retry-compensation",
                        retryCompensation)
                .post(
                        "/api/v1/axiom/orchestrator/instances/{id}/steps/{stepId}/complete",
                        completeStep)
                .post("/api/v1/axiom/orchestrator/instances/{id}/steps/{stepId}/fail", failStep)
                .post(
                        "/api/v1/axiom/orchestrator/instances/{id}/steps/{stepId}/compensate",
                        compensateStep);
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

    @SuppressWarnings("unchecked")
    private static StepConfig parseStepConfig(StepType type, Map<String, Object> config) {
        if (config == null) config = Map.of();
        return switch (type) {
            case TOOL_CALL ->
                    new StepConfig.ToolCallConfig(
                            (String) config.getOrDefault("toolName", ""), config);
            case SUB_WORKFLOW ->
                    new StepConfig.SubWorkflowConfig(
                            (String) config.getOrDefault("workflowId", ""),
                            (Map<String, String>) config.getOrDefault("inputMapping", Map.of()));
            case HUMAN_APPROVAL ->
                    new StepConfig.HumanApprovalConfig(
                            (String) config.getOrDefault("approverAgentId", ""),
                            (String) config.getOrDefault("message", ""));
        };
    }

    private record DefineWorkflowRequest(
            String name, List<StepDefDto> steps, List<EdgeDefDto> edges) {}

    private record StepDefDto(
            String stepId,
            String name,
            String type,
            String executor,
            Map<String, Object> config,
            long timeoutMs,
            int maxRetries,
            long initialDelayMs,
            double backoffMultiplier,
            String compensationType,
            Map<String, Object> compensationConfig,
            Map<String, String> compensationInputMapping,
            Map<String, String> inputMapping) {}

    private record EdgeDefDto(String fromStepId, String toStepId, String condition) {}

    private record StartWorkflowRequest(Map<String, Object> input, long timeoutMs) {}

    private record CancelRequest(String reason) {}

    private record CompensateRequest(String reason) {}

    private record ResumeRequest(String fromStepId, Map<String, Object> resumeInput) {}

    private record CompleteStepRequest(Map<String, Object> output, String requestToken) {}

    private record FailStepRequest(
            String code,
            String errorMessage,
            boolean retryable,
            String category,
            String requestToken) {}

    private record CompensateStepRequest(
            Map<String, Object> compensateInput, String requestToken) {}

    private static boolean isWorkflowOwner(WorkflowDefinition def, Principal principal) {
        return def != null && def.agentId().equals(principal.id());
    }

    private static boolean isInstanceOwner(WorkflowInstance instance, Principal principal) {
        return instance != null && instance.agentId().equals(principal.id());
    }
}
