package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.execution.CompensationAction;
import io.github.rawwise.execution.ExecutionManager;
import io.github.rawwise.execution.ExecutionPlan;
import io.github.rawwise.execution.ExecutionStep;
import io.github.rawwise.execution.RetryPolicy;
import io.github.rawwise.execution.StepLog;
import io.github.rawwise.execution.StepResult;
import io.github.rawwise.governance.auth.Principal;
import io.javalin.http.Handler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionRoutes {

    private static final Logger log = LoggerFactory.getLogger(ExecutionRoutes.class);

    private final ExecutionManager executionManager;

    public final Handler createPlan;
    public final Handler getPlan;
    public final Handler listPlans;
    public final Handler cancelPlan;
    public final Handler startStep;
    public final Handler completeStep;
    public final Handler failStep;
    public final Handler skipStep;
    public final Handler retryStep;
    public final Handler compensatePlan;
    public final Handler compensateStep;
    public final Handler resumePlan;
    public final Handler getStepLogs;

    public ExecutionRoutes(ExecutionManager executionManager) {
        this.executionManager = executionManager;

        this.createPlan =
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
                        var body = ctx.bodyAsClass(CreatePlanRequest.class);
                        String userId = getUserId(principal);
                        RetryPolicy defaultRetryPolicy =
                                parseRetryPolicy(body.defaultRetryPolicy());

                        List<ExecutionStep> steps = new ArrayList<>();
                        if (body.steps() != null) {
                            for (CreateStepRequest stepReq : body.steps()) {
                                ExecutionStep step =
                                        new ExecutionStep(
                                                "pending",
                                                "pending",
                                                0,
                                                stepReq.description() != null
                                                        ? stepReq.description()
                                                        : "",
                                                stepReq.type() != null
                                                        ? ExecutionStep.StepType.valueOf(
                                                                stepReq.type().toUpperCase())
                                                        : ExecutionStep.StepType.TOOL_CALL,
                                                ExecutionStep.StepStatus.PENDING,
                                                stepReq.toolName() != null
                                                        ? stepReq.toolName()
                                                        : "",
                                                stepReq.toolInput() != null
                                                        ? stepReq.toolInput()
                                                        : Map.of(),
                                                null,
                                                stepReq.retryPolicy() != null
                                                        ? parseRetryPolicy(stepReq.retryPolicy())
                                                        : null,
                                                stepReq.compensation() != null
                                                        ? parseCompensationAction(
                                                                stepReq.compensation())
                                                        : null,
                                                stepReq.contextRef() != null
                                                        ? stepReq.contextRef()
                                                        : "",
                                                stepReq.dependsOn() != null
                                                        ? stepReq.dependsOn()
                                                        : List.of(),
                                                0,
                                                1,
                                                0,
                                                0,
                                                List.of());
                                steps.add(step);
                            }
                        }

                        ExecutionPlan plan =
                                executionManager.createPlan(
                                        principal.id(),
                                        userId,
                                        body.sessionId(),
                                        body.description() != null ? body.description() : "",
                                        steps,
                                        defaultRetryPolicy);

                        ctx.status(201).json(plan);
                    } catch (IOException e) {
                        log.error("Failed to create execution plan", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to create execution plan")));
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

        this.getPlan =
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
                        String planId = ctx.pathParam("planId");
                        var plan = executionManager.getPlan(planId);
                        if (plan.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "PLAN_NOT_FOUND",
                                                            "message",
                                                            "Plan not found: " + planId)));
                            return;
                        }
                        ctx.json(plan.get());
                    } catch (IOException e) {
                        log.error("Failed to get execution plan", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to get execution plan")));
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

        this.listPlans =
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
                        String sessionId = ctx.queryParam("sessionId");
                        List<ExecutionPlan> plans;
                        if (sessionId != null && !sessionId.isBlank()) {
                            plans = executionManager.listPlansBySession(sessionId);
                        } else {
                            plans = executionManager.listPlansByAgent(principal.id());
                        }
                        ctx.json(Map.of("plans", plans));
                    } catch (IOException e) {
                        log.error("Failed to list execution plans", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to list execution plans")));
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

        this.cancelPlan =
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
                        String planId = ctx.pathParam("planId");
                        boolean cancelled = executionManager.cancelPlan(planId);
                        if (!cancelled) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "PLAN_NOT_FOUND",
                                                            "message",
                                                            "Plan not found: " + planId)));
                            return;
                        }
                        ctx.json(Map.of("planId", planId, "status", "cancelled"));
                    } catch (IOException e) {
                        log.error("Failed to cancel execution plan", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to cancel execution plan")));
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

        this.startStep =
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
                        String planId = ctx.pathParam("planId");
                        int stepIndex = Integer.parseInt(ctx.pathParam("stepIndex"));
                        ExecutionStep step = executionManager.startStep(planId, stepIndex);
                        ctx.json(step);
                    } catch (IOException e) {
                        log.error("Failed to start step", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to start step")));
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
                    } catch (IndexOutOfBoundsException e) {
                        ctx.status(404)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "STEP_NOT_FOUND",
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
                        String planId = ctx.pathParam("planId");
                        int stepIndex = Integer.parseInt(ctx.pathParam("stepIndex"));
                        var body = ctx.bodyAsClass(CompleteStepRequest.class);
                        Map<String, String> metrics =
                                body.metrics() != null ? body.metrics() : Map.of();
                        StepResult result =
                                new StepResult(
                                        body.success(),
                                        body.output() != null ? body.output() : "",
                                        body.error() != null ? body.error() : "",
                                        metrics);
                        ExecutionStep step =
                                executionManager.completeStep(planId, stepIndex, result);
                        ctx.json(step);
                    } catch (IOException e) {
                        log.error("Failed to complete step", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to complete step")));
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
                    } catch (IndexOutOfBoundsException e) {
                        ctx.status(404)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "STEP_NOT_FOUND",
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
                        String planId = ctx.pathParam("planId");
                        int stepIndex = Integer.parseInt(ctx.pathParam("stepIndex"));
                        var body = ctx.bodyAsClass(FailStepRequest.class);
                        ExecutionStep step =
                                executionManager.failStep(planId, stepIndex, body.error());
                        ctx.json(step);
                    } catch (IOException e) {
                        log.error("Failed to fail step", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to fail step")));
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
                    } catch (IndexOutOfBoundsException e) {
                        ctx.status(404)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "STEP_NOT_FOUND",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.skipStep =
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
                        String planId = ctx.pathParam("planId");
                        int stepIndex = Integer.parseInt(ctx.pathParam("stepIndex"));
                        var body = ctx.bodyAsClass(SkipStepRequest.class);
                        ExecutionStep step =
                                executionManager.skipStep(planId, stepIndex, body.reason());
                        ctx.json(step);
                    } catch (IOException e) {
                        log.error("Failed to skip step", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to skip step")));
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
                    } catch (IndexOutOfBoundsException e) {
                        ctx.status(404)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "STEP_NOT_FOUND",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.retryStep =
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
                        String planId = ctx.pathParam("planId");
                        int stepIndex = Integer.parseInt(ctx.pathParam("stepIndex"));
                        ExecutionStep step = executionManager.retryStep(planId, stepIndex);
                        ctx.json(step);
                    } catch (IOException e) {
                        log.error("Failed to retry step", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to retry step")));
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
                    } catch (IndexOutOfBoundsException e) {
                        ctx.status(404)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "STEP_NOT_FOUND",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.compensatePlan =
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
                        String planId = ctx.pathParam("planId");
                        int fromStepIndex;
                        try {
                            var body = ctx.bodyAsClass(CompensatePlanRequest.class);
                            fromStepIndex = body.fromStepIndex();
                        } catch (Exception e) {
                            var planOpt = executionManager.getPlan(planId);
                            if (planOpt.isPresent()) {
                                fromStepIndex = planOpt.get().steps().size() - 1;
                            } else {
                                fromStepIndex = 0;
                            }
                        }
                        ExecutionPlan plan = executionManager.compensatePlan(planId, fromStepIndex);
                        ctx.json(plan);
                    } catch (IOException e) {
                        log.error("Failed to compensate plan", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to compensate plan")));
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
                    } catch (IndexOutOfBoundsException e) {
                        ctx.status(404)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "STEP_NOT_FOUND",
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
                        String planId = ctx.pathParam("planId");
                        int stepIndex = Integer.parseInt(ctx.pathParam("stepIndex"));
                        ExecutionStep step = executionManager.compensateStep(planId, stepIndex);
                        ctx.json(step);
                    } catch (IOException e) {
                        log.error("Failed to compensate step", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to compensate step")));
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
                    } catch (IndexOutOfBoundsException e) {
                        ctx.status(404)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "STEP_NOT_FOUND",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.resumePlan =
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
                        String planId = ctx.pathParam("planId");
                        int fromStepIndex = 0;
                        try {
                            var body = ctx.bodyAsClass(ResumePlanRequest.class);
                            fromStepIndex = body.fromStepIndex();
                        } catch (Exception ignored) {
                        }
                        ExecutionPlan plan = executionManager.resumePlan(planId, fromStepIndex);
                        ctx.json(plan);
                    } catch (IOException e) {
                        log.error("Failed to resume plan", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to resume plan")));
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
                    } catch (IndexOutOfBoundsException e) {
                        ctx.status(404)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "STEP_NOT_FOUND",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.getStepLogs =
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
                        String planId = ctx.pathParam("planId");
                        int stepIndex = Integer.parseInt(ctx.pathParam("stepIndex"));
                        List<StepLog> logs = executionManager.getStepLogs(planId, stepIndex);
                        ctx.json(Map.of("logs", logs));
                    } catch (IOException e) {
                        log.error("Failed to get step logs", e);
                        ctx.status(500)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INTERNAL_ERROR",
                                                        "message",
                                                        "Failed to get step logs")));
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
                    } catch (IndexOutOfBoundsException e) {
                        ctx.status(404)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "STEP_NOT_FOUND",
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

    @SuppressWarnings("unchecked")
    private static RetryPolicy parseRetryPolicy(Map<String, Object> policyMap) {
        if (policyMap == null) {
            return RetryPolicy.NO_RETRY;
        }
        int maxAttempts = getInt(policyMap, "maxAttempts", 3);
        long initialDelayMs = getLong(policyMap, "initialDelayMs", 1000L);
        double backoffMultiplier = getDouble(policyMap, "backoffMultiplier", 2.0);
        long maxDelayMs = getLong(policyMap, "maxDelayMs", 60000L);
        String retryOnStr = getString(policyMap, "retryOn", "ON_FAILURE");
        RetryPolicy.RetryOn retryOn;
        try {
            retryOn = RetryPolicy.RetryOn.valueOf(retryOnStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            retryOn = RetryPolicy.RetryOn.ON_FAILURE;
        }
        return new RetryPolicy(maxAttempts, initialDelayMs, backoffMultiplier, maxDelayMs, retryOn);
    }

    @SuppressWarnings("unchecked")
    private static CompensationAction parseCompensationAction(Map<String, Object> compMap) {
        if (compMap == null) {
            return null;
        }
        String compensationId =
                getString(compMap, "compensationId", "comp-" + System.currentTimeMillis());
        String type = getString(compMap, "type", "MARK_ONLY");
        String toolName = getString(compMap, "toolName", "");
        Map<String, String> toolInput =
                compMap.get("toolInput") instanceof Map<?, ?> m
                        ? (Map<String, String>) m
                        : Map.of();
        String description = getString(compMap, "description", "");
        CompensationAction.CompensationType compType;
        try {
            compType = CompensationAction.CompensationType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException e) {
            compType = CompensationAction.CompensationType.MARK_ONLY;
        }
        return new CompensationAction(compensationId, compType, toolName, toolInput, description);
    }

    private static int getInt(Map<String, Object> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value instanceof Number n) {
            return n.intValue();
        }
        return defaultValue;
    }

    private static long getLong(Map<String, Object> map, String key, long defaultValue) {
        Object value = map.get(key);
        if (value instanceof Number n) {
            return n.longValue();
        }
        return defaultValue;
    }

    private static double getDouble(Map<String, Object> map, String key, double defaultValue) {
        Object value = map.get(key);
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        return defaultValue;
    }

    private static String getString(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        if (value instanceof String s) {
            return s;
        }
        return defaultValue;
    }

    private record CreatePlanRequest(
            String sessionId,
            String description,
            List<CreateStepRequest> steps,
            Map<String, Object> defaultRetryPolicy) {}

    private record CreateStepRequest(
            String description,
            String type,
            String toolName,
            Map<String, String> toolInput,
            Map<String, Object> retryPolicy,
            Map<String, Object> compensation,
            String contextRef,
            List<Integer> dependsOn) {}

    private record CompleteStepRequest(
            boolean success, String output, String error, Map<String, String> metrics) {}

    private record FailStepRequest(String error) {}

    private record SkipStepRequest(String reason) {}

    private record CompensatePlanRequest(int fromStepIndex) {}

    private record ResumePlanRequest(int fromStepIndex) {}
}
