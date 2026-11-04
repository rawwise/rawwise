package io.github.rawwise.execution;

import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultExecutionManager implements ExecutionManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultExecutionManager.class);

    static final String PREFIX_PLAN = "exec:plan:";
    static final String PREFIX_PLAN_IDX_AGENT = "exec:plan:idx:agent:";
    static final String PREFIX_PLAN_IDX_SESSION = "exec:plan:idx:session:";

    private static final Pattern COMPENSATION_VAR_PATTERN =
            Pattern.compile(
                    "\\$\\{(step\\.result\\.output|step\\.result\\.error|step\\.toolInput\\.([\\w.\\-]+)|plan\\.steps\\[(\\d+)\\]\\.result\\.output)\\}");

    private final Metastore metastore;
    private final ContextManager contextManager;

    public DefaultExecutionManager(Metastore metastore, ContextManager contextManager) {
        this.metastore = metastore;
        this.contextManager = contextManager;
    }

    @Override
    public ExecutionPlan createPlan(
            String agentId,
            String userId,
            String sessionId,
            String description,
            List<ExecutionStep> steps,
            RetryPolicy defaultRetryPolicy)
            throws IOException {
        String planId = "plan-" + UUID.randomUUID().toString();
        long now = System.currentTimeMillis();

        List<ExecutionStep> enrichedSteps = new ArrayList<>(steps.size());
        for (int i = 0; i < steps.size(); i++) {
            ExecutionStep step = steps.get(i);
            RetryPolicy policy =
                    step.retryPolicy() != null ? step.retryPolicy() : defaultRetryPolicy;
            enrichedSteps.add(
                    new ExecutionStep(
                            "step-" + planId + "-" + i,
                            planId,
                            i,
                            step.description(),
                            step.type(),
                            ExecutionStep.StepStatus.PENDING,
                            step.toolName(),
                            step.toolInput(),
                            null,
                            policy,
                            step.compensation(),
                            step.contextRef(),
                            step.dependsOn(),
                            0,
                            policy.maxAttempts(),
                            0,
                            0,
                            List.of()));
        }

        ExecutionPlan plan =
                new ExecutionPlan(
                        planId,
                        agentId,
                        userId,
                        sessionId,
                        description,
                        ExecutionPlan.PlanStatus.PENDING,
                        enrichedSteps,
                        Map.of(),
                        defaultRetryPolicy,
                        now,
                        now,
                        0);

        byte[] planKey = planKey(planId);
        byte[] planValue = ExecutionJson.writeValueAsBytes(plan);
        byte[] agentIdxKey =
                (PREFIX_PLAN_IDX_AGENT + encode(agentId) + ":" + planId)
                        .getBytes(StandardCharsets.UTF_8);
        byte[] markerValue = planId.getBytes(StandardCharsets.UTF_8);

        List<Precondition> preconditions = List.of(new Precondition.NotExists(planKey));
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(new Mutation.Put(planKey, planValue));
        mutations.add(new Mutation.Put(agentIdxKey, markerValue));
        if (sessionId != null) {
            byte[] sessionIdxKey =
                    (PREFIX_PLAN_IDX_SESSION + encode(sessionId) + ":" + planId)
                            .getBytes(StandardCharsets.UTF_8);
            mutations.add(new Mutation.Put(sessionIdxKey, markerValue));
        }

        boolean success = metastore.compareAndSwap(preconditions, mutations);
        if (!success) {
            throw new IOException("Plan already exists: " + planId);
        }

        return plan;
    }

    @Override
    public Optional<ExecutionPlan> getPlan(String planId) throws IOException {
        byte[] key = planKey(planId);
        Optional<byte[]> value = metastore.get(key);
        if (value.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ExecutionJson.readValue(value.get(), ExecutionPlan.class));
    }

    @Override
    public List<ExecutionPlan> listPlansByAgent(String agentId) throws IOException {
        byte[] prefix =
                (PREFIX_PLAN_IDX_AGENT + encode(agentId) + ":").getBytes(StandardCharsets.UTF_8);
        List<KeyValue> kvs = metastore.scanAll(prefix);
        List<ExecutionPlan> plans = new ArrayList<>(kvs.size());
        for (KeyValue kv : kvs) {
            String planId = new String(kv.value(), StandardCharsets.UTF_8);
            Optional<byte[]> planBytes = metastore.get(planKey(planId));
            if (planBytes.isPresent()) {
                plans.add(ExecutionJson.readValue(planBytes.get(), ExecutionPlan.class));
            } else {
                metastore.batchWrite(List.of(new Mutation.Delete(kv.key())));
            }
        }
        return plans;
    }

    @Override
    public List<ExecutionPlan> listPlansBySession(String sessionId) throws IOException {
        if (sessionId == null) {
            return List.of();
        }
        byte[] prefix =
                (PREFIX_PLAN_IDX_SESSION + encode(sessionId) + ":")
                        .getBytes(StandardCharsets.UTF_8);
        List<KeyValue> kvs = metastore.scanAll(prefix);
        List<ExecutionPlan> plans = new ArrayList<>(kvs.size());
        for (KeyValue kv : kvs) {
            String planId = new String(kv.value(), StandardCharsets.UTF_8);
            Optional<byte[]> planBytes = metastore.get(planKey(planId));
            if (planBytes.isPresent()) {
                plans.add(ExecutionJson.readValue(planBytes.get(), ExecutionPlan.class));
            } else {
                metastore.batchWrite(List.of(new Mutation.Delete(kv.key())));
            }
        }
        return plans;
    }

    @Override
    public boolean cancelPlan(String planId) throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);
        if (plan.status() != ExecutionPlan.PlanStatus.PENDING
                && plan.status() != ExecutionPlan.PlanStatus.RUNNING) {
            return false;
        }
        ExecutionPlan cancelled =
                new ExecutionPlan(
                        plan.planId(),
                        plan.agentId(),
                        plan.userId(),
                        plan.sessionId(),
                        plan.description(),
                        ExecutionPlan.PlanStatus.CANCELLED,
                        plan.steps(),
                        plan.metadata(),
                        plan.defaultRetryPolicy(),
                        plan.createdAt(),
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        storePlanCAS(cancelled);
        return true;
    }

    @Override
    public ExecutionStep startStep(String planId, int stepIndex) throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);
        validateStepIndex(plan, stepIndex);
        ExecutionStep step = plan.steps().get(stepIndex);

        if (step.status() != ExecutionStep.StepStatus.PENDING
                && step.status() != ExecutionStep.StepStatus.RETRYING) {
            throw new IllegalStateException(
                    "Cannot start step "
                            + stepIndex
                            + " in status "
                            + step.status()
                            + " for plan "
                            + planId);
        }

        long now = System.currentTimeMillis();
        ExecutionStep started =
                new ExecutionStep(
                        step.stepId(),
                        step.planId(),
                        step.stepIndex(),
                        step.description(),
                        step.type(),
                        ExecutionStep.StepStatus.RUNNING,
                        step.toolName(),
                        step.toolInput(),
                        step.result(),
                        step.retryPolicy(),
                        step.compensation(),
                        step.contextRef(),
                        step.dependsOn(),
                        step.attemptCount() + 1,
                        step.maxAttempts(),
                        now,
                        step.finishedAt(),
                        step.logs());

        ExecutionPlan.PlanStatus newPlanStatus = plan.status();
        if (plan.status() == ExecutionPlan.PlanStatus.PENDING) {
            newPlanStatus = ExecutionPlan.PlanStatus.RUNNING;
        }

        List<ExecutionStep> steps = new ArrayList<>(plan.steps());
        steps.set(stepIndex, started);
        ExecutionPlan updatedPlan =
                new ExecutionPlan(
                        plan.planId(),
                        plan.agentId(),
                        plan.userId(),
                        plan.sessionId(),
                        plan.description(),
                        newPlanStatus,
                        steps,
                        plan.metadata(),
                        plan.defaultRetryPolicy(),
                        plan.createdAt(),
                        now,
                        plan.finishedAt());

        storePlanCAS(updatedPlan);
        return started;
    }

    @Override
    public ExecutionStep completeStep(String planId, int stepIndex, StepResult result)
            throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);
        validateStepIndex(plan, stepIndex);
        ExecutionStep step = plan.steps().get(stepIndex);

        long now = System.currentTimeMillis();
        ExecutionStep completed =
                new ExecutionStep(
                        step.stepId(),
                        step.planId(),
                        step.stepIndex(),
                        step.description(),
                        step.type(),
                        ExecutionStep.StepStatus.SUCCEEDED,
                        step.toolName(),
                        step.toolInput(),
                        result,
                        step.retryPolicy(),
                        step.compensation(),
                        step.contextRef(),
                        step.dependsOn(),
                        step.attemptCount(),
                        step.maxAttempts(),
                        step.startedAt(),
                        now,
                        step.logs());

        plan = updateStepInPlan(plan, stepIndex, completed);

        if (completed.contextRef() != null
                && !completed.contextRef().isEmpty()
                && result.success()
                && plan.sessionId() != null) {
            try {
                contextManager.putSessionEntry(
                        plan.sessionId(),
                        completed.contextRef(),
                        result.output(),
                        "application/json",
                        "utf-8",
                        Map.of("stepId", completed.stepId(), "planId", planId));
            } catch (Exception e) {
                log.warn(
                        "Failed to put session entry for step {}: {}",
                        completed.stepId(),
                        e.getMessage());
            }
        }

        plan = loadPlanOrThrow(planId);
        plan = checkAndUpdatePlanStatus(plan);
        return plan.steps().get(stepIndex);
    }

    @Override
    public ExecutionStep failStep(String planId, int stepIndex, String error) throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);
        validateStepIndex(plan, stepIndex);
        ExecutionStep step = plan.steps().get(stepIndex);

        long now = System.currentTimeMillis();
        ExecutionStep failed =
                new ExecutionStep(
                        step.stepId(),
                        step.planId(),
                        step.stepIndex(),
                        step.description(),
                        step.type(),
                        ExecutionStep.StepStatus.FAILED,
                        step.toolName(),
                        step.toolInput(),
                        StepResult.ofFailure(error),
                        step.retryPolicy(),
                        step.compensation(),
                        step.contextRef(),
                        step.dependsOn(),
                        step.attemptCount(),
                        step.maxAttempts(),
                        step.startedAt(),
                        now,
                        step.logs());

        plan = updateStepInPlan(plan, stepIndex, failed);
        plan = checkAndUpdatePlanStatus(plan);
        return plan.steps().get(stepIndex);
    }

    @Override
    public ExecutionStep skipStep(String planId, int stepIndex, String reason) throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);
        validateStepIndex(plan, stepIndex);
        ExecutionStep step = plan.steps().get(stepIndex);

        if (step.status() != ExecutionStep.StepStatus.PENDING) {
            throw new IllegalStateException(
                    "Cannot skip step "
                            + stepIndex
                            + " in status "
                            + step.status()
                            + " for plan "
                            + planId);
        }

        long now = System.currentTimeMillis();
        List<StepLog> logs = appendLog(step, "INFO", "Step skipped: " + reason);
        ExecutionStep skipped =
                new ExecutionStep(
                        step.stepId(),
                        step.planId(),
                        step.stepIndex(),
                        step.description(),
                        step.type(),
                        ExecutionStep.StepStatus.SKIPPED,
                        step.toolName(),
                        step.toolInput(),
                        step.result(),
                        step.retryPolicy(),
                        step.compensation(),
                        step.contextRef(),
                        step.dependsOn(),
                        step.attemptCount(),
                        step.maxAttempts(),
                        step.startedAt(),
                        now,
                        logs);

        plan = updateStepInPlan(plan, stepIndex, skipped);
        plan = checkAndUpdatePlanStatus(plan);
        return plan.steps().get(stepIndex);
    }

    @Override
    public ExecutionStep retryStep(String planId, int stepIndex) throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);
        validateStepIndex(plan, stepIndex);
        ExecutionStep step = plan.steps().get(stepIndex);

        if (step.status() != ExecutionStep.StepStatus.FAILED) {
            throw new IllegalStateException(
                    "Cannot retry step "
                            + stepIndex
                            + " in status "
                            + step.status()
                            + " for plan "
                            + planId);
        }

        if (step.attemptCount() >= step.maxAttempts()) {
            throw new IllegalStateException(
                    "Max attempts ("
                            + step.maxAttempts()
                            + ") reached for step "
                            + stepIndex
                            + " in plan "
                            + planId);
        }

        RetryPolicy policy =
                step.retryPolicy() != null ? step.retryPolicy() : plan.defaultRetryPolicy();
        long delay = policy != null ? policy.computeDelay(step.attemptCount() + 1) : 0;

        List<StepLog> logs =
                appendLog(
                        step,
                        "INFO",
                        "Retrying step after "
                                + delay
                                + "ms (attempt "
                                + (step.attemptCount() + 1)
                                + " of "
                                + step.maxAttempts()
                                + ")");
        ExecutionStep retried =
                new ExecutionStep(
                        step.stepId(),
                        step.planId(),
                        step.stepIndex(),
                        step.description(),
                        step.type(),
                        ExecutionStep.StepStatus.RETRYING,
                        step.toolName(),
                        step.toolInput(),
                        step.result(),
                        step.retryPolicy(),
                        step.compensation(),
                        step.contextRef(),
                        step.dependsOn(),
                        step.attemptCount(),
                        step.maxAttempts(),
                        step.startedAt(),
                        step.finishedAt(),
                        logs);

        plan = updateStepInPlan(plan, stepIndex, retried);
        return plan.steps().get(stepIndex);
    }

    @Override
    public ExecutionPlan compensatePlan(String planId, int fromStepIndex) throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);

        if (plan.status() != ExecutionPlan.PlanStatus.FAILED
                && plan.status() != ExecutionPlan.PlanStatus.CANCELLED
                && plan.status() != ExecutionPlan.PlanStatus.COMPENSATING) {
            throw new IllegalStateException(
                    "Cannot compensate plan " + planId + " in status " + plan.status());
        }

        ExecutionPlan.PlanStatus preCompensationStatus;
        if (plan.status() == ExecutionPlan.PlanStatus.COMPENSATING) {
            String preStatus = plan.metadata().get("preCompensationStatus");
            if (preStatus != null) {
                preCompensationStatus = ExecutionPlan.PlanStatus.valueOf(preStatus);
            } else {
                boolean anyFailed = false;
                for (ExecutionStep s : plan.steps()) {
                    if (s.status() == ExecutionStep.StepStatus.FAILED) {
                        anyFailed = true;
                        break;
                    }
                }
                preCompensationStatus =
                        anyFailed
                                ? ExecutionPlan.PlanStatus.FAILED
                                : ExecutionPlan.PlanStatus.CANCELLED;
            }
        } else {
            preCompensationStatus = plan.status();
        }

        if (plan.status() != ExecutionPlan.PlanStatus.COMPENSATING) {
            Map<String, String> metadata = new HashMap<>(plan.metadata());
            metadata.put("preCompensationStatus", preCompensationStatus.name());
            ExecutionPlan compensatingPlan =
                    new ExecutionPlan(
                            plan.planId(),
                            plan.agentId(),
                            plan.userId(),
                            plan.sessionId(),
                            plan.description(),
                            ExecutionPlan.PlanStatus.COMPENSATING,
                            plan.steps(),
                            metadata,
                            plan.defaultRetryPolicy(),
                            plan.createdAt(),
                            System.currentTimeMillis(),
                            plan.finishedAt());
            storePlanCAS(compensatingPlan);
        }

        for (int i = fromStepIndex; i >= 0; i--) {
            plan = loadPlanOrThrow(planId);
            ExecutionStep step = plan.steps().get(i);

            if (step.status() == ExecutionStep.StepStatus.SUCCEEDED
                    && step.compensation() != null) {
                ExecutionStep compensated = compensateStepInMemory(step, plan);
                plan = updateStepInPlan(plan, i, compensated);
            } else if (step.status() == ExecutionStep.StepStatus.COMPENSATED) {
                log.info("Step {} already compensated, skipping", step.stepId());
            }
        }

        ExecutionPlan finalPlan = loadPlanOrThrow(planId);

        boolean hasCompensationSteps = false;
        boolean allCompensated = true;
        boolean anyCompensating = false;
        for (ExecutionStep s : finalPlan.steps()) {
            if (s.compensation() != null) {
                hasCompensationSteps = true;
                if (s.status() != ExecutionStep.StepStatus.COMPENSATED) {
                    allCompensated = false;
                }
                if (s.status() == ExecutionStep.StepStatus.COMPENSATING) {
                    anyCompensating = true;
                }
            }
        }

        ExecutionPlan.PlanStatus finalStatus;
        if (!hasCompensationSteps) {
            finalStatus = preCompensationStatus;
        } else if (allCompensated) {
            finalStatus = ExecutionPlan.PlanStatus.COMPENSATED;
        } else if (anyCompensating) {
            finalStatus = ExecutionPlan.PlanStatus.COMPENSATING;
        } else {
            finalStatus = ExecutionPlan.PlanStatus.FAILED;
        }

        long finishedAt;
        if (finalStatus == ExecutionPlan.PlanStatus.COMPENSATING) {
            finishedAt = finalPlan.finishedAt();
        } else if (!hasCompensationSteps) {
            finishedAt = finalPlan.finishedAt();
        } else {
            finishedAt = System.currentTimeMillis();
        }

        ExecutionPlan donePlan =
                new ExecutionPlan(
                        finalPlan.planId(),
                        finalPlan.agentId(),
                        finalPlan.userId(),
                        finalPlan.sessionId(),
                        finalPlan.description(),
                        finalStatus,
                        finalPlan.steps(),
                        finalPlan.metadata(),
                        finalPlan.defaultRetryPolicy(),
                        finalPlan.createdAt(),
                        System.currentTimeMillis(),
                        finishedAt);

        storePlanCAS(donePlan);
        return donePlan;
    }

    @Override
    public ExecutionStep compensateStep(String planId, int stepIndex) throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);

        if (plan.status() != ExecutionPlan.PlanStatus.FAILED
                && plan.status() != ExecutionPlan.PlanStatus.CANCELLED
                && plan.status() != ExecutionPlan.PlanStatus.COMPENSATING) {
            throw new IllegalStateException(
                    "Cannot compensate step in plan " + planId + " with status " + plan.status());
        }

        validateStepIndex(plan, stepIndex);
        ExecutionStep step = plan.steps().get(stepIndex);

        if (step.status() != ExecutionStep.StepStatus.SUCCEEDED) {
            throw new IllegalStateException(
                    "Cannot compensate step "
                            + stepIndex
                            + " in status "
                            + step.status()
                            + " for plan "
                            + planId);
        }

        if (step.compensation() == null) {
            throw new IllegalStateException(
                    "Step " + stepIndex + " in plan " + planId + " has no compensation defined");
        }

        ExecutionStep compensated = compensateStepInMemory(step, plan);
        plan = updateStepInPlan(plan, stepIndex, compensated);
        return plan.steps().get(stepIndex);
    }

    @Override
    public ExecutionPlan resumePlan(String planId, int fromStepIndex) throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);

        if (plan.status() != ExecutionPlan.PlanStatus.FAILED
                && plan.status() != ExecutionPlan.PlanStatus.CANCELLED) {
            throw new IllegalStateException(
                    "Cannot resume plan " + planId + " in status " + plan.status());
        }

        long now = System.currentTimeMillis();
        List<ExecutionStep> steps = new ArrayList<>(plan.steps());
        for (int i = fromStepIndex; i < steps.size(); i++) {
            ExecutionStep step = steps.get(i);
            if (step.status() == ExecutionStep.StepStatus.FAILED
                    || step.status() == ExecutionStep.StepStatus.PENDING) {
                steps.set(
                        i,
                        new ExecutionStep(
                                step.stepId(),
                                step.planId(),
                                step.stepIndex(),
                                step.description(),
                                step.type(),
                                ExecutionStep.StepStatus.PENDING,
                                step.toolName(),
                                step.toolInput(),
                                null,
                                step.retryPolicy(),
                                step.compensation(),
                                step.contextRef(),
                                step.dependsOn(),
                                0,
                                step.maxAttempts(),
                                0,
                                0,
                                List.of()));
            }
        }

        ExecutionPlan resumed =
                new ExecutionPlan(
                        plan.planId(),
                        plan.agentId(),
                        plan.userId(),
                        plan.sessionId(),
                        plan.description(),
                        ExecutionPlan.PlanStatus.RUNNING,
                        steps,
                        plan.metadata(),
                        plan.defaultRetryPolicy(),
                        plan.createdAt(),
                        now,
                        0);

        storePlanCAS(resumed);
        return resumed;
    }

    @Override
    public void appendStepLog(String planId, int stepIndex, String level, String message)
            throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);
        validateStepIndex(plan, stepIndex);
        ExecutionStep step = plan.steps().get(stepIndex);

        List<StepLog> logs = appendLog(step, level, message);
        ExecutionStep updated =
                new ExecutionStep(
                        step.stepId(),
                        step.planId(),
                        step.stepIndex(),
                        step.description(),
                        step.type(),
                        step.status(),
                        step.toolName(),
                        step.toolInput(),
                        step.result(),
                        step.retryPolicy(),
                        step.compensation(),
                        step.contextRef(),
                        step.dependsOn(),
                        step.attemptCount(),
                        step.maxAttempts(),
                        step.startedAt(),
                        step.finishedAt(),
                        logs);

        updateStepInPlan(plan, stepIndex, updated);
    }

    @Override
    public List<StepLog> getStepLogs(String planId, int stepIndex) throws IOException {
        ExecutionPlan plan = loadPlanOrThrow(planId);
        validateStepIndex(plan, stepIndex);
        return plan.steps().get(stepIndex).logs();
    }

    private ExecutionPlan loadPlanOrThrow(String planId) throws IOException {
        byte[] key = planKey(planId);
        Optional<byte[]> value = metastore.get(key);
        if (value.isEmpty()) {
            throw new IOException("Plan not found: " + planId);
        }
        return ExecutionJson.readValue(value.get(), ExecutionPlan.class);
    }

    private void storePlanCAS(ExecutionPlan plan) throws IOException {
        byte[] key = planKey(plan.planId());
        byte[] current = metastore.get(key).orElse(null);
        if (current == null) {
            throw new IOException("Plan not found: " + plan.planId());
        }
        byte[] newValue = ExecutionJson.writeValueAsBytes(plan);
        boolean success =
                metastore.compareAndSwap(
                        List.of(
                                new Precondition.Exists(key),
                                new Precondition.ValueEquals(key, current)),
                        List.of(new Mutation.Put(key, newValue)));
        if (!success) {
            throw new IOException("CAS failed for plan: " + plan.planId());
        }
    }

    private ExecutionPlan updateStepInPlan(
            ExecutionPlan plan, int stepIndex, ExecutionStep updatedStep) throws IOException {
        List<ExecutionStep> steps = new ArrayList<>(plan.steps());
        steps.set(stepIndex, updatedStep);
        ExecutionPlan updated =
                new ExecutionPlan(
                        plan.planId(),
                        plan.agentId(),
                        plan.userId(),
                        plan.sessionId(),
                        plan.description(),
                        plan.status(),
                        steps,
                        plan.metadata(),
                        plan.defaultRetryPolicy(),
                        plan.createdAt(),
                        System.currentTimeMillis(),
                        plan.finishedAt());
        storePlanCAS(updated);
        return updated;
    }

    private void validateStepIndex(ExecutionPlan plan, int stepIndex) {
        if (stepIndex < 0 || stepIndex >= plan.steps().size()) {
            throw new IndexOutOfBoundsException(
                    "Step index "
                            + stepIndex
                            + " out of bounds for plan "
                            + plan.planId()
                            + " with "
                            + plan.steps().size()
                            + " steps");
        }
    }

    private ExecutionPlan checkAndUpdatePlanStatus(ExecutionPlan plan) throws IOException {
        List<ExecutionStep> steps = plan.steps();
        boolean allDone = steps.size() > 0;
        boolean anyFailed = false;
        for (ExecutionStep step : steps) {
            if (step.status() != ExecutionStep.StepStatus.SUCCEEDED
                    && step.status() != ExecutionStep.StepStatus.SKIPPED) {
                allDone = false;
            }
            if (step.status() == ExecutionStep.StepStatus.FAILED) {
                anyFailed = true;
            }
        }

        ExecutionPlan.PlanStatus newStatus = plan.status();
        long finishedAt = plan.finishedAt();
        if (allDone) {
            newStatus = ExecutionPlan.PlanStatus.SUCCEEDED;
            finishedAt = System.currentTimeMillis();
        } else if (anyFailed) {
            newStatus = ExecutionPlan.PlanStatus.FAILED;
            finishedAt = System.currentTimeMillis();
        }

        if (newStatus != plan.status()) {
            ExecutionPlan updated =
                    new ExecutionPlan(
                            plan.planId(),
                            plan.agentId(),
                            plan.userId(),
                            plan.sessionId(),
                            plan.description(),
                            newStatus,
                            plan.steps(),
                            plan.metadata(),
                            plan.defaultRetryPolicy(),
                            plan.createdAt(),
                            System.currentTimeMillis(),
                            finishedAt);
            storePlanCAS(updated);
            return updated;
        }
        return plan;
    }

    private ExecutionStep compensateStepInMemory(ExecutionStep step, ExecutionPlan plan) {
        List<StepLog> logs =
                appendLog(step, "INFO", "Starting compensation for step " + step.stepId());
        CompensationAction comp = step.compensation();

        ExecutionStep.StepStatus finalStatus = ExecutionStep.StepStatus.COMPENSATING;
        Map<String, String> metrics =
                step.result() != null ? new HashMap<>(step.result().metrics()) : new HashMap<>();

        switch (comp.type()) {
            case TOOL_CALL:
                Map<String, String> resolvedInput = new HashMap<>();
                for (Map.Entry<String, String> entry : comp.toolInput().entrySet()) {
                    resolvedInput.put(
                            entry.getKey(), resolveCompensationInput(entry.getValue(), step, plan));
                }
                for (Map.Entry<String, String> entry : resolvedInput.entrySet()) {
                    metrics.put("compensation." + entry.getKey(), entry.getValue());
                }
                finalStatus = ExecutionStep.StepStatus.COMPENSATED;
                break;
            case MARK_ONLY:
                finalStatus = ExecutionStep.StepStatus.COMPENSATED;
                break;
            case MANUAL:
                finalStatus = ExecutionStep.StepStatus.COMPENSATING;
                break;
        }

        StepResult updatedResult =
                step.result() != null
                        ? new StepResult(
                                step.result().success(),
                                step.result().output(),
                                step.result().error(),
                                metrics)
                        : new StepResult(false, "", "", metrics);

        return new ExecutionStep(
                step.stepId(),
                step.planId(),
                step.stepIndex(),
                step.description(),
                step.type(),
                finalStatus,
                step.toolName(),
                step.toolInput(),
                updatedResult,
                step.retryPolicy(),
                step.compensation(),
                step.contextRef(),
                step.dependsOn(),
                step.attemptCount(),
                step.maxAttempts(),
                step.startedAt(),
                System.currentTimeMillis(),
                logs);
    }

    private String resolveCompensationInput(
            String template, ExecutionStep step, ExecutionPlan plan) {
        Matcher matcher = COMPENSATION_VAR_PATTERN.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String group1 = matcher.group(1);
            String group2 = matcher.group(2);
            String group3 = matcher.group(3);
            String replacement = "";

            if (group3 != null) {
                int idx = Integer.parseInt(group3);
                if (idx >= 0 && idx < plan.steps().size()) {
                    ExecutionStep refStep = plan.steps().get(idx);
                    replacement = refStep.result() != null ? refStep.result().output() : "";
                }
            } else if (group2 != null) {
                replacement = step.toolInput().getOrDefault(group2, "");
            } else if ("step.result.output".equals(group1)) {
                replacement = step.result() != null ? step.result().output() : "";
            } else if ("step.result.error".equals(group1)) {
                replacement = step.result() != null ? step.result().error() : "";
            }

            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private List<StepLog> appendLog(ExecutionStep step, String level, String message) {
        List<StepLog> logs = new ArrayList<>(step.logs());
        logs.add(new StepLog(System.currentTimeMillis(), level, message));
        return List.copyOf(logs);
    }

    private byte[] planKey(String planId) {
        return (PREFIX_PLAN + planId).getBytes(StandardCharsets.UTF_8);
    }

    private static String encode(String raw) {
        return URLEncoder.encode(raw, StandardCharsets.UTF_8);
    }
}
