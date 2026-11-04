package io.github.rawwise.axiom.orchestrator;

import io.github.rawwise.axiom.memory.exception.WorkflowDefinitionInUseException;
import io.github.rawwise.axiom.memory.exception.WorkflowInstanceNotFoundException;
import io.github.rawwise.axiom.memory.exception.WorkflowNotFoundException;
import io.github.rawwise.axiom.memory.exception.WorkflowQuotaExceededException;
import io.github.rawwise.axiom.memory.exception.WorkflowStepInvalidStateException;
import io.github.rawwise.axiom.memory.exception.WorkflowStepNotFoundException;
import io.github.rawwise.axiom.pipeline.state.StateEngine;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DefaultAgentOrchestrator implements AgentOrchestrator {

    private final WorkflowCoordinator coordinator;
    private final StateEngine stateEngine;
    private final AxiomOrchestratorOptions options;

    private final Map<String, WorkflowDefinition> workflowDefinitions = new ConcurrentHashMap<>();
    private final Map<String, WorkflowInstance> workflowInstances = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentHashMap<String, WorkflowStepState>> stepStates =
            new ConcurrentHashMap<>();
    private final Map<String, String> requestTokenDedup = new ConcurrentHashMap<>();

    @Override
    public WorkflowValidationResult defineWorkflow(
            String name, String agentId, List<WorkflowStepDef> steps, List<WorkflowEdgeDef> edges) {
        List<ValidationError> errors = validateDag(steps, edges);
        if (!errors.isEmpty()) {
            return new WorkflowValidationResult.Invalid(errors);
        }

        String workflowId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        WorkflowDefinition definition =
                new WorkflowDefinition(
                        workflowId,
                        name,
                        agentId,
                        steps,
                        edges,
                        WorkflowStatus.ACTIVE,
                        1,
                        Map.of(),
                        now,
                        now);

        workflowDefinitions.put(workflowId, definition);
        log.info("Workflow defined: id={}, name={}, agentId={}", workflowId, name, agentId);
        return new WorkflowValidationResult.Valid(definition);
    }

    @Override
    public WorkflowDefinition getWorkflow(String workflowId) {
        return workflowDefinitions.get(workflowId);
    }

    @Override
    public List<WorkflowDefinition> listWorkflows(String agentId, int offset, int limit) {
        return workflowDefinitions.values().stream()
                .filter(d -> d.agentId().equals(agentId))
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public boolean deleteWorkflow(String workflowId) {
        WorkflowDefinition definition = workflowDefinitions.get(workflowId);
        if (definition == null) {
            throw new WorkflowNotFoundException("Workflow not found: " + workflowId);
        }

        boolean hasRunningInstances =
                workflowInstances.values().stream()
                        .anyMatch(
                                i ->
                                        i.workflowId().equals(workflowId)
                                                && (i.status() == WorkflowInstanceStatus.RUNNING
                                                        || i.status()
                                                                == WorkflowInstanceStatus.PENDING));

        if (hasRunningInstances) {
            throw new WorkflowDefinitionInUseException(
                    "Cannot delete workflow with running instances: " + workflowId);
        }

        workflowDefinitions.remove(workflowId);
        log.info("Workflow deleted: id={}", workflowId);
        return true;
    }

    @Override
    public WorkflowInstance startWorkflow(
            String workflowId, String agentId, Map<String, Object> input, Duration timeout) {
        WorkflowDefinition definition = workflowDefinitions.get(workflowId);
        if (definition == null) {
            throw new WorkflowNotFoundException("Workflow not found: " + workflowId);
        }
        if (definition.status() != WorkflowStatus.ACTIVE) {
            throw new WorkflowNotFoundException("Workflow is not active: " + workflowId);
        }

        long runningCount =
                workflowInstances.values().stream()
                        .filter(
                                i ->
                                        i.status() == WorkflowInstanceStatus.RUNNING
                                                || i.status() == WorkflowInstanceStatus.PENDING)
                        .count();
        if (runningCount >= options.maxConcurrentInstances()) {
            throw new WorkflowQuotaExceededException(
                    "Max concurrent instances exceeded: " + runningCount);
        }

        long agentRunningCount =
                workflowInstances.values().stream()
                        .filter(
                                i ->
                                        i.agentId().equals(agentId)
                                                && (i.status() == WorkflowInstanceStatus.RUNNING
                                                        || i.status()
                                                                == WorkflowInstanceStatus.PENDING))
                        .count();
        if (agentRunningCount >= options.maxConcurrentInstancesPerAgent()) {
            throw new WorkflowQuotaExceededException(
                    "Max concurrent instances per agent exceeded: " + agentRunningCount);
        }

        String instanceId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        long timeoutMs = timeout != null ? timeout.toMillis() : 0;

        List<String> stepIds =
                definition.steps().stream()
                        .map(WorkflowStepDef::stepId)
                        .collect(Collectors.toList());

        ConcurrentHashMap<String, WorkflowStepState> states = new ConcurrentHashMap<>();
        for (WorkflowStepDef step : definition.steps()) {
            states.put(
                    step.stepId(),
                    new WorkflowStepState(
                            step.stepId(),
                            StepStatus.PENDING,
                            Map.of(),
                            Map.of(),
                            null,
                            0,
                            null,
                            0,
                            0));
        }

        List<String> coordResources;
        try {
            coordResources = coordinator.allocateCoordResources(instanceId, definition);
        } catch (Exception e) {
            stepStates.remove(instanceId);
            throw new WorkflowQuotaExceededException(
                    "Failed to allocate coordination resources: " + e.getMessage());
        }

        WorkflowInstance instance =
                new WorkflowInstance(
                        instanceId,
                        workflowId,
                        definition.version(),
                        agentId,
                        input,
                        Map.of(),
                        WorkflowInstanceStatus.RUNNING,
                        null,
                        null,
                        null,
                        stepIds,
                        coordResources,
                        timeoutMs,
                        now,
                        0);
        workflowInstances.put(instanceId, instance);
        stepStates.put(instanceId, states);

        evaluateInitialSteps(instanceId, definition);

        log.info("Workflow started: instanceId={}, workflowId={}", instanceId, workflowId);
        return instance;
    }

    @Override
    public WorkflowInstance getWorkflowInstance(String instanceId) {
        return workflowInstances.get(instanceId);
    }

    @Override
    public List<WorkflowInstance> listInstances(String agentId, int offset, int limit) {
        return workflowInstances.values().stream()
                .filter(i -> agentId == null || agentId.isEmpty() || i.agentId().equals(agentId))
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public boolean cancelWorkflow(String instanceId, String reason) {
        WorkflowInstance instance = workflowInstances.get(instanceId);
        if (instance == null) {
            throw new WorkflowInstanceNotFoundException(
                    "Workflow instance not found: " + instanceId);
        }
        if (instance.status() != WorkflowInstanceStatus.RUNNING
                && instance.status() != WorkflowInstanceStatus.PENDING) {
            throw new WorkflowStepInvalidStateException(
                    "Cannot cancel instance in state: " + instance.status());
        }

        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states != null) {
            states.replaceAll(
                    (stepId, s) -> {
                        if (s.status() == StepStatus.PENDING
                                || s.status() == StepStatus.READY
                                || s.status() == StepStatus.BLOCKED
                                || s.status() == StepStatus.WAITING
                                || s.status() == StepStatus.RETRYING) {
                            return new WorkflowStepState(
                                    s.stepId(),
                                    StepStatus.CANCELLED,
                                    s.input(),
                                    s.output(),
                                    s.error(),
                                    s.retryCount(),
                                    s.nextRetryAt(),
                                    s.startedAt(),
                                    System.currentTimeMillis());
                        }
                        return s;
                    });
        }

        WorkflowInstance cancelled =
                new WorkflowInstance(
                        instance.instanceId(),
                        instance.workflowId(),
                        instance.workflowVersion(),
                        instance.agentId(),
                        instance.input(),
                        instance.output(),
                        WorkflowInstanceStatus.CANCELLED,
                        instance.error(),
                        reason,
                        instance.compensateReason(),
                        instance.stepIds(),
                        instance.coordinationResourceIds(),
                        instance.timeoutMs(),
                        instance.startedAt(),
                        System.currentTimeMillis());
        workflowInstances.put(instanceId, cancelled);

        coordinator.releaseCoordResources(instanceId);
        cleanupRequestTokens(instanceId);
        log.info("Workflow cancelled: instanceId={}, reason={}", instanceId, reason);
        return true;
    }

    @Override
    public boolean compensateWorkflow(String instanceId, String reason) {
        WorkflowInstance instance = workflowInstances.get(instanceId);
        if (instance == null) {
            throw new WorkflowInstanceNotFoundException(
                    "Workflow instance not found: " + instanceId);
        }
        if (instance.status() != WorkflowInstanceStatus.FAILED
                && instance.status() != WorkflowInstanceStatus.TIMEOUT
                && instance.status() != WorkflowInstanceStatus.COMPENSATING) {
            throw new WorkflowStepInvalidStateException(
                    "Cannot compensate instance in state: " + instance.status());
        }

        WorkflowDefinition definition = workflowDefinitions.get(instance.workflowId());
        List<String> reverseOrder =
                coordinator.reverseTopologicalSort(definition.steps(), definition.edges());

        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states != null) {
            for (String stepId : reverseOrder) {
                states.computeIfPresent(
                        stepId,
                        (key, s) -> {
                            if (s.status() == StepStatus.COMPLETED) {
                                return new WorkflowStepState(
                                        s.stepId(),
                                        StepStatus.COMPENSATING,
                                        s.input(),
                                        s.output(),
                                        s.error(),
                                        s.retryCount(),
                                        s.nextRetryAt(),
                                        s.startedAt(),
                                        s.completedAt());
                            }
                            return s;
                        });
            }
        }

        WorkflowInstance compensating =
                new WorkflowInstance(
                        instance.instanceId(),
                        instance.workflowId(),
                        instance.workflowVersion(),
                        instance.agentId(),
                        instance.input(),
                        instance.output(),
                        WorkflowInstanceStatus.COMPENSATING,
                        instance.error(),
                        instance.cancelReason(),
                        reason,
                        instance.stepIds(),
                        instance.coordinationResourceIds(),
                        instance.timeoutMs(),
                        instance.startedAt(),
                        instance.completedAt());
        workflowInstances.put(instanceId, compensating);

        log.info("Workflow compensation started: instanceId={}, reason={}", instanceId, reason);
        return true;
    }

    @Override
    public boolean retryCompensation(String instanceId) {
        WorkflowInstance instance = workflowInstances.get(instanceId);
        if (instance == null) {
            throw new WorkflowInstanceNotFoundException(
                    "Workflow instance not found: " + instanceId);
        }
        if (instance.status() != WorkflowInstanceStatus.COMPENSATION_FAILED) {
            throw new WorkflowStepInvalidStateException(
                    "Cannot retry compensation for instance in state: " + instance.status());
        }

        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states != null) {
            states.replaceAll(
                    (stepId, s) -> {
                        if (s.status() == StepStatus.COMPENSATION_FAILED) {
                            return new WorkflowStepState(
                                    s.stepId(),
                                    StepStatus.COMPENSATING,
                                    s.input(),
                                    s.output(),
                                    s.error(),
                                    s.retryCount(),
                                    s.nextRetryAt(),
                                    s.startedAt(),
                                    s.completedAt());
                        }
                        return s;
                    });
        }

        WorkflowInstance retrying =
                new WorkflowInstance(
                        instance.instanceId(),
                        instance.workflowId(),
                        instance.workflowVersion(),
                        instance.agentId(),
                        instance.input(),
                        instance.output(),
                        WorkflowInstanceStatus.COMPENSATING,
                        instance.error(),
                        instance.cancelReason(),
                        instance.compensateReason(),
                        instance.stepIds(),
                        instance.coordinationResourceIds(),
                        instance.timeoutMs(),
                        instance.startedAt(),
                        instance.completedAt());
        workflowInstances.put(instanceId, retrying);

        log.info("Workflow compensation retried: instanceId={}", instanceId);
        return true;
    }

    @Override
    public WorkflowInstance resumeWorkflow(
            String instanceId, String fromStepId, Map<String, Object> resumeInput) {
        WorkflowInstance instance = workflowInstances.get(instanceId);
        if (instance == null) {
            throw new WorkflowInstanceNotFoundException(
                    "Workflow instance not found: " + instanceId);
        }
        if (instance.status() != WorkflowInstanceStatus.FAILED
                && instance.status() != WorkflowInstanceStatus.CANCELLED) {
            throw new WorkflowStepInvalidStateException(
                    "Cannot resume instance in state: " + instance.status());
        }

        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states != null) {
            states.computeIfPresent(
                    fromStepId,
                    (key, s) ->
                            new WorkflowStepState(
                                    s.stepId(),
                                    StepStatus.READY,
                                    resumeInput != null ? resumeInput : s.input(),
                                    s.output(),
                                    null,
                                    0,
                                    null,
                                    System.currentTimeMillis(),
                                    0));
        }

        WorkflowInstance resumed =
                new WorkflowInstance(
                        instance.instanceId(),
                        instance.workflowId(),
                        instance.workflowVersion(),
                        instance.agentId(),
                        instance.input(),
                        instance.output(),
                        WorkflowInstanceStatus.RUNNING,
                        null,
                        null,
                        instance.compensateReason(),
                        instance.stepIds(),
                        instance.coordinationResourceIds(),
                        instance.timeoutMs(),
                        instance.startedAt(),
                        instance.completedAt());
        workflowInstances.put(instanceId, resumed);

        coordinator.evaluateDownstream(instanceId, fromStepId);

        log.info("Workflow resumed: instanceId={}, fromStepId={}", instanceId, fromStepId);
        return resumed;
    }

    @Override
    public WorkflowStepState completeStep(
            String instanceId, String stepId, Map<String, Object> output, String requestToken) {
        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states == null) {
            throw new WorkflowStepNotFoundException(
                    "Step not found: instanceId=" + instanceId + ", stepId=" + stepId);
        }

        if (requestToken != null) {
            String tokenKey = instanceId + ":" + stepId + ":" + requestToken;
            String existing = requestTokenDedup.putIfAbsent(tokenKey, "1");
            if (existing != null) {
                WorkflowStepState current = states.get(stepId);
                if (current != null) return current;
            }
        }

        AtomicReference<WorkflowStepState> result = new AtomicReference<>();
        states.compute(
                stepId,
                (key, current) -> {
                    if (current == null) {
                        throw new WorkflowStepNotFoundException(
                                "Step not found: instanceId=" + instanceId + ", stepId=" + stepId);
                    }
                    if (current.status() != StepStatus.RUNNING
                            && current.status() != StepStatus.WAITING
                            && current.status() != StepStatus.READY) {
                        result.set(current);
                        return current;
                    }
                    WorkflowStepState updated =
                            new WorkflowStepState(
                                    stepId,
                                    StepStatus.COMPLETED,
                                    current.input(),
                                    output == null ? Map.of() : Map.copyOf(output),
                                    null,
                                    current.retryCount(),
                                    null,
                                    current.startedAt(),
                                    System.currentTimeMillis());
                    result.set(updated);
                    return updated;
                });

        WorkflowStepState finalState = result.get();
        if (finalState != null && finalState.status() == StepStatus.COMPLETED) {
            coordinator.evaluateDownstream(instanceId, stepId);
            checkInstanceCompletion(instanceId);
        }

        log.info("Step completed: instanceId={}, stepId={}", instanceId, stepId);
        return finalState;
    }

    @Override
    public WorkflowStepState failStep(
            String instanceId, String stepId, StepError error, String requestToken) {
        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states == null) {
            throw new WorkflowStepNotFoundException(
                    "Step not found: instanceId=" + instanceId + ", stepId=" + stepId);
        }

        if (requestToken != null) {
            String tokenKey = instanceId + ":" + stepId + ":" + requestToken;
            String existing = requestTokenDedup.putIfAbsent(tokenKey, "1");
            if (existing != null) {
                WorkflowStepState current = states.get(stepId);
                if (current != null) return current;
            }
        }

        AtomicReference<WorkflowStepState> result = new AtomicReference<>();
        states.compute(
                stepId,
                (key, current) -> {
                    if (current == null) {
                        throw new WorkflowStepNotFoundException(
                                "Step not found: instanceId=" + instanceId + ", stepId=" + stepId);
                    }
                    if (current.status() != StepStatus.RUNNING
                            && current.status() != StepStatus.WAITING
                            && current.status() != StepStatus.READY) {
                        result.set(current);
                        return current;
                    }

                    WorkflowStepDef stepDef = findStepDef(instanceId, stepId);
                    if (stepDef != null
                            && error.retryable()
                            && current.retryCount() < stepDef.retryPolicy().maxRetries()) {
                        long delay =
                                (long)
                                        (stepDef.retryPolicy().initialDelayMs()
                                                * Math.pow(
                                                        stepDef.retryPolicy().backoffMultiplier(),
                                                        current.retryCount()));
                        long nextRetryAt = System.currentTimeMillis() + delay;
                        WorkflowStepState updated =
                                new WorkflowStepState(
                                        stepId,
                                        StepStatus.RETRYING,
                                        current.input(),
                                        current.output(),
                                        error.message(),
                                        current.retryCount() + 1,
                                        nextRetryAt,
                                        current.startedAt(),
                                        0);
                        result.set(updated);
                        return updated;
                    } else {
                        WorkflowStepState updated =
                                new WorkflowStepState(
                                        stepId,
                                        StepStatus.FAILED,
                                        current.input(),
                                        current.output(),
                                        error.message(),
                                        current.retryCount(),
                                        null,
                                        current.startedAt(),
                                        System.currentTimeMillis());
                        result.set(updated);
                        return updated;
                    }
                });

        WorkflowStepState finalState = result.get();
        if (finalState != null && finalState.status() == StepStatus.FAILED) {
            workflowInstances.computeIfPresent(
                    instanceId,
                    (key, inst) ->
                            new WorkflowInstance(
                                    inst.instanceId(),
                                    inst.workflowId(),
                                    inst.workflowVersion(),
                                    inst.agentId(),
                                    inst.input(),
                                    inst.output(),
                                    WorkflowInstanceStatus.FAILED,
                                    error.message(),
                                    inst.cancelReason(),
                                    inst.compensateReason(),
                                    inst.stepIds(),
                                    inst.coordinationResourceIds(),
                                    inst.timeoutMs(),
                                    inst.startedAt(),
                                    System.currentTimeMillis()));
        }

        coordinator.evaluateDownstream(instanceId, stepId);

        log.info(
                "Step failed: instanceId={}, stepId={}, retryable={}",
                instanceId,
                stepId,
                error.retryable());
        return finalState;
    }

    @Override
    public WorkflowStepState compensateStep(
            String instanceId,
            String stepId,
            Map<String, Object> compensateInput,
            String requestToken) {
        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states == null) {
            throw new WorkflowStepNotFoundException(
                    "Step not found: instanceId=" + instanceId + ", stepId=" + stepId);
        }

        if (requestToken != null) {
            String tokenKey = instanceId + ":" + stepId + ":" + requestToken;
            String existing = requestTokenDedup.putIfAbsent(tokenKey, "1");
            if (existing != null) {
                WorkflowStepState current = states.get(stepId);
                if (current != null) return current;
            }
        }

        AtomicReference<WorkflowStepState> result = new AtomicReference<>();
        states.compute(
                stepId,
                (key, current) -> {
                    if (current == null) {
                        throw new WorkflowStepNotFoundException(
                                "Step not found: instanceId=" + instanceId + ", stepId=" + stepId);
                    }
                    if (current.status() != StepStatus.COMPENSATING) {
                        result.set(current);
                        return current;
                    }
                    WorkflowStepState updated =
                            new WorkflowStepState(
                                    stepId,
                                    StepStatus.COMPENSATED,
                                    current.input(),
                                    current.output(),
                                    null,
                                    current.retryCount(),
                                    null,
                                    current.startedAt(),
                                    System.currentTimeMillis());
                    result.set(updated);
                    return updated;
                });

        WorkflowStepState finalState = result.get();
        if (finalState != null && finalState.status() == StepStatus.COMPENSATED) {
            checkCompensationCompletion(instanceId);
        }

        log.info("Step compensated: instanceId={}, stepId={}", instanceId, stepId);
        return finalState;
    }

    private List<ValidationError> validateDag(
            List<WorkflowStepDef> steps, List<WorkflowEdgeDef> edges) {
        List<ValidationError> errors = new ArrayList<>();
        Set<String> stepIds = new HashSet<>();
        Map<String, Integer> stepIdCount = new HashMap<>();

        for (WorkflowStepDef step : steps) {
            stepIdCount.merge(step.stepId(), 1, Integer::sum);
            if (step.stepId().contains("/")) {
                errors.add(
                        new ValidationError(
                                ValidationErrorType.INVALID_STEP_ID_FORMAT,
                                step.stepId(),
                                "Step ID must not contain '/': " + step.stepId()));
            }
        }

        for (var entry : stepIdCount.entrySet()) {
            if (entry.getValue() > 1) {
                errors.add(
                        new ValidationError(
                                ValidationErrorType.DUPLICATE_STEP_ID,
                                entry.getKey(),
                                "Duplicate step ID: " + entry.getKey()));
            }
        }

        stepIds.addAll(stepIdCount.keySet());

        for (WorkflowEdgeDef edge : edges) {
            if (edge.fromStepId().equals(edge.toStepId())) {
                errors.add(
                        new ValidationError(
                                ValidationErrorType.SELF_LOOP,
                                edge.fromStepId(),
                                "Self-loop detected: " + edge.fromStepId()));
            }
            if (!stepIds.contains(edge.fromStepId())) {
                errors.add(
                        new ValidationError(
                                ValidationErrorType.INVALID_EDGE_REF,
                                edge.fromStepId(),
                                "Edge references non-existent fromStepId: " + edge.fromStepId()));
            }
            if (!stepIds.contains(edge.toStepId())) {
                errors.add(
                        new ValidationError(
                                ValidationErrorType.INVALID_EDGE_REF,
                                edge.toStepId(),
                                "Edge references non-existent toStepId: " + edge.toStepId()));
            }
        }

        if (detectCycle(steps, edges)) {
            errors.add(
                    new ValidationError(
                            ValidationErrorType.CYCLE, null, "Cycle detected in workflow DAG"));
        }

        if (steps.size() > options.maxWorkflowSteps()) {
            errors.add(
                    new ValidationError(
                            ValidationErrorType.TOO_MANY_FAN_OUT,
                            null,
                            "Too many steps: "
                                    + steps.size()
                                    + ", max: "
                                    + options.maxWorkflowSteps()));
        }

        int maxDepth = computeMaxDepth(steps, edges);
        if (maxDepth > options.maxWorkflowDepth()) {
            errors.add(
                    new ValidationError(
                            ValidationErrorType.TOO_DEEP,
                            null,
                            "DAG depth "
                                    + maxDepth
                                    + " exceeds max "
                                    + options.maxWorkflowDepth()));
        }

        int maxFanOut = computeMaxFanOut(edges);
        if (maxFanOut > options.maxWorkflowFanOut()) {
            errors.add(
                    new ValidationError(
                            ValidationErrorType.TOO_MANY_FAN_OUT,
                            null,
                            "Max fan-out "
                                    + maxFanOut
                                    + " exceeds max "
                                    + options.maxWorkflowFanOut()));
        }

        return errors;
    }

    private boolean detectCycle(List<WorkflowStepDef> steps, List<WorkflowEdgeDef> edges) {
        Map<String, Set<String>> adj = new HashMap<>();
        for (WorkflowStepDef step : steps) {
            adj.put(step.stepId(), new HashSet<>());
        }
        for (WorkflowEdgeDef edge : edges) {
            if (adj.containsKey(edge.fromStepId())) {
                adj.get(edge.fromStepId()).add(edge.toStepId());
            }
        }

        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();

        for (String stepId : adj.keySet()) {
            if (hasCycle(stepId, adj, visited, recursionStack)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasCycle(
            String stepId,
            Map<String, Set<String>> adj,
            Set<String> visited,
            Set<String> recursionStack) {
        if (recursionStack.contains(stepId)) {
            return true;
        }
        if (visited.contains(stepId)) {
            return false;
        }

        visited.add(stepId);
        recursionStack.add(stepId);

        for (String neighbor : adj.getOrDefault(stepId, Set.of())) {
            if (hasCycle(neighbor, adj, visited, recursionStack)) {
                return true;
            }
        }

        recursionStack.remove(stepId);
        return false;
    }

    private int computeMaxDepth(List<WorkflowStepDef> steps, List<WorkflowEdgeDef> edges) {
        Map<String, Set<String>> adj = new HashMap<>();
        for (WorkflowStepDef step : steps) {
            adj.put(step.stepId(), new HashSet<>());
        }
        for (WorkflowEdgeDef edge : edges) {
            if (adj.containsKey(edge.fromStepId())) {
                adj.get(edge.fromStepId()).add(edge.toStepId());
            }
        }

        Set<String> hasIncoming = new HashSet<>();
        for (WorkflowEdgeDef edge : edges) {
            hasIncoming.add(edge.toStepId());
        }

        Map<String, Integer> depthMap = new HashMap<>();
        int maxDepth = 0;
        for (WorkflowStepDef step : steps) {
            if (!hasIncoming.contains(step.stepId())) {
                maxDepth = Math.max(maxDepth, computeDepth(step.stepId(), adj, depthMap));
            }
        }
        return maxDepth;
    }

    private int computeDepth(
            String stepId, Map<String, Set<String>> adj, Map<String, Integer> depthMap) {
        if (depthMap.containsKey(stepId)) {
            return depthMap.get(stepId);
        }
        Set<String> neighbors = adj.getOrDefault(stepId, Set.of());
        if (neighbors.isEmpty()) {
            depthMap.put(stepId, 1);
            return 1;
        }
        int depth = 1;
        for (String neighbor : neighbors) {
            depth = Math.max(depth, 1 + computeDepth(neighbor, adj, depthMap));
        }
        depthMap.put(stepId, depth);
        return depth;
    }

    private int computeMaxFanOut(List<WorkflowEdgeDef> edges) {
        Map<String, Long> fanOut =
                edges.stream()
                        .collect(
                                Collectors.groupingBy(
                                        WorkflowEdgeDef::fromStepId, Collectors.counting()));
        return fanOut.values().stream().mapToInt(Long::intValue).max().orElse(0);
    }

    private void evaluateInitialSteps(String instanceId, WorkflowDefinition definition) {
        Set<String> hasIncoming =
                definition.edges().stream()
                        .map(WorkflowEdgeDef::toStepId)
                        .collect(Collectors.toSet());

        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states == null) return;

        for (WorkflowStepDef step : definition.steps()) {
            if (!hasIncoming.contains(step.stepId())) {
                states.computeIfPresent(
                        step.stepId(),
                        (key, current) -> {
                            if (current.status() == StepStatus.PENDING) {
                                return new WorkflowStepState(
                                        step.stepId(),
                                        StepStatus.READY,
                                        current.input(),
                                        current.output(),
                                        current.error(),
                                        current.retryCount(),
                                        current.nextRetryAt(),
                                        System.currentTimeMillis(),
                                        current.completedAt());
                            }
                            return current;
                        });
            }
        }
    }

    private void checkInstanceCompletion(String instanceId) {
        workflowInstances.computeIfPresent(
                instanceId,
                (key, instance) -> {
                    ConcurrentHashMap<String, WorkflowStepState> states =
                            stepStates.get(instanceId);
                    if (states == null) return instance;

                    boolean allTerminal =
                            states.values().stream()
                                    .allMatch(
                                            s ->
                                                    s.status() == StepStatus.COMPLETED
                                                            || s.status() == StepStatus.FAILED
                                                            || s.status() == StepStatus.CANCELLED
                                                            || s.status() == StepStatus.SKIPPED
                                                            || s.status() == StepStatus.COMPENSATED
                                                            || s.status()
                                                                    == StepStatus
                                                                            .COMPENSATION_FAILED
                                                            || s.status() == StepStatus.TIMEOUT);

                    if (!allTerminal) return instance;

                    boolean anyFailed =
                            states.values().stream()
                                    .anyMatch(
                                            s ->
                                                    s.status() == StepStatus.FAILED
                                                            || s.status() == StepStatus.CANCELLED
                                                            || s.status() == StepStatus.TIMEOUT
                                                            || s.status()
                                                                    == StepStatus
                                                                            .COMPENSATION_FAILED);

                    if (!anyFailed) {
                        coordinator.releaseCoordResources(instanceId);
                        cleanupRequestTokens(instanceId);
                        log.info("Workflow instance completed: {}", instanceId);
                        return new WorkflowInstance(
                                instance.instanceId(),
                                instance.workflowId(),
                                instance.workflowVersion(),
                                instance.agentId(),
                                instance.input(),
                                instance.output(),
                                WorkflowInstanceStatus.COMPLETED,
                                instance.error(),
                                instance.cancelReason(),
                                instance.compensateReason(),
                                instance.stepIds(),
                                instance.coordinationResourceIds(),
                                instance.timeoutMs(),
                                instance.startedAt(),
                                System.currentTimeMillis());
                    }
                    return instance;
                });
    }

    private void checkCompensationCompletion(String instanceId) {
        workflowInstances.computeIfPresent(
                instanceId,
                (key, instance) -> {
                    ConcurrentHashMap<String, WorkflowStepState> states =
                            stepStates.get(instanceId);
                    if (states == null) return instance;

                    boolean anyCompensating =
                            states.values().stream()
                                    .anyMatch(s -> s.status() == StepStatus.COMPENSATING);

                    if (anyCompensating) return instance;

                    boolean anyFailed =
                            states.values().stream()
                                    .anyMatch(s -> s.status() == StepStatus.COMPENSATION_FAILED);

                    WorkflowInstanceStatus newStatus =
                            anyFailed
                                    ? WorkflowInstanceStatus.COMPENSATION_FAILED
                                    : WorkflowInstanceStatus.COMPENSATED;

                    if (newStatus == WorkflowInstanceStatus.COMPENSATED
                            || newStatus == WorkflowInstanceStatus.COMPENSATION_FAILED) {
                        cleanupRequestTokens(instanceId);
                    }

                    if (newStatus == WorkflowInstanceStatus.COMPENSATED) {
                        coordinator.releaseCoordResources(instanceId);
                    }

                    log.info("Workflow compensation {}: instanceId={}", newStatus, instanceId);
                    return new WorkflowInstance(
                            instance.instanceId(),
                            instance.workflowId(),
                            instance.workflowVersion(),
                            instance.agentId(),
                            instance.input(),
                            instance.output(),
                            newStatus,
                            instance.error(),
                            instance.cancelReason(),
                            instance.compensateReason(),
                            instance.stepIds(),
                            instance.coordinationResourceIds(),
                            instance.timeoutMs(),
                            instance.startedAt(),
                            System.currentTimeMillis());
                });
    }

    private WorkflowStepDef findStepDef(String instanceId, String stepId) {
        WorkflowInstance instance = workflowInstances.get(instanceId);
        if (instance == null) return null;
        WorkflowDefinition definition = workflowDefinitions.get(instance.workflowId());
        if (definition == null) return null;
        return definition.steps().stream()
                .filter(s -> s.stepId().equals(stepId))
                .findFirst()
                .orElse(null);
    }

    private void cleanupRequestTokens(String instanceId) {
        requestTokenDedup.keySet().removeIf(key -> key.startsWith(instanceId + ":"));
    }

    @Override
    public List<WorkflowStepState> getStepStates(String instanceId) {
        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states == null) {
            return List.of();
        }
        return List.copyOf(states.values());
    }

    @Override
    public void triggerStepRetry(String instanceId, String stepId) {
        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states == null) return;

        states.computeIfPresent(
                stepId,
                (key, current) -> {
                    if (current.status() != StepStatus.RETRYING) return current;
                    if (current.nextRetryAt() != null
                            && current.nextRetryAt() > System.currentTimeMillis()) {
                        return current;
                    }
                    log.info("Step retry triggered: instanceId={}, stepId={}", instanceId, stepId);
                    return new WorkflowStepState(
                            stepId,
                            StepStatus.RUNNING,
                            current.input(),
                            current.output(),
                            null,
                            current.retryCount(),
                            null,
                            System.currentTimeMillis(),
                            0);
                });
    }

    @Override
    public boolean timeoutWorkflow(String instanceId) {
        AtomicReference<Boolean> changed = new AtomicReference<>(false);
        AtomicReference<WorkflowInstanceStatus> nextStatusRef = new AtomicReference<>();
        workflowInstances.computeIfPresent(
                instanceId,
                (key, instance) -> {
                    if (instance.status() != WorkflowInstanceStatus.RUNNING) {
                        return instance;
                    }
                    changed.set(true);

                    ConcurrentHashMap<String, WorkflowStepState> states =
                            stepStates.get(instanceId);
                    if (states != null) {
                        states.replaceAll(
                                (stepId, s) -> {
                                    if (s.status() == StepStatus.RUNNING
                                            || s.status() == StepStatus.READY
                                            || s.status() == StepStatus.WAITING
                                            || s.status() == StepStatus.RETRYING) {
                                        return new WorkflowStepState(
                                                s.stepId(),
                                                StepStatus.TIMEOUT,
                                                s.input(),
                                                s.output(),
                                                "Timed out",
                                                s.retryCount(),
                                                s.nextRetryAt(),
                                                s.startedAt(),
                                                System.currentTimeMillis());
                                    }
                                    return s;
                                });
                    }

                    boolean hasCompensableSteps = false;
                    if (states != null) {
                        WorkflowDefinition wfDef = workflowDefinitions.get(instance.workflowId());
                        if (wfDef != null) {
                            Map<String, WorkflowStepDef> stepDefMap =
                                    wfDef.steps().stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            WorkflowStepDef::stepId, s -> s));
                            hasCompensableSteps =
                                    states.values().stream()
                                            .anyMatch(
                                                    s ->
                                                            (s.status() == StepStatus.TIMEOUT
                                                                            || s.status()
                                                                                    == StepStatus
                                                                                            .FAILED)
                                                                    && stepDefMap.containsKey(
                                                                            s.stepId())
                                                                    && stepDefMap
                                                                                    .get(s.stepId())
                                                                                    .compensation()
                                                                            != null);
                        }
                    }

                    WorkflowInstanceStatus nextStatus;
                    String error;
                    if (hasCompensableSteps) {
                        nextStatus = WorkflowInstanceStatus.COMPENSATING;
                        error = "Workflow instance timed out, initiating compensation";
                    } else {
                        nextStatus = WorkflowInstanceStatus.CANCELLED;
                        error = "Workflow instance timed out";
                    }
                    nextStatusRef.set(nextStatus);

                    if (nextStatus == WorkflowInstanceStatus.CANCELLED) {
                        try {
                            coordinator.releaseCoordResources(instanceId);
                        } catch (Exception ignored) {
                        }
                        cleanupRequestTokens(instanceId);
                    }

                    return new WorkflowInstance(
                            instance.instanceId(),
                            instance.workflowId(),
                            instance.workflowVersion(),
                            instance.agentId(),
                            instance.input(),
                            instance.output(),
                            nextStatus,
                            error,
                            instance.cancelReason(),
                            instance.compensateReason(),
                            instance.stepIds(),
                            instance.coordinationResourceIds(),
                            instance.timeoutMs(),
                            instance.startedAt(),
                            System.currentTimeMillis());
                });

        if (nextStatusRef.get() == WorkflowInstanceStatus.COMPENSATING) {
            try {
                compensateWorkflow(instanceId, "Auto-compensation after timeout");
            } catch (Exception ignored) {
            }
        }

        return changed.get();
    }

    @Override
    public void markStepReady(String instanceId, String stepId) {
        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states == null) return;

        states.computeIfPresent(
                stepId,
                (key, current) -> {
                    if (current.status() != StepStatus.PENDING
                            && current.status() != StepStatus.RETRYING) return current;
                    return new WorkflowStepState(
                            stepId,
                            StepStatus.READY,
                            current.input(),
                            current.output(),
                            current.error(),
                            current.retryCount(),
                            current.nextRetryAt(),
                            System.currentTimeMillis(),
                            current.completedAt());
                });
    }

    @Override
    public void markStepSkipped(String instanceId, String stepId) {
        ConcurrentHashMap<String, WorkflowStepState> states = stepStates.get(instanceId);
        if (states == null) return;

        states.computeIfPresent(
                stepId,
                (key, current) -> {
                    if (current.status() != StepStatus.PENDING) return current;
                    return new WorkflowStepState(
                            stepId,
                            StepStatus.SKIPPED,
                            current.input(),
                            current.output(),
                            current.error(),
                            current.retryCount(),
                            current.nextRetryAt(),
                            current.startedAt(),
                            System.currentTimeMillis());
                });
    }
}
