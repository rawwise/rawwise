package io.github.rawwise.axiom.orchestrator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultWorkflowCoordinator implements WorkflowCoordinator {

    private volatile AgentOrchestrator orchestrator;

    public void setOrchestrator(AgentOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Override
    public void evaluateDownstream(String instanceId, String completedStepId) {
        if (orchestrator == null) return;
        WorkflowInstance instance = orchestrator.getWorkflowInstance(instanceId);
        if (instance == null) return;
        WorkflowDefinition def = orchestrator.getWorkflow(instance.workflowId());
        if (def == null) return;

        List<WorkflowStepState> states = orchestrator.getStepStates(instanceId);
        Map<String, StepStatus> statusMap =
                states.stream()
                        .collect(
                                Collectors.toMap(
                                        WorkflowStepState::stepId, WorkflowStepState::status));

        StepStatus completedStatus = statusMap.get(completedStepId);
        if (completedStatus == null) return;

        for (WorkflowEdgeDef edge : def.edges()) {
            if (!edge.fromStepId().equals(completedStepId)) continue;

            boolean conditionMet =
                    switch (edge.condition()) {
                        case ALWAYS -> true;
                        case ON_SUCCESS -> completedStatus == StepStatus.COMPLETED;
                        case ON_FAILURE -> completedStatus == StepStatus.FAILED;
                        case ON_TIMEOUT -> completedStatus == StepStatus.TIMEOUT;
                    };

            if (!conditionMet) continue;

            StepStatus targetStatus = statusMap.get(edge.toStepId());
            if (targetStatus != StepStatus.PENDING) continue;

            boolean allUpstreamDone =
                    def.edges().stream()
                            .filter(e -> e.toStepId().equals(edge.toStepId()))
                            .allMatch(
                                    e -> {
                                        StepStatus fromStatus = statusMap.get(e.fromStepId());
                                        return fromStatus == StepStatus.COMPLETED
                                                || fromStatus == StepStatus.FAILED
                                                || fromStatus == StepStatus.TIMEOUT
                                                || fromStatus == StepStatus.SKIPPED
                                                || fromStatus == StepStatus.CANCELLED;
                                    });

            if (allUpstreamDone) {
                orchestrator.markStepReady(instanceId, edge.toStepId());
            }
        }

        for (WorkflowStepDef stepDef : def.steps()) {
            StepStatus currentStatus = statusMap.get(stepDef.stepId());
            if (currentStatus != StepStatus.PENDING) continue;

            List<WorkflowEdgeDef> incomingEdges =
                    def.edges().stream()
                            .filter(e -> e.toStepId().equals(stepDef.stepId()))
                            .toList();

            if (incomingEdges.isEmpty()) continue;

            boolean allUpstreamTerminal =
                    incomingEdges.stream()
                            .allMatch(e -> isTerminalStatus(statusMap.get(e.fromStepId())));

            if (!allUpstreamTerminal) continue;

            boolean anyEdgeSatisfied =
                    incomingEdges.stream()
                            .anyMatch(
                                    e ->
                                            isEdgeConditionSatisfied(
                                                    e, statusMap.get(e.fromStepId())));

            if (!anyEdgeSatisfied) {
                orchestrator.markStepSkipped(instanceId, stepDef.stepId());
            }
        }
    }

    private boolean isTerminalStatus(StepStatus status) {
        return status == StepStatus.COMPLETED
                || status == StepStatus.FAILED
                || status == StepStatus.TIMEOUT
                || status == StepStatus.CANCELLED
                || status == StepStatus.SKIPPED
                || status == StepStatus.COMPENSATED
                || status == StepStatus.COMPENSATION_FAILED;
    }

    private boolean isEdgeConditionSatisfied(WorkflowEdgeDef edge, StepStatus fromStatus) {
        return edge.condition() == EdgeCondition.ALWAYS
                || (edge.condition() == EdgeCondition.ON_SUCCESS
                        && fromStatus == StepStatus.COMPLETED)
                || (edge.condition() == EdgeCondition.ON_FAILURE && fromStatus == StepStatus.FAILED)
                || (edge.condition() == EdgeCondition.ON_TIMEOUT
                        && fromStatus == StepStatus.TIMEOUT);
    }

    @Override
    public List<String> topologicalSort(List<WorkflowStepDef> steps, List<WorkflowEdgeDef> edges) {
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adj = new HashMap<>();
        for (WorkflowStepDef step : steps) {
            inDegree.put(step.stepId(), 0);
            adj.put(step.stepId(), new ArrayList<>());
        }
        for (WorkflowEdgeDef edge : edges) {
            if (adj.containsKey(edge.fromStepId())) {
                adj.get(edge.fromStepId()).add(edge.toStepId());
                inDegree.merge(edge.toStepId(), 1, Integer::sum);
            }
        }
        Queue<String> queue = new LinkedList<>();
        for (var entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) queue.add(entry.getKey());
        }
        List<String> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            String node = queue.poll();
            result.add(node);
            for (String next : adj.get(node)) {
                int newDegree = inDegree.merge(next, -1, Integer::sum);
                if (newDegree == 0) queue.add(next);
            }
        }
        return result;
    }

    @Override
    public List<String> reverseTopologicalSort(
            List<WorkflowStepDef> steps, List<WorkflowEdgeDef> edges) {
        List<String> sorted = topologicalSort(steps, edges);
        Collections.reverse(sorted);
        return sorted;
    }

    @Override
    public List<String> allocateCoordResources(String instanceId, WorkflowDefinition definition) {
        return List.of();
    }

    @Override
    public void releaseCoordResources(String instanceId) {}
}
